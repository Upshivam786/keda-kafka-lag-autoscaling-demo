package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Upshivam786/keda-kafka-demo/app/internal/config"
	"github.com/Upshivam786/keda-kafka-demo/app/internal/consumer"
	appmetrics "github.com/Upshivam786/keda-kafka-demo/app/internal/metrics"
	"github.com/Upshivam786/keda-kafka-demo/app/internal/server"
)

const (
	initialRetryDelay = 1 * time.Second
	maxRetryDelay     = 30 * time.Second
)

func main() {
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}),
	)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"starting Kafka consumer",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"consumer_group", cfg.KafkaConsumerGroup,
		"processing_delay_ms", cfg.ProcessingDelayMs,
	)

	registry := prometheus.NewRegistry()
	metrics := appmetrics.New(registry)

	httpServer := server.New(cfg.HTTPPort, registry)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Start HTTP endpoints independently of Kafka connectivity.
	go func() {
		logger.Info("HTTP server starting", "port", cfg.HTTPPort)

		if err := httpServer.Start(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server stopped with error", "error", err)
			stop()
		}
	}()

	retryDelay := initialRetryDelay

	for {
		if ctx.Err() != nil {
			break
		}

		saramaConfig := sarama.NewConfig()
		saramaConfig.Version = sarama.V3_9_0_0
		saramaConfig.Consumer.Group.Rebalance.Strategy =
			sarama.NewBalanceStrategyRoundRobin()
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

		logger.Info(
			"connecting to Kafka",
			"brokers", cfg.KafkaBrokers,
			"retry_delay", retryDelay,
		)

		group, err := sarama.NewConsumerGroup(
			cfg.KafkaBrokers,
			cfg.KafkaConsumerGroup,
			saramaConfig,
		)
		if err != nil {
			logger.Error(
				"failed to create Kafka consumer group",
				"error", err,
				"retry_in", retryDelay,
			)

			if !waitForRetry(ctx, retryDelay) {
				break
			}

			retryDelay *= 2
			if retryDelay > maxRetryDelay {
				retryDelay = maxRetryDelay
			}

			continue
		}

		// Kafka connection succeeded.
		retryDelay = initialRetryDelay

		logger.Info("Kafka consumer connected; waiting for consumer group assignment")

		c := consumer.New(group, cfg, logger, metrics, httpServer)

		err = c.Run(ctx)

		// Once consumption stops, the pod is no longer ready.
		httpServer.SetReady(false)

		if closeErr := group.Close(); closeErr != nil {
			logger.Error(
				"failed to close Kafka consumer group",
				"error", closeErr,
			)
		}

		if ctx.Err() != nil {
			break
		}

		if err != nil {
			logger.Error(
				"Kafka consumer stopped; retrying",
				"error", err,
				"retry_in", retryDelay,
			)
		} else {
			logger.Warn(
				"Kafka consumer stopped unexpectedly; retrying",
				"retry_in", retryDelay,
			)
		}

		if !waitForRetry(ctx, retryDelay) {
			break
		}

		retryDelay *= 2
		if retryDelay > maxRetryDelay {
			retryDelay = maxRetryDelay
		}
	}

	httpServer.SetReady(false)

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown failed", "error", err)
	}

	logger.Info("Kafka consumer stopped")
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
