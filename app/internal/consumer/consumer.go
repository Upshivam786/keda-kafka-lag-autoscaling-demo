package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"github.com/Upshivam786/keda-kafka-demo/app/internal/config"
	appmetrics "github.com/Upshivam786/keda-kafka-demo/app/internal/metrics"
)

type Readiness interface {
	SetReady(bool)
}

type Consumer struct {
	client    sarama.ConsumerGroup
	topic     string
	delay     time.Duration
	logger    *slog.Logger
	metrics   *appmetrics.Metrics
	readiness Readiness
}

type Handler struct {
	delay     time.Duration
	logger    *slog.Logger
	metrics   *appmetrics.Metrics
	readiness Readiness
}

func New(
	client sarama.ConsumerGroup,
	cfg config.Config,
	logger *slog.Logger,
	metrics *appmetrics.Metrics,
	readiness Readiness,
) *Consumer {
	return &Consumer{
		client:    client,
		topic:     cfg.KafkaTopic,
		delay:     time.Duration(cfg.ProcessingDelayMs) * time.Millisecond,
		logger:    logger,
		metrics:   metrics,
		readiness: readiness,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	handler := &Handler{
		delay:     c.delay,
		logger:    c.logger,
		metrics:   c.metrics,
		readiness: c.readiness,
	}

	for {
		if err := c.client.Consume(ctx, []string{c.topic}, handler); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	h.logger.Info("consumer group session started")

	if h.readiness != nil {
		h.readiness.SetReady(true)
	}

	return nil
}

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	if h.readiness != nil {
		h.readiness.SetReady(false)
	}

	h.logger.Info("consumer group session ending")
	return nil
}

func (h *Handler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			start := time.Now()

			err := h.processMessage(message)

			h.metrics.ObserveProcessing(start, err)

			if err != nil {
				h.logger.Error(
					"message processing failed",
					"topic", message.Topic,
					"partition", message.Partition,
					"offset", message.Offset,
					"error", err,
				)
				continue
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *Handler) processMessage(message *sarama.ConsumerMessage) error {
	h.logger.Info(
		"processing message",
		"topic", message.Topic,
		"partition", message.Partition,
		"offset", message.Offset,
	)

	if h.delay > 0 {
		time.Sleep(h.delay)
	}

	h.logger.Info(
		"message processed",
		"topic", message.Topic,
		"partition", message.Partition,
		"offset", message.Offset,
	)

	return nil
}
