package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaConsumerGroup string
	ProcessingDelayMs  int
	HTTPPort           int
}

func Load() (Config, error) {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "demo-topic")
	group := getEnv("KAFKA_CONSUMER_GROUP", "demo-consumer-group")

	delay, err := getPositiveIntEnv("PROCESSING_DELAY_MS", 3000)
	if err != nil {
		return Config{}, fmt.Errorf("invalid PROCESSING_DELAY_MS: %w", err)
	}

	port, err := getPositiveIntEnv("HTTP_PORT", 8080)
	if err != nil {
		return Config{}, fmt.Errorf("invalid HTTP_PORT: %w", err)
	}

	return Config{
		KafkaBrokers:       splitAndTrim(brokers),
		KafkaTopic:         topic,
		KafkaConsumerGroup: group,
		ProcessingDelayMs:  delay,
		HTTPPort:           port,
	}, nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getPositiveIntEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}

	if parsed < 0 {
		return 0, fmt.Errorf("%s must not be negative", key)
	}

	return parsed, nil
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}
