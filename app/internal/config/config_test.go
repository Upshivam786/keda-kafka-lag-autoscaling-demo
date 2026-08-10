package config

import (
	"os"
	"reflect"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("PROCESSING_DELAY_MS", "")
	t.Setenv("HTTP_PORT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	expectedBrokers := []string{"localhost:9092"}

	if !reflect.DeepEqual(cfg.KafkaBrokers, expectedBrokers) {
		t.Fatalf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, expectedBrokers)
	}

	if cfg.KafkaTopic != "demo-topic" {
		t.Fatalf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "demo-topic")
	}

	if cfg.KafkaConsumerGroup != "demo-consumer-group" {
		t.Fatalf(
			"KafkaConsumerGroup = %q, want %q",
			cfg.KafkaConsumerGroup,
			"demo-consumer-group",
		)
	}

	if cfg.ProcessingDelayMs != 3000 {
		t.Fatalf("ProcessingDelayMs = %d, want 3000", cfg.ProcessingDelayMs)
	}

	if cfg.HTTPPort != 8080 {
		t.Fatalf("HTTPPort = %d, want 8080", cfg.HTTPPort)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-1:9092, broker-2:9092")
	t.Setenv("KAFKA_TOPIC", "orders")
	t.Setenv("KAFKA_CONSUMER_GROUP", "orders-consumer")
	t.Setenv("PROCESSING_DELAY_MS", "1500")
	t.Setenv("HTTP_PORT", "9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	expectedBrokers := []string{"broker-1:9092", "broker-2:9092"}

	if !reflect.DeepEqual(cfg.KafkaBrokers, expectedBrokers) {
		t.Fatalf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, expectedBrokers)
	}

	if cfg.KafkaTopic != "orders" {
		t.Fatalf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "orders")
	}

	if cfg.KafkaConsumerGroup != "orders-consumer" {
		t.Fatalf("KafkaConsumerGroup = %q, want %q", cfg.KafkaConsumerGroup, "orders-consumer")
	}

	if cfg.ProcessingDelayMs != 1500 {
		t.Fatalf("ProcessingDelayMs = %d, want 1500", cfg.ProcessingDelayMs)
	}

	if cfg.HTTPPort != 9090 {
		t.Fatalf("HTTPPort = %d, want 9090", cfg.HTTPPort)
	}
}

func TestLoadInvalidProcessingDelay(t *testing.T) {
	t.Setenv("PROCESSING_DELAY_MS", "not-a-number")

	_, err := Load()

	if err == nil {
		t.Fatal("expected error for invalid PROCESSING_DELAY_MS")
	}
}

func TestLoadInvalidHTTPPort(t *testing.T) {
	t.Setenv("HTTP_PORT", "not-a-number")

	_, err := Load()

	if err == nil {
		t.Fatal("expected error for invalid HTTP_PORT")
	}
}

func TestEnvironmentIsClean(t *testing.T) {
	for _, key := range []string{
		"KAFKA_BROKERS",
		"KAFKA_TOPIC",
		"KAFKA_CONSUMER_GROUP",
		"PROCESSING_DELAY_MS",
		"HTTP_PORT",
	} {
		_ = os.Unsetenv(key)
	}
}
