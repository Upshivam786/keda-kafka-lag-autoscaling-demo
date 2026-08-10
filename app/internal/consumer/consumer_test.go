package consumer

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestProcessMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handler := &Handler{
		delay:  0,
		logger: logger,
	}

	message := &sarama.ConsumerMessage{
		Topic:     "demo-topic",
		Partition: 0,
		Offset:    42,
		Value:     []byte("test-message"),
	}

	if err := handler.processMessage(message); err != nil {
		t.Fatalf("processMessage() returned error: %v", err)
	}
}

func TestProcessMessageAppliesDelay(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	delay := 50 * time.Millisecond

	handler := &Handler{
		delay:  delay,
		logger: logger,
	}

	message := &sarama.ConsumerMessage{
		Topic:     "demo-topic",
		Partition: 0,
		Offset:    42,
		Value:     []byte("test-message"),
	}

	start := time.Now()

	if err := handler.processMessage(message); err != nil {
		t.Fatalf("processMessage() returned error: %v", err)
	}

	elapsed := time.Since(start)

	if elapsed < delay {
		t.Fatalf(
			"processing completed too quickly: elapsed=%v, expected at least %v",
			elapsed,
			delay,
		)
	}
}
