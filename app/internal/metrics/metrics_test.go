package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var errTestProcessing = errors.New("test processing error")

func TestObserveProcessingSuccess(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := New(registry)

	start := time.Now().Add(-10 * time.Millisecond)

	m.ObserveProcessing(start, nil)

	processed := testutil.ToFloat64(m.MessagesProcessed)

	if processed != 1 {
		t.Fatalf("MessagesProcessed = %v, want 1", processed)
	}

	failed := testutil.ToFloat64(m.MessagesFailed)

	if failed != 0 {
		t.Fatalf("MessagesFailed = %v, want 0", failed)
	}
}

func TestObserveProcessingFailure(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := New(registry)

	start := time.Now().Add(-10 * time.Millisecond)

	m.ObserveProcessing(start, errTestProcessing)

	processed := testutil.ToFloat64(m.MessagesProcessed)

	if processed != 0 {
		t.Fatalf("MessagesProcessed = %v, want 0", processed)
	}

	failed := testutil.ToFloat64(m.MessagesFailed)

	if failed != 1 {
		t.Fatalf("MessagesFailed = %v, want 1", failed)
	}
}
