package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	MessagesProcessed  prometheus.Counter
	MessagesFailed     prometheus.Counter
	ProcessingDuration prometheus.Histogram
}

func New(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		MessagesProcessed: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_consumer_messages_processed_total",
				Help: "Total number of Kafka messages successfully processed.",
			},
		),
		MessagesFailed: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_consumer_messages_failed_total",
				Help: "Total number of Kafka messages that failed processing.",
			},
		),
		ProcessingDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "kafka_consumer_message_processing_duration_seconds",
				Help: "Time spent processing Kafka messages.",
			},
		),
	}

	registry.MustRegister(
		m.MessagesProcessed,
		m.MessagesFailed,
		m.ProcessingDuration,
	)

	return m
}

func (m *Metrics) ObserveProcessing(start time.Time, err error) {
	m.ProcessingDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		m.MessagesFailed.Inc()
		return
	}

	m.MessagesProcessed.Inc()
}
