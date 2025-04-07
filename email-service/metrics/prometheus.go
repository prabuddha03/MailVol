package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EmailsSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "emails_sent_total",
			Help: "Total number of emails sent",
		},
		[]string{"status", "template"},
	)

	EmailRenderDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "email_render_duration_seconds",
			Help:    "Time taken to render email templates",
			Buckets: prometheus.DefBuckets,
		},
	)

	KafkaConsumeLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kafka_consume_latency_seconds",
			Help:    "Time taken to consume messages from Kafka",
			Buckets: prometheus.DefBuckets,
		},
	)

	SESSendDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ses_send_duration_seconds",
			Help:    "Time taken to send emails via SES",
			Buckets: prometheus.DefBuckets,
		},
	)
)
