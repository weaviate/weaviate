package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	BatchTime *prometheus.HistogramVec
}

func NewPrometheusMetrics() *PrometheusMetrics { // TODO don't rely on global state for registration
	return &PrometheusMetrics{
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),
	}
}
