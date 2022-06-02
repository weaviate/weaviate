//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	BatchTime       *prometheus.HistogramVec
	AsyncOperations *prometheus.GaugeVec
	LSMSegmentCount *prometheus.GaugeVec
}

func NewPrometheusMetrics() *PrometheusMetrics { // TODO don't rely on global state for registration
	return &PrometheusMetrics{
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),

		AsyncOperations: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "async_operations_running",
			Help: "Number of currently ongoing async operations",
		}, []string{"operation", "class_name", "shard_name", "path"}),

		LSMSegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_active_segments",
			Help: "Number of currently ongoing async operations",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
	}
}
