//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usagegcs

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds all Prometheus metrics for the usage module
type metrics struct {
	// Operation metrics
	OperationTotal   *prometheus.CounterVec
	OperationLatency *prometheus.HistogramVec

	// Resource metrics
	ResourceCount *prometheus.GaugeVec
	ResourceSize  *prometheus.GaugeVec

	UploadedFileSize prometheus.Gauge
}

// NewMetrics creates and registers all Prometheus metrics for the usage module
func NewMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		OperationTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "weaviate_usage_module_operation_total",
				Help: "Total number of usage operations",
			},
			[]string{"operation", "status"}, // operation: collect/upload, status: success/error
		),

		OperationLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "weaviate_usage_module_operation_latency_seconds",
				Help:    "Latency of usage operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"}, // collect/upload
		),

		ResourceCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "weaviate_usage_module_resource_count",
				Help: "Number of resources in the system",
			},
			[]string{"type"}, // type: collections/shards/backups
		),
		UploadedFileSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_usage_module_uploaded_file_size_bytes",
			Help: "Size of the uploaded usage file in bytes",
		}),
	}

	// Register all metrics
	reg.MustRegister(
		m.OperationTotal,
		m.OperationLatency,
		m.ResourceCount,
		m.UploadedFileSize,
	)

	return m
}
