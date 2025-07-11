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

package usage

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	// Operation metrics
	OperationTotal   *prometheus.CounterVec
	OperationLatency *prometheus.HistogramVec

	// Resource metrics
	ResourceCount    *prometheus.GaugeVec
	UploadedFileSize prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer, moduleName string) *Metrics {
	moduleName = fmt.Sprintf("weaviate_%s", strings.ReplaceAll(strings.ToLower(moduleName), "-", "_"))
	return &Metrics{
		OperationTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: moduleName + "_operations_total",
				Help: "Total number of " + moduleName + " operations",
			},
			[]string{"operation", "status"}, // operation: collect/upload, status: success/error
		),
		OperationLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    moduleName + "_operation_latency_seconds",
				Help:    "Latency of usage operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"}, // collect/upload
		),
		ResourceCount: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: moduleName + "_resource_count",
				Help: "Number of resources tracked by " + moduleName,
			},
			[]string{"resource_type"}, // type: collections/shards/backups
		),
		UploadedFileSize: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: moduleName + "_uploaded_file_size_bytes",
				Help: "Size of the last uploaded usage file in bytes",
			},
		),
	}
}
