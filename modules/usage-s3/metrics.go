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

package usages3

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	OperationTotal   *prometheus.CounterVec
	ResourceCount    *prometheus.GaugeVec
	UploadedFileSize prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		OperationTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "usage_s3_operations_total",
				Help: "Total number of usage-s3 operations",
			},
			[]string{"operation", "status"},
		),
		ResourceCount: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "usage_s3_resource_count",
				Help: "Number of resources tracked by usage-s3",
			},
			[]string{"resource_type"},
		),
		UploadedFileSize: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "usage_s3_uploaded_file_size_bytes",
				Help: "Size of the last uploaded usage file in bytes",
			},
		),
	}
}
