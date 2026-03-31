//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package export

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ExportMetrics holds Prometheus metrics for the export subsystem.
type ExportMetrics struct {
	ObjectsTotal         prometheus.Counter
	OperationsTotal      *prometheus.CounterVec
	CoordinationDuration prometheus.Histogram
	Duration             prometheus.Histogram
}

// NewExportMetrics creates and registers export metrics with the given registerer.
func NewExportMetrics(reg prometheus.Registerer) *ExportMetrics {
	r := promauto.With(reg)
	return &ExportMetrics{
		ObjectsTotal: r.NewCounter(prometheus.CounterOpts{
			Name: "weaviate_export_objects_total",
			Help: "Total number of objects exported.",
		}),
		OperationsTotal: r.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_export_operations_total",
			Help: "Total number of export operations by terminal status.",
		}, []string{"status"}),
		CoordinationDuration: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_export_coordination_duration_seconds",
			Help:    "Duration of the export two-phase commit coordination (prepare all nodes, write metadata, commit all nodes). Does not include the scan and upload phase.",
			Buckets: []float64{1, 5, 10, 15, 30, 45, 60},
		}),
		Duration: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_export_duration_seconds",
			Help:    "Duration of the export scan and upload phase on a participant node.",
			Buckets: []float64{10, 30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400},
		}),
	}
}
