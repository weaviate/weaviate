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

package batch

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BatchStreamingMetrics contains a set of functions that are invoked
// on different stages of the batch streaming process to report metrics.
type BatchStreamingMetrics struct {
	OnStreamStart         func()
	OnStreamStop          func()
	OnStreamError         func(numErrs int)
	OnWorkerReport        func(throughputEma float64)
	OnProcessingQueuePush func(enqueued int)
	OnProcessingQueuePull func(dequeued int)
}

func NewBatchStreamingMetrics(reg prometheus.Registerer) *BatchStreamingMetrics {
	if reg == nil {
		return nil
	}

	openStreams := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_open_streams",
		Help:      "Number of currently open batch streaming connections",
	}, []string{})

	streamTotalErrors := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_total_errors_per_stream",
		Help:      "Total number of errors reported across all streams",
	}, []string{})

	streamProcessingThroughputEma := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_throughput_ema",
		Help:      "Exponential moving average of the throughput (objects / second) for the internal processing queue",
	}, []string{})

	streamEnqueuedObjects := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_enqueued_objects_total",
		Help:      "Total number of objects and references enqueued for processing across all streams",
	}, []string{})

	return &BatchStreamingMetrics{
		OnStreamStart: func() {
			openStreams.WithLabelValues().Inc()
		},
		OnStreamStop: func() {
			openStreams.WithLabelValues().Dec()
		},
		OnStreamError: func(numErrs int) {
			streamTotalErrors.WithLabelValues().Add(float64(numErrs))
		},
		OnWorkerReport: func(throughputEma float64) {
			streamProcessingThroughputEma.WithLabelValues().Observe(throughputEma)
		},
		OnProcessingQueuePush: func(enqueued int) {
			streamEnqueuedObjects.WithLabelValues().Add(float64(enqueued))
		},
		OnProcessingQueuePull: func(dequeued int) {
			streamEnqueuedObjects.WithLabelValues().Add(-float64(dequeued))
		},
	}
}
