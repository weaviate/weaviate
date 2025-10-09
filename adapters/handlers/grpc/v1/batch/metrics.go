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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ReplicationEngineOpsCallbacks contains a set of callback functions that are invoked
// on different stages of a replication operation's lifecycle.
type BatchStreamingCallbacks struct {
	OnStreamStart   func()
	OnStreamStop    func()
	OnStreamRequest func(queueLen float64)
	OnStreamError   func(streamId string)
	OnWorkerReport  func(streamId string, throughputEma float64, processingTimeEma time.Duration)
}

type BatchStreamingCallbacksBuilder struct {
	callbacks BatchStreamingCallbacks
}

func NewBatchStreamingCallbacksBuilder() *BatchStreamingCallbacksBuilder {
	return &BatchStreamingCallbacksBuilder{
		callbacks: BatchStreamingCallbacks{
			OnStreamStart:   func() {},
			OnStreamStop:    func() {},
			OnStreamRequest: func(queueLen float64) {},
			OnStreamError:   func(streamId string) {},
			OnWorkerReport:  func(streamId string, throughputEma float64, processingTimeEma time.Duration) {},
		},
	}
}

func (b *BatchStreamingCallbacksBuilder) WithStreamStart(callback func()) *BatchStreamingCallbacksBuilder {
	b.callbacks.OnStreamStart = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamStop(callback func()) *BatchStreamingCallbacksBuilder {
	b.callbacks.OnStreamStop = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamRequest(callback func(ratio float64)) *BatchStreamingCallbacksBuilder {
	b.callbacks.OnStreamRequest = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamError(callback func(streamId string)) *BatchStreamingCallbacksBuilder {
	b.callbacks.OnStreamError = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithWorkerReport(callback func(streamId string, throughputEma float64, processingTimeEma time.Duration)) *BatchStreamingCallbacksBuilder {
	b.callbacks.OnWorkerReport = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) Build() *BatchStreamingCallbacks {
	return &b.callbacks
}

func NewBatchStreamingCallbacks(reg prometheus.Registerer) *BatchStreamingCallbacks {
	if reg == nil {
		return nil
	}

	openStreams := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_open_streams",
		Help:      "Number of currently open batch streaming connections",
	}, []string{})

	processingQueueUtilization := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_queue_utilization",
		Help:      "Relative utilization of the batch processing queue",
	}, []string{})

	streamTotalErrors := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_total_errors_per_stream",
		Help:      "Total number of errors encountered during batch streaming operations",
	}, []string{"stream_id"})

	// Note: this is not a histogram of all processing times, but rather the EMA of processing times
	// reported by the workers. As such, it is expected that there are far fewer observations than
	// actual requests processed.
	streamProcessingTimeEma := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_time_ema",
		Help:      "Exponential moving average of the processing time for the internal processing queue",
	}, []string{"stream_id"})
	streamProcessingThroughputEma := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_throughput_ema",
		Help:      "Exponential moving average of the throughput (objects / second) for the internal processing queue",
	}, []string{"stream_id"})

	return NewBatchStreamingCallbacksBuilder().
		WithStreamStart(func() {
			openStreams.WithLabelValues().Inc()
		}).
		WithStreamStop(func() {
			openStreams.WithLabelValues().Dec()
		}).
		WithStreamRequest(func(ratio float64) {
			processingQueueUtilization.WithLabelValues().Observe(ratio)
		}).
		WithStreamError(func(streamId string) {
			streamTotalErrors.WithLabelValues(streamId).Inc()
		}).
		WithWorkerReport(func(streamId string, throughputEma float64, processingTimeEma time.Duration) {
			streamProcessingThroughputEma.WithLabelValues(streamId).Observe(throughputEma)
			streamProcessingTimeEma.WithLabelValues(streamId).Observe(float64(processingTimeEma.Seconds()))
		}).
		Build()
}
