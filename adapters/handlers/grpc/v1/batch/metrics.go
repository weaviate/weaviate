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
	onStreamStart     func()
	onStreamStop      func()
	onStreamRequest   func(streamId string, queueLen float64)
	onStreamError     func(streamId string)
	onSchedulerReport func(streamId string, currentBatchSize int, processingTimeEma time.Duration)
}

type BatchStreamingCallbacksBuilder struct {
	callbacks BatchStreamingCallbacks
}

func NewBatchStreamingCallbacksBuilder() *BatchStreamingCallbacksBuilder {
	return &BatchStreamingCallbacksBuilder{
		callbacks: BatchStreamingCallbacks{
			onStreamStart:     func() {},
			onStreamStop:      func() {},
			onStreamRequest:   func(streamId string, queueLen float64) {},
			onStreamError:     func(streamId string) {},
			onSchedulerReport: func(streamId string, currentBatchSize int, processingTimeEma time.Duration) {},
		},
	}
}

func (b *BatchStreamingCallbacksBuilder) WithStreamStart(callback func()) *BatchStreamingCallbacksBuilder {
	b.callbacks.onStreamStart = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamStop(callback func()) *BatchStreamingCallbacksBuilder {
	b.callbacks.onStreamStop = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamRequest(callback func(streamId string, queueLen float64)) *BatchStreamingCallbacksBuilder {
	b.callbacks.onStreamRequest = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithStreamError(callback func(streamId string)) *BatchStreamingCallbacksBuilder {
	b.callbacks.onStreamError = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) WithSchedulerReport(callback func(streamId string, currentBatchSize int, processingTimeEma time.Duration)) *BatchStreamingCallbacksBuilder {
	b.callbacks.onSchedulerReport = callback
	return b
}

func (b *BatchStreamingCallbacksBuilder) Build() *BatchStreamingCallbacks {
	return &b.callbacks
}

func (m *BatchStreamingCallbacks) OnStreamStart() {
	m.onStreamStart()
}

func (m *BatchStreamingCallbacks) OnStreamStop() {
	m.onStreamStop()
}

func (m *BatchStreamingCallbacks) OnStreamRequest(streamId string, queueLen float64) {
	m.onStreamRequest(streamId, queueLen)
}

func (m *BatchStreamingCallbacks) OnStreamError(streamId string) {
	m.onStreamError(streamId)
}

func (m *BatchStreamingCallbacks) OnSchedulerReport(streamId string, currentBatchSize int, processingTimeEma time.Duration) {
	m.onSchedulerReport(streamId, currentBatchSize, processingTimeEma)
}

func NewBatchStreamingCallbacks(reg prometheus.Registerer) *BatchStreamingCallbacks {
	openStreams := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_open_streams",
		Help:      "Number of currently open batch streaming connections",
	}, []string{})

	streamWriteQueueSizeEma := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_write_queue_size_ema",
		Help:      "Exponential moving average of the write queue size per stream",
	}, []string{"stream_id"})

	streamTotalErrors := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_total_errors",
		Help:      "Total number of errors encountered during batch streaming operations",
	}, []string{"stream_id"})

	streamProcessingBatchSize := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_batch_size",
		Help:      "Current batch size used by the internal processing queue",
	}, []string{"stream_id"})
	// Note: this is not a histogram of all processing times, but rather the EMA of processing times
	// reported by the workers. As such, it is expected that there are far fewer observations than
	// actual requests processed.
	streamProcessingTimeEma := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      "batch_streaming_processing_time_ema",
		Help:      "Exponential moving average of the processing time for the internal processing queue",
	}, []string{"stream_id"})

	return NewBatchStreamingCallbacksBuilder().
		WithStreamStart(func() {
			openStreams.WithLabelValues().Inc()
		}).
		WithStreamStop(func() {
			openStreams.WithLabelValues().Dec()
		}).
		WithStreamRequest(func(streamId string, queueLen float64) {
			streamWriteQueueSizeEma.WithLabelValues(streamId).Observe(queueLen)
		}).
		WithStreamError(func(streamId string) {
			streamTotalErrors.WithLabelValues(streamId).Inc()
		}).
		WithSchedulerReport(func(streamId string, currentBatchSize int, processingTimeEma time.Duration) {
			streamProcessingBatchSize.WithLabelValues(streamId).Set(float64(currentBatchSize))
			streamProcessingTimeEma.WithLabelValues(streamId).Observe(float64(processingTimeEma.Seconds()))
		}).
		Build()
}
