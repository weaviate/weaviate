//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ReplicationEngineOpsCallbacks contains a set of callback functions that are invoked
// on different stages of a replication operation's lifecycle.
type ReplicationEngineOpsCallbacks struct {
	onPrepareProcessing func(node string)
	onOpPending         func(node string)
	onOpSkipped         func(node string)
	onOpStart           func(node string)
	onOpComplete        func(node string)
	onOpFailed          func(node string)
	onOpCancelled       func(node string)
}

// ReplicationEngineOpsCallbacksBuilder helps construct an ReplicationEngineOpsCallbacks instance with
// custom behavior for each stage of a replication operation.
type ReplicationEngineOpsCallbacksBuilder struct {
	callbacks ReplicationEngineOpsCallbacks
}

// NewReplicationEngineOpsCallbacksBuilder initializes a new ReplicationEngineOpsCallbacksBuilder with
// no-op default callbacks.
func NewReplicationEngineOpsCallbacksBuilder() *ReplicationEngineOpsCallbacksBuilder {
	return &ReplicationEngineOpsCallbacksBuilder{
		callbacks: ReplicationEngineOpsCallbacks{
			onPrepareProcessing: func(node string) {},
			onOpPending:         func(node string) {},
			onOpSkipped:         func(node string) {},
			onOpStart:           func(node string) {},
			onOpComplete:        func(node string) {},
			onOpFailed:          func(node string) {},
			onOpCancelled:       func(node string) {},
		},
	}
}

// WithPrepareProcessing sets a callback to be executed before starting to process replication operations
// for a given node. This can be used to initialize metrics like counters and gauges to ensure they are
// exposed with an initial value, avoiding gaps when the engine starts.
func (b *ReplicationEngineOpsCallbacksBuilder) WithPrepareProcessing(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onPrepareProcessing = callback
	return b
}

// WithOpPendingCallback sets a callback to be executed when a replication
// operation becomes pending for the given node.
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpPendingCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpPending = callback
	return b
}

// WithOpSkippedCallback sets a callback to be executed when a replication
// operation is skipped because already running or completed execution (successfully or with a failure)
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpSkippedCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpSkipped = callback
	return b
}

// WithOpStartCallback sets a callback to be executed when a replication
// operation starts processing for the given node.
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpStartCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpStart = callback
	return b
}

// WithOpCompleteCallback sets a callback to be executed when a replication
// operation completes successfully for the given node.
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpCompleteCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpComplete = callback
	return b
}

// WithOpFailedCallback sets a callback to be executed when a replication
// operation fails for the given node.
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpFailedCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpFailed = callback
	return b
}

// WithOpCancelledCallback sets a callback to be executed when a replication
// operation is cancelled for the given node.
func (b *ReplicationEngineOpsCallbacksBuilder) WithOpCancelledCallback(callback func(node string)) *ReplicationEngineOpsCallbacksBuilder {
	b.callbacks.onOpCancelled = callback
	return b
}

// Build finalizes the configuration and returns the ReplicationEngineOpsCallbacks instance.
func (b *ReplicationEngineOpsCallbacksBuilder) Build() *ReplicationEngineOpsCallbacks {
	return &b.callbacks
}

func (m *ReplicationEngineOpsCallbacks) OnPrepareProcessing(node string) {
	m.onPrepareProcessing(node)
}

// OnOpPending invokes the configured callback for when a replication operation becomes pending.
func (m *ReplicationEngineOpsCallbacks) OnOpPending(node string) {
	m.onOpPending(node)
}

// OnOpSkipped invokes the configured callback when a replication operation is skipped
func (m *ReplicationEngineOpsCallbacks) OnOpSkipped(node string) {
	m.onOpSkipped(node)
}

// OnOpStart invokes the configured callback for when a replication operation starts.
func (m *ReplicationEngineOpsCallbacks) OnOpStart(node string) {
	m.onOpStart(node)
}

// OnOpComplete invokes the configured callback for when a replication operation completes successfully.
func (m *ReplicationEngineOpsCallbacks) OnOpComplete(node string) {
	m.onOpComplete(node)
}

// OnOpFailed invokes the configured callback for when a replication operation fails.
func (m *ReplicationEngineOpsCallbacks) OnOpFailed(node string) {
	m.onOpFailed(node)
}

// OnOpCancelled invokes the configured callback for when a replication operation is cancelled.
func (m *ReplicationEngineOpsCallbacks) OnOpCancelled(node string) {
	m.onOpCancelled(node)
}

// NewReplicationEngineOpsCallbacks creates and registers Prometheus metrics for tracking
// replication operations and returns a ReplicationEngineOpsCallbacks instance configured to update those metrics.
//
// The following metrics are registered with the provided registerer:
// - weaviate_replication_pending_operations (GaugeVec)
// - weaviate_replication_ongoing_operations (GaugeVec)
// - weaviate_replication_complete_operations (CounterVec)
// - weaviate_replication_failed_operations (CounterVec)
// - weaviate_replication_cancelled_operations (CounterVec)
//
// All metrics are labeled by node and automatically updated through the callback lifecycle.
//
// The operation lifecycle and corresponding metric updates are as follows:
// 1. When an operation is **registered as pending**, increment `replication_pending_operations`.
// 2. When (and if) an operations is **skipped** (cancelled before starting), decrement `replication_pending_operations`.
// 3. When an operation **starts**, decrement `replication_pending_operations` and increment `replication_ongoing_operations`.
// 4. When an operation **completes successfully**, decrement `replication_ongoing_operations` and increment `replication_complete_operations`.
// 5. When an operation **fails**, decrement `replication_ongoing_operations` and increment `replication_failed_operations`.
//
// This ensures that gauges (`pending`, `ongoing`) reflect the current number of active operations,
// while counters (`complete`, `failed`) accumulate totals over time.
func NewReplicationEngineOpsCallbacks(reg prometheus.Registerer) *ReplicationEngineOpsCallbacks {
	pendingOps := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_pending_operations",
		Help:      "Number of replication operations pending processing",
	}, []string{"node"})

	ongoingOps := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_ongoing_operations",
		Help:      "Number of replication operations currently in progress",
	}, []string{"node"})

	completeOps := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "replication_complete_operations",
		Help:      "Number of successfully completed replication operations",
	}, []string{"node"})

	failedOps := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "replication_failed_operations",
		Help:      "Number of failed replication operations",
	}, []string{"node"})

	cancelledOps := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "replication_cancelled_operations",
		Help:      "Number of cancelled replication operations",
	}, []string{"node"})

	return NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			// Add(0) is used to ensure that the metric exists for the given node label
			// and will be scraped by Prometheus even before any real increment happens.
			// This avoids gaps and allows queries like increase() and rate() to work correctly
			// from startup.
			pendingOps.WithLabelValues(node).Add(0)
			ongoingOps.WithLabelValues(node).Add(0)
			completeOps.WithLabelValues(node).Add(0)
			failedOps.WithLabelValues(node).Add(0)
		}).
		WithOpPendingCallback(func(node string) {
			pendingOps.WithLabelValues(node).Inc()
		}).
		WithOpSkippedCallback(func(node string) {
			pendingOps.WithLabelValues(node).Dec()
		}).
		WithOpStartCallback(func(node string) {
			pendingOps.WithLabelValues(node).Dec()
			ongoingOps.WithLabelValues(node).Inc()
		}).
		WithOpCompleteCallback(func(node string) {
			ongoingOps.WithLabelValues(node).Dec()
			completeOps.WithLabelValues(node).Inc()
		}).
		WithOpFailedCallback(func(node string) {
			ongoingOps.WithLabelValues(node).Dec()
			failedOps.WithLabelValues(node).Inc()
		}).
		WithOpCancelledCallback(func(node string) {
			ongoingOps.WithLabelValues(node).Dec()
			cancelledOps.WithLabelValues(node).Inc()
		}).
		Build()
}

// ReplicationEngineCallbacks contains a set of callback functions that are invoked
// during the lifecycle of the replication engine and its internal components.
//
// These callbacks allow external systems to react to state transitions in the
// engine, producer, and consumer.
type ReplicationEngineCallbacks struct {
	onEngineStart   func(node string)
	onEngineStop    func(node string)
	onProducerStart func(node string)
	onProducerStop  func(node string)
	onConsumerStart func(node string)
	onConsumerStop  func(node string)
}

// ReplicationEngineCallbacksBuilder helps construct an ReplicationEngineCallbacks instance
// by allowing selective customization of lifecycle hooks.
//
// All callbacks default to no-ops unless explicitly overridden.
type ReplicationEngineCallbacksBuilder struct {
	callbacks ReplicationEngineCallbacks
}

// NewReplicationEngineCallbacksBuilder initializes a new ReplicationEngineCallbacksBuilder with
// default no-op functions for all lifecycle callbacks.
func NewReplicationEngineCallbacksBuilder() *ReplicationEngineCallbacksBuilder {
	return &ReplicationEngineCallbacksBuilder{
		callbacks: ReplicationEngineCallbacks{
			onEngineStart:   func(node string) {},
			onEngineStop:    func(node string) {},
			onProducerStart: func(node string) {},
			onProducerStop:  func(node string) {},
			onConsumerStart: func(node string) {},
			onConsumerStop:  func(node string) {},
		},
	}
}

// WithEngineStartCallback sets the callback to be executed when the replication engine starts.
func (b *ReplicationEngineCallbacksBuilder) WithEngineStartCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onEngineStart = callback
	return b
}

// WithEngineStopCallback sets the callback to be executed when the replication engine stops.
func (b *ReplicationEngineCallbacksBuilder) WithEngineStopCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onEngineStop = callback
	return b
}

// WithProducerStartCallback sets the callback to be executed when the replication engine's producer starts.
func (b *ReplicationEngineCallbacksBuilder) WithProducerStartCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onProducerStart = callback
	return b
}

// WithProducerStopCallback sets the callback to be executed when the replication engine's producer stops.
func (b *ReplicationEngineCallbacksBuilder) WithProducerStopCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onProducerStop = callback
	return b
}

// WithConsumerStartCallback sets the callback to be executed when the replication engine's consumer starts.
func (b *ReplicationEngineCallbacksBuilder) WithConsumerStartCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onConsumerStart = callback
	return b
}

// WithConsumerStopCallback sets the callback to be executed when the replication engine's consumer stops.
func (b *ReplicationEngineCallbacksBuilder) WithConsumerStopCallback(callback func(node string)) *ReplicationEngineCallbacksBuilder {
	b.callbacks.onConsumerStop = callback
	return b
}

// Build finalizes the builder and returns the ReplicationEngineCallbacks instance.
func (b *ReplicationEngineCallbacksBuilder) Build() *ReplicationEngineCallbacks {
	return &b.callbacks
}

// OnEngineStart invokes the configured callback for when the engine starts.
func (m *ReplicationEngineCallbacks) OnEngineStart(node string) {
	m.onEngineStart(node)
}

// OnEngineStop invokes the configured callback for when the engine stops.
func (m *ReplicationEngineCallbacks) OnEngineStop(node string) {
	m.onEngineStop(node)
}

// OnProducerStart invokes the configured callback for when the producer starts.
func (m *ReplicationEngineCallbacks) OnProducerStart(node string) {
	m.onProducerStart(node)
}

// OnProducerStop invokes the configured callback for when the producer stops.
func (m *ReplicationEngineCallbacks) OnProducerStop(node string) {
	m.onProducerStop(node)
}

// OnConsumerStart invokes the configured callback for when the consumer starts.
func (m *ReplicationEngineCallbacks) OnConsumerStart(node string) {
	m.onConsumerStart(node)
}

// OnConsumerStop invokes the configured callback for when the consumer stops.
func (m *ReplicationEngineCallbacks) OnConsumerStop(node string) {
	m.onConsumerStop(node)
}

// NewReplicationEngineCallbacks creates and registers Prometheus metrics
// to track the lifecycle status of the replication engine and its internal components.
//
// It returns an ReplicationEngineCallbacks instance that updates the following metrics:
// - weaviate_replication_engine_running_status (GaugeVec)
// - weaviate_replication_engine_producer_running_status (GaugeVec)
// - weaviate_replication_engine_consumer_running_status (GaugeVec)
//
// All metrics are labeled by node and reflect the current running state:
// - 1 = running
// - 0 = not running
//
// This provides visibility into whether the engine, producer, or consumer
// is currently active on a per-node basis.
func NewReplicationEngineCallbacks(reg prometheus.Registerer) *ReplicationEngineCallbacks {
	engineRunning := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_engine_running_status",
		Help:      "The Replication engine running status (0: not running, 1: running)",
	}, []string{"node"})

	producerRunning := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_engine_producer_running_status",
		Help:      "The replication engine producer running status (0: not running, 1: running)",
	}, []string{"node"})

	consumerRunning := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_engine_consumer_running_status",
		Help:      "The replication engine consumer running status (0: not running, 1: running)",
	}, []string{"node"})

	return NewReplicationEngineCallbacksBuilder().
		WithEngineStartCallback(func(node string) { engineRunning.WithLabelValues(node).Set(1) }).
		WithEngineStopCallback(func(node string) { engineRunning.WithLabelValues(node).Set(0) }).
		WithProducerStartCallback(func(node string) { producerRunning.WithLabelValues(node).Set(1) }).
		WithProducerStopCallback(func(node string) { producerRunning.WithLabelValues(node).Set(0) }).
		WithConsumerStartCallback(func(node string) { consumerRunning.WithLabelValues(node).Set(1) }).
		WithConsumerStopCallback(func(node string) { consumerRunning.WithLabelValues(node).Set(0) }).
		Build()
}
