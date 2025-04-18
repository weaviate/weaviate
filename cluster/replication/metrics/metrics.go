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

// OpCallbacks contains a set of callback functions that are invoked
// on different stages of a replication operation's lifecycle.
type OpCallbacks struct {
	onOpPending  func(node string)
	onOpStart    func(node string)
	onOpComplete func(node string)
	onOpFailed   func(node string)
}

// OpCallbacksBuilder helps construct an OpCallbacks instance with
// custom behavior for each stage of a replication operation.
type OpCallbacksBuilder struct {
	callbacks OpCallbacks
}

// NewOpCallbacksBuilder initializes a new OpCallbacksBuilder with
// no-op default callbacks.
func NewOpCallbacksBuilder() *OpCallbacksBuilder {
	return &OpCallbacksBuilder{
		callbacks: OpCallbacks{
			onOpPending:  func(node string) {},
			onOpStart:    func(node string) {},
			onOpComplete: func(node string) {},
			onOpFailed:   func(node string) {},
		},
	}
}

// WithOpPendingCallback sets a callback to be executed when a replication
// operation becomes pending for the given node.
func (b *OpCallbacksBuilder) WithOpPendingCallback(callback func(node string)) *OpCallbacksBuilder {
	b.callbacks.onOpPending = callback
	return b
}

// WithOpStartCallback sets a callback to be executed when a replication
// operation starts processing for the given node.
func (b *OpCallbacksBuilder) WithOpStartCallback(callback func(node string)) *OpCallbacksBuilder {
	b.callbacks.onOpStart = callback
	return b
}

// WithOpCompleteCallback sets a callback to be executed when a replication
// operation completes successfully for the given node.
func (b *OpCallbacksBuilder) WithOpCompleteCallback(callback func(node string)) *OpCallbacksBuilder {
	b.callbacks.onOpComplete = callback
	return b
}

// WithOpFailedCallback sets a callback to be executed when a replication
// operation fails for the given node.
func (b *OpCallbacksBuilder) WithOpFailedCallback(callback func(node string)) *OpCallbacksBuilder {
	b.callbacks.onOpFailed = callback
	return b
}

// Build finalizes the configuration and returns the OpCallbacks instance.
func (b *OpCallbacksBuilder) Build() *OpCallbacks {
	return &b.callbacks
}

// OnOpPending invokes the configured callback for when a replication operation becomes pending.
func (m *OpCallbacks) OnOpPending(node string) {
	m.onOpPending(node)
}

// OnOpStart invokes the configured callback for when a replication operation starts.
func (m *OpCallbacks) OnOpStart(node string) {
	m.onOpStart(node)
}

// OnOpComplete invokes the configured callback for when a replication operation completes successfully.
func (m *OpCallbacks) OnOpComplete(node string) {
	m.onOpComplete(node)
}

// OnOpFailed invokes the configured callback for when a replication operation fails.
func (m *OpCallbacks) OnOpFailed(node string) {
	m.onOpFailed(node)
}

// NewReplicationCallbackMetrics creates and registers Prometheus metrics for tracking
// replication operations and returns an OpCallbacks instance configured to update those metrics.
//
// The following metrics are registered with the provided registerer:
// - weaviate_replication_pending_operations (GaugeVec)
// - weaviate_replication_ongoing_operations (GaugeVec)
// - weaviate_replication_complete_operations (CounterVec)
// - weaviate_replication_failed_operations (CounterVec)
//
// All metrics are labeled by node and automatically updated through the callback lifecycle.
func NewReplicationCallbackMetrics(reg prometheus.Registerer) *OpCallbacks {
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

	// Here we define how replication operation metrics are updated during the operation lifecycle:
	//
	// 1. When an operation is **registered as pending**, we increment `replication_pending_operations`.
	// 2. When the operation **starts**, we:
	//    - Decrement `replication_pending_operations` because the op is no longer waiting.
	//    - Increment `replication_ongoing_operations` to reflect that it is now in progress.
	// 3. When the operation **completes successfully**, we:
	//    - Decrement `replication_ongoing_operations` as it’s no longer running.
	//    - Increment `replication_complete_operations` to track the total number of successful ops.
	// 4. When the operation **fails**, we:
	//    - Decrement `replication_ongoing_operations` as the op is no longer running.
	//    - Increment `replication_failed_operations` to track the total number of failures.
	//
	// This ensures that gauges (`pending`, `ongoing`) reflect the current state,
	// while counters (`complete`, `failed`) accumulate totals over time.
	return NewOpCallbacksBuilder().
		WithOpPendingCallback(func(node string) {
			pendingOps.WithLabelValues(node).Inc()
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
		Build()
}
