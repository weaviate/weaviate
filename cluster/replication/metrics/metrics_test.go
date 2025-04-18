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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpCallbacks(t *testing.T) {
	t.Run("default callbacks should be no-op", func(t *testing.T) {
		callbacks := NewOpCallbacksBuilder().Build()
		callbacks.OnOpPending("node1")
		callbacks.OnOpStart("node1")
		callbacks.OnOpComplete("node1")
		callbacks.OnOpFailed("node1")
	})

	t.Run("custom callbacks should be called with correct parameters", func(t *testing.T) {
		// GIVEN
		var (
			pendingNode  string
			startNode    string
			completeNode string
			failedNode   string
		)

		callbacks := NewOpCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				pendingNode = node
			}).
			WithOpStartCallback(func(node string) {
				startNode = node
			}).
			WithOpCompleteCallback(func(node string) {
				completeNode = node
			}).
			WithOpFailedCallback(func(node string) {
				failedNode = node
			}).
			Build()

		// WHEN
		expectedNode := "test-node"
		callbacks.OnOpPending(expectedNode)
		callbacks.OnOpStart(expectedNode)
		callbacks.OnOpComplete(expectedNode)
		callbacks.OnOpFailed(expectedNode)

		// THEN
		assert.Equal(t, expectedNode, pendingNode)
		assert.Equal(t, expectedNode, startNode)
		assert.Equal(t, expectedNode, completeNode)
		assert.Equal(t, expectedNode, failedNode)
	})

	t.Run("only op pending", func(t *testing.T) {
		// GIVEN
		pendingCalled := false
		callbacks := NewOpCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				pendingCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpPending("node1")

		// THEN
		assert.True(t, pendingCalled)
	})

	t.Run("only op start", func(t *testing.T) {
		// GIVEN
		startCalled := false
		callbacks := NewOpCallbacksBuilder().
			WithOpStartCallback(func(node string) {
				startCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpStart("node1")

		// THEN
		assert.True(t, startCalled)
	})

	t.Run("only op complete", func(t *testing.T) {
		// GIVEN
		completeCalled := false
		callbacks := NewOpCallbacksBuilder().
			WithOpCompleteCallback(func(node string) {
				completeCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpComplete("node1")

		// THEN
		assert.True(t, completeCalled)
	})

	t.Run("only op failed", func(t *testing.T) {
		// GIVEN
		failedCalled := false
		callbacks := NewOpCallbacksBuilder().
			WithOpFailedCallback(func(node string) {
				failedCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpFailed("node1")

		// THEN
		assert.True(t, failedCalled)
	})
}

func TestMetricsCollection(t *testing.T) {
	t.Run("metrics should track operations correctly", func(t *testing.T) {
		// GIVEN
		reg := prometheus.NewRegistry()
		callbacks := NewReplicationCallbackMetrics(reg)
		node := "test-node"

		// Process first operation completing successfully
		callbacks.OnOpPending(node)
		callbacks.OnOpStart(node)
		callbacks.OnOpComplete(node)

		// Process second operation completing with a failure
		callbacks.OnOpPending(node)
		callbacks.OnOpStart(node)
		callbacks.OnOpFailed(node) // This one fails

		// Start a third operation but leave it running
		callbacks.OnOpPending(node)
		callbacks.OnOpStart(node)

		// Start a fourth operation but leave it pending
		callbacks.OnOpPending(node)

		// WHEN
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		metrics := make(map[string]*io_prometheus_client.MetricFamily)
		for _, mf := range metricFamilies {
			metrics[mf.GetName()] = mf
		}

		// THEN
		assert.Equal(t, float64(1), metrics["weaviate_replication_pending_operations"].GetMetric()[0].GetGauge().GetValue())
		assert.Equal(t, float64(1), metrics["weaviate_replication_ongoing_operations"].GetMetric()[0].GetGauge().GetValue())
		assert.Equal(t, float64(1), metrics["weaviate_replication_complete_operations"].GetMetric()[0].GetCounter().GetValue())
		assert.Equal(t, float64(1), metrics["weaviate_replication_failed_operations"].GetMetric()[0].GetCounter().GetValue())
	})

	t.Run("metrics should be tracked separately for different nodes", func(t *testing.T) {
		// GIVEN
		reg := prometheus.NewRegistry()
		callbacks := NewReplicationCallbackMetrics(reg)
		node1 := "node-1"
		node2 := "node-2"

		// Node 1 ops
		callbacks.OnOpPending(node1)
		callbacks.OnOpStart(node1)
		callbacks.OnOpComplete(node1)

		callbacks.OnOpPending(node1)
		callbacks.OnOpStart(node1)
		callbacks.OnOpFailed(node1)

		// Node 2 ops
		callbacks.OnOpPending(node2)
		callbacks.OnOpStart(node2)
		callbacks.OnOpComplete(node2)

		callbacks.OnOpPending(node2)
		callbacks.OnOpStart(node2)
		callbacks.OnOpComplete(node2)

		// Pending operation for node 2
		callbacks.OnOpPending(node2)

		// WHEN
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		pendingByNode := make(map[string]float64)
		ongoingByNode := make(map[string]float64)
		completeByNode := make(map[string]float64)
		failedByNode := make(map[string]float64)

		for _, mf := range metricFamilies {
			for _, m := range mf.GetMetric() {
				var nodeLabel string
				for _, labelPair := range m.GetLabel() {
					if labelPair.GetName() == "node" {
						nodeLabel = labelPair.GetValue()
						break
					}
				}

				switch mf.GetName() {
				case "weaviate_replication_pending_operations":
					pendingByNode[nodeLabel] = m.GetGauge().GetValue()
				case "weaviate_replication_ongoing_operations":
					ongoingByNode[nodeLabel] = m.GetGauge().GetValue()
				case "weaviate_replication_complete_operations":
					completeByNode[nodeLabel] = m.GetCounter().GetValue()
				case "weaviate_replication_failed_operations":
					failedByNode[nodeLabel] = m.GetCounter().GetValue()
				}
			}
		}

		// THEN (for node1)
		assert.Equal(t, float64(0), pendingByNode[node1])
		assert.Equal(t, float64(0), ongoingByNode[node1])
		assert.Equal(t, float64(1), completeByNode[node1])
		assert.Equal(t, float64(1), failedByNode[node1])

		// THEN (for node2)
		assert.Equal(t, float64(1), pendingByNode[node2])
		assert.Equal(t, float64(0), ongoingByNode[node2])
		assert.Equal(t, float64(2), completeByNode[node2])
		assert.Equal(t, float64(0), failedByNode[node2])
	})
}
