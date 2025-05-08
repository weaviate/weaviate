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

package metrics_test

import (
	"testing"

	"github.com/weaviate/weaviate/cluster/replication/metrics"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestOpCallbacks(t *testing.T) {
	t.Run("default callbacks should be no-op", func(t *testing.T) {
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().Build()
		callbacks.OnPrepareProcessing("node1")
		callbacks.OnOpPending("node1")
		callbacks.OnOpSkipped("node1")
		callbacks.OnOpStart("node1")
		callbacks.OnOpComplete("node1")
		callbacks.OnOpFailed("node1")
		callbacks.OnOpCancelled("node1")
	})

	t.Run("custom callbacks should be called with correct parameters", func(t *testing.T) {
		// GIVEN
		var (
			prepareProcessingNode string
			pendingNode           string
			skippedNode           string
			startNode             string
			completeNode          string
			failedNode            string
			cancelledNode         string
		)

		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				prepareProcessingNode = node
			}).
			WithOpPendingCallback(func(node string) {
				pendingNode = node
			}).
			WithOpSkippedCallback(func(node string) {
				skippedNode = node
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
			WithOpCancelledCallback(func(node string) {
				cancelledNode = node
			}).
			Build()

		// WHEN
		expectedNode := "test-node"
		callbacks.OnPrepareProcessing(expectedNode)
		callbacks.OnOpPending(expectedNode)
		callbacks.OnOpSkipped(expectedNode)
		callbacks.OnOpStart(expectedNode)
		callbacks.OnOpComplete(expectedNode)
		callbacks.OnOpFailed(expectedNode)
		callbacks.OnOpCancelled(expectedNode)

		// THEN
		require.Equal(t, expectedNode, prepareProcessingNode, "invalid prepare processing callback node")
		require.Equal(t, expectedNode, pendingNode, "invalid pending callback node")
		require.Equal(t, expectedNode, skippedNode, "invalid skipped callback node")
		require.Equal(t, expectedNode, startNode, "invalid start callback node")
		require.Equal(t, expectedNode, completeNode, "invalid complete callback node")
		require.Equal(t, expectedNode, failedNode, "invalid failed callback node")
		require.Equal(t, expectedNode, cancelledNode, "invalid cancelled callback node")
	})

	t.Run("only prepare processing", func(t *testing.T) {
		// GIVEN
		prepareProcessingCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				prepareProcessingCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnPrepareProcessing("node1")

		// THEN
		require.True(t, prepareProcessingCalled, "expected prepare processing callback to be called")
	})

	t.Run("only op pending", func(t *testing.T) {
		// GIVEN
		pendingCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				pendingCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpPending("node1")

		// THEN
		require.True(t, pendingCalled, "expected pending callback to be called")
	})

	t.Run("only op skipped", func(t *testing.T) {
		// GIVEN
		skippedCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpSkippedCallback(func(node string) {
				skippedCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpSkipped("node1")

		// THEN
		require.True(t, skippedCalled, "expected skipped callback to be called")
	})

	t.Run("only op start", func(t *testing.T) {
		// GIVEN
		startCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpStartCallback(func(node string) {
				startCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpStart("node1")

		// THEN
		require.True(t, startCalled, "expected start callback to be called")
	})

	t.Run("only op complete", func(t *testing.T) {
		// GIVEN
		completeCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpCompleteCallback(func(node string) {
				completeCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpComplete("node1")

		// THEN
		require.True(t, completeCalled, "expected complete callback to be called")
	})

	t.Run("only op failed", func(t *testing.T) {
		// GIVEN
		failedCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpFailedCallback(func(node string) {
				failedCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpFailed("node1")

		// THEN
		require.True(t, failedCalled, "expected failed callback to be called")
	})

	t.Run("only op cancelled", func(t *testing.T) {
		// GIVEN
		cancelledCalled := false
		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpCancelledCallback(func(node string) {
				cancelledCalled = true
			}).
			Build()

		// WHEN
		callbacks.OnOpCancelled("node1")

		// THEN
		require.True(t, cancelledCalled, "expected cancelled callback to be called")
	})
}

func TestMetricsCollection(t *testing.T) {
	t.Run("metrics should track operations correctly", func(t *testing.T) {
		// GIVEN
		reg := prometheus.NewPedanticRegistry()
		callbacks := metrics.NewReplicationEngineOpsCallbacks(reg)
		node := "test-node"

		callbacks.OnPrepareProcessing(node)

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

		// Start a fifth operation but skip it
		callbacks.OnOpPending(node)
		callbacks.OnOpSkipped(node)

		// Start a sixth operation but cancel it
		callbacks.OnOpPending(node)
		callbacks.OnOpStart(node)
		callbacks.OnOpCancelled(node)

		// WHEN
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		metricsByName := make(map[string]*io_prometheus_client.MetricFamily)
		for _, mf := range metricFamilies {
			metricsByName[mf.GetName()] = mf
		}

		// THEN
		require.Equal(t, float64(1), metricsByName["weaviate_replication_pending_operations"].GetMetric()[0].GetGauge().GetValue())
		require.Equal(t, float64(1), metricsByName["weaviate_replication_ongoing_operations"].GetMetric()[0].GetGauge().GetValue())
		require.Equal(t, float64(1), metricsByName["weaviate_replication_complete_operations"].GetMetric()[0].GetCounter().GetValue())
		require.Equal(t, float64(1), metricsByName["weaviate_replication_failed_operations"].GetMetric()[0].GetCounter().GetValue())
		require.Equal(t, float64(1), metricsByName["weaviate_replication_cancelled_operations"].GetMetric()[0].GetCounter().GetValue())
	})

	t.Run("metrics should be tracked separately for different nodes", func(t *testing.T) {
		// GIVEN
		reg := prometheus.NewPedanticRegistry()
		callbacks := metrics.NewReplicationEngineOpsCallbacks(reg)
		node1 := "node-1"
		node2 := "node-2"

		callbacks.OnPrepareProcessing(node1)
		callbacks.OnPrepareProcessing(node2)

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

		// Pending operation for node 2 then skipped
		callbacks.OnOpPending(node2)
		callbacks.OnOpSkipped(node2)

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
		require.Equal(t, float64(0), pendingByNode[node1], "invalid pending callback node")
		require.Equal(t, float64(0), ongoingByNode[node1], "invalid ongoing callback node")
		require.Equal(t, float64(1), completeByNode[node1], "invalid complete callback node")
		require.Equal(t, float64(1), failedByNode[node1], "invalid failed callback node")

		// THEN (for node2)
		require.Equal(t, float64(1), pendingByNode[node2], "invalid pending callback node")
		require.Equal(t, float64(0), ongoingByNode[node2], "invalid ongoing callback node")
		require.Equal(t, float64(2), completeByNode[node2], "invalid complete callback node")
		require.Equal(t, float64(0), failedByNode[node2], "invalid failed callback node")
	})
}

func TestEngineCallbacks(t *testing.T) {
	t.Run("default callbacks should be no-op", func(t *testing.T) {
		callbacks := metrics.NewReplicationEngineCallbacksBuilder().Build()
		callbacks.OnEngineStart("node1")
		callbacks.OnEngineStop("node1")
		callbacks.OnProducerStart("node1")
		callbacks.OnProducerStop("node1")
		callbacks.OnConsumerStart("node1")
		callbacks.OnConsumerStop("node1")
	})

	t.Run("custom callbacks should be called with correct parameters", func(t *testing.T) {
		var (
			engineStartedNode, engineStoppedNode                             string
			producerStartedNode, producerStoppedNode                         string
			consumerStartedNode, consumerStoppedNode                         string
			engineStartedCallbacksCounter, engineStoppedCallbacksCounter     int
			producerStartedCallbacksCounter, producerStoppedCallbacksCounter int
			consumerStartedCallbacksCounter, consumerStoppedCallbacksCounter int
		)

		callbacks := metrics.NewReplicationEngineCallbacksBuilder().
			WithEngineStartCallback(func(node string) {
				engineStartedNode = node
				engineStartedCallbacksCounter++
			}).
			WithEngineStopCallback(func(node string) {
				engineStoppedNode = node
				engineStoppedCallbacksCounter++
			}).
			WithProducerStartCallback(func(node string) {
				producerStartedNode = node
				producerStartedCallbacksCounter++
			}).
			WithProducerStopCallback(func(node string) {
				producerStoppedNode = node
				producerStoppedCallbacksCounter++
			}).
			WithConsumerStartCallback(func(node string) {
				consumerStartedNode = node
				consumerStartedCallbacksCounter++
			}).
			WithConsumerStopCallback(func(node string) {
				consumerStoppedNode = node
				consumerStoppedCallbacksCounter++
			}).
			Build()

		node := "node-test"
		callbacks.OnEngineStart(node)
		callbacks.OnEngineStop(node)
		callbacks.OnProducerStart(node)
		callbacks.OnProducerStop(node)
		callbacks.OnConsumerStart(node)
		callbacks.OnConsumerStop(node)

		require.Equal(t, node, engineStartedNode, "invalid node in engine start callback")
		require.Equal(t, node, engineStoppedNode, "invalid node in engine stop callback")
		require.Equal(t, node, producerStartedNode, "invalid node in producer start callback")
		require.Equal(t, node, producerStoppedNode, "invalid node in producer stop callback")
		require.Equal(t, node, consumerStartedNode, "invalid node in consumer start callback")
		require.Equal(t, node, consumerStoppedNode, "invalid node in consumer stop callback")
		require.Equal(t, 1, engineStartedCallbacksCounter, "invalid engine started callback counter")
		require.Equal(t, 1, engineStoppedCallbacksCounter, "invalid engine stop callback counter")
		require.Equal(t, 1, producerStartedCallbacksCounter, "invalid producer start callback counter")
		require.Equal(t, 1, producerStoppedCallbacksCounter, "invalid producer stop callback counter")
		require.Equal(t, 1, consumerStartedCallbacksCounter, "invalid consumer start callback counter")
		require.Equal(t, 1, consumerStoppedCallbacksCounter, "invalid consumer stop callback counter")
	})
}

func TestEngineMetricsCollection(t *testing.T) {
	t.Run("engine lifecycle metrics are tracked correctly", func(t *testing.T) {
		// GIVEN
		reg := prometheus.NewPedanticRegistry()
		callbacks := metrics.NewReplicationEngineCallbacks(reg)
		node := "node1"

		// WHEN
		callbacks.OnEngineStart(node)
		callbacks.OnProducerStart(node)
		callbacks.OnConsumerStart(node)
		afterStartMetricFamilies, err := reg.Gather()

		// THEN
		require.NoError(t, err)
		afterStartMetrics := collectMetrics(afterStartMetricFamilies, node)
		require.Equal(t, float64(1), afterStartMetrics["engine"], "invalid engine running status")
		require.Equal(t, float64(1), afterStartMetrics["producer"], "invalid producer running status")
		require.Equal(t, float64(1), afterStartMetrics["consumer"], "invalid consumer running status")

		// WHEN
		callbacks.OnProducerStop(node)
		callbacks.OnConsumerStop(node)
		callbacks.OnEngineStop(node)
		afterStopMetricFamilies, err := reg.Gather()

		// THEN
		require.NoError(t, err)
		afterStopMetrics := collectMetrics(afterStopMetricFamilies, node)
		require.Equal(t, float64(0), afterStopMetrics["engine"], "invalid engine running status")
		require.Equal(t, float64(0), afterStopMetrics["producer"], "invalid producer running status")
		require.Equal(t, float64(0), afterStopMetrics["consumer"], "invalid consumer running status")
	})
}

func collectMetrics(metricFamilies []*io_prometheus_client.MetricFamily, node string) map[string]float64 {
	values := make(map[string]float64)
	for _, mf := range metricFamilies {
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "node" && label.GetValue() == node {
					switch mf.GetName() {
					case "weaviate_replication_engine_running_status":
						values["engine"] = m.GetGauge().GetValue()
					case "weaviate_replication_engine_producer_running_status":
						values["producer"] = m.GetGauge().GetValue()
					case "weaviate_replication_engine_consumer_running_status":
						values["consumer"] = m.GetGauge().GetValue()
					}
				}
			}
		}
	}
	return values
}
