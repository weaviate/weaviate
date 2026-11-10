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

package hfresh

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// StoreRemoveDuration records under the "remove" store operation, and a
// Metrics without Prometheus ignores it.
func TestStoreRemoveDuration(t *testing.T) {
	prom := monitoring.GetMetrics()
	// a label of its own per run: the registry is the process's
	class := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	NewMetrics(prom, class, "shard").StoreRemoveDuration(time.Now())

	obs, err := prom.VectorIndexStoreOperationsDurations.GetMetricWith(prometheus.Labels{
		"class_name": class, "shard_name": "shard", "operation": "remove",
	})
	require.NoError(t, err)
	var out dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&out))
	require.Equal(t, uint64(1), out.GetSummary().GetSampleCount())

	NewMetrics(nil, class, "shard").StoreRemoveDuration(time.Now())
}

// The cleanup metrics count and time under the "cleanup" background operation.
func TestCleanupMetrics(t *testing.T) {
	prom := monitoring.GetMetrics()
	// a label of its own per run: the registry is the process's
	class := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	labels := prometheus.Labels{"class_name": class, "shard_name": "shard", "operation": "cleanup"}
	m := NewMetrics(prom, class, "shard")
	m.IncCleanupCount()
	m.CleanupDuration(time.Now())

	gauge, err := prom.VectorIndexBackgroundOperationsCount.GetMetricWith(labels)
	require.NoError(t, err)
	var count dto.Metric
	require.NoError(t, gauge.(prometheus.Metric).Write(&count))
	require.Equal(t, float64(1), count.GetGauge().GetValue())

	obs, err := prom.VectorIndexBackgroundOperationsDurations.GetMetricWith(labels)
	require.NoError(t, err)
	var duration dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&duration))
	require.NotZero(t, duration.GetSummary().GetSampleCount()+duration.GetHistogram().GetSampleCount())

	disabled := NewMetrics(nil, class, "shard")
	disabled.IncCleanupCount()
	disabled.CleanupDuration(time.Now())
}
