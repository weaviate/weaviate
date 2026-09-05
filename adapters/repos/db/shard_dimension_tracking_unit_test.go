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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestClearDimensionMetrics(t *testing.T) {
	cfg := IndexConfig{TrackVectorDimensions: true}

	t.Run("zeroes the namespaced series", func(t *testing.T) {
		promMetrics := *monitoring.GetMetrics()
		o := &nodeWideMetricsObserver{db: &DB{promMetrics: &promMetrics}}
		o.sendVectorDimensions("ns_a:Clear", "shard1", "ns_a", DimensionMetrics{Uncompressed: 20, Compressed: 4})

		clearDimensionMetrics(cfg, &promMetrics, "ns_a:Clear", "shard1")

		dims, segs := dimensionGaugesOf(t, "ns_a:Clear", "shard1", "ns_a")
		assert.Zero(t, dims)
		assert.Zero(t, segs)
	})

	t.Run("grouped mode is a no-op", func(t *testing.T) {
		promMetrics := *monitoring.GetMetrics()
		o := &nodeWideMetricsObserver{db: &DB{promMetrics: &promMetrics}}
		o.sendVectorDimensions("ns_a:ClearGrouped", "shard1", "ns_a", DimensionMetrics{Uncompressed: 20})

		// The node total is recomputed from active shards only, so the drop is
		// accounted for on the next observer tick instead of here.
		grouped := promMetrics
		grouped.Group = true
		clearDimensionMetrics(cfg, &grouped, "ns_a:ClearGrouped", "shard1")

		dims, _ := dimensionGaugesOf(t, "ns_a:ClearGrouped", "shard1", "ns_a")
		assert.Equal(t, 20.0, dims)
	})

	t.Run("dimension tracking off is a no-op", func(t *testing.T) {
		promMetrics := *monitoring.GetMetrics()
		o := &nodeWideMetricsObserver{db: &DB{promMetrics: &promMetrics}}
		o.sendVectorDimensions("ns_a:ClearUntracked", "shard1", "ns_a", DimensionMetrics{Uncompressed: 20})

		clearDimensionMetrics(IndexConfig{}, &promMetrics, "ns_a:ClearUntracked", "shard1")

		dims, _ := dimensionGaugesOf(t, "ns_a:ClearUntracked", "shard1", "ns_a")
		assert.Equal(t, 20.0, dims)
	})
}
