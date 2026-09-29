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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The version-map warmup is hfresh's post-startup cache fill; it always runs
// in the background, so it reports under (hfresh, async). The series lives on
// the default registry shared by every test, hence the delta.
func TestWarmVersionMapReportsPrefill(t *testing.T) {
	const total = 20
	vectors, _ := testinghelpers.RandomVecs(total, 1, 32)

	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk("",
		func(ctx context.Context, id uint64, _ string) ([]float32, error) {
			return vectors[id], nil
		})
	index := makeHFreshWithConfig(t, store, cfg, uc)

	ctx := t.Context()
	for i := range total {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	labels := prometheus.Labels{
		"index_type": string(monitoring.VectorIndexTypeHFresh),
		"mode":       string(monitoring.PrefillModeAsync),
	}
	count := func() uint64 {
		n, err := monitoring.HistogramSampleCount(prometheus.DefaultGatherer,
			"weaviate_vector_cache_prefill_duration_seconds", labels)
		require.NoError(t, err)
		return n
	}
	active := func() float64 {
		v, err := monitoring.GaugeValue(prometheus.DefaultGatherer,
			"weaviate_vector_cache_prefill_active", labels)
		require.NoError(t, err)
		return v
	}

	before, activeBefore := count(), active()
	index.warmVersionMap()

	require.Equal(t, before+1, count(), "a completed warmup is observed once")
	require.Equal(t, activeBefore, active(), "a completed warmup is no longer active")
}
