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

//go:build !race

package flat

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/monitoring/metricstest"
)

// flatPrefillCount reads the flat preload series off the default registry.
// It is shared by every test in the binary, so callers compare deltas.
func flatPrefillCount(t *testing.T) uint64 {
	t.Helper()
	n, err := metricstest.SampleCount(prometheus.DefaultGatherer,
		"weaviate_vector_cache_prefill_duration_seconds",
		prometheus.Labels{
			"index_type": string(monitoring.VectorIndexTypeFlat),
			"mode":       string(monitoring.PrefillModeSync),
		})
	require.NoError(t, err)
	return n
}

// An uncached flat index has no vector cache, so PostStartup preloads nothing
// and must not report a prefill.
func Test_NoRace_Flat_UncachedIndexReportsNoPrefill(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	store := loadTestStore(t, dirName)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		ID:                "uncached-prefill-metrics",
		RootPath:          dirName,
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, flatent.UserConfig{}, store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)
	require.NoError(t, index.Add(ctx, 0, []float32{1, 0, 0}))

	before := flatPrefillCount(t)
	index.PostStartup(ctx)

	require.Equal(t, before, flatPrefillCount(t))
}
