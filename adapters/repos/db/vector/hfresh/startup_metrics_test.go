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
	monitoringhelpers "github.com/weaviate/weaviate/usecases/monitoring/testinghelpers"
)

// The version-map warmup is hfresh's post-startup cache fill; it always runs
// in the background, so it reports under (hfresh, async). The series live on
// the default registry shared by every test, hence the deltas.

var hfreshPrefillLabels = prometheus.Labels{
	"index_type": string(monitoring.VectorIndexTypeHFresh),
	"mode":       string(monitoring.PrefillModeAsync),
}

func hfreshPrefillCount(t *testing.T) uint64 {
	t.Helper()
	n, err := monitoringhelpers.SampleCount(prometheus.DefaultGatherer,
		"weaviate_vector_cache_prefill_duration_seconds", hfreshPrefillLabels)
	require.NoError(t, err)
	return n
}

// newWarmupTestIndex builds an hfresh index holding total vectors (zero for an
// empty tenant).
func newWarmupTestIndex(t *testing.T, total int) *HFresh {
	t.Helper()
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
	return index
}

func TestWarmVersionMapReportsPrefill(t *testing.T) {
	index := newWarmupTestIndex(t, 20)

	before := hfreshPrefillCount(t)
	index.warmVersionMap()

	require.Equal(t, before+1, hfreshPrefillCount(t), "a completed warmup is observed once")
}

// Tenant creation is exactly the churn the prefill filter exists for: an
// empty index has nothing to warm and must not record a microsecond sample.
func TestWarmVersionMapEmptyIndexReportsNoPrefill(t *testing.T) {
	index := newWarmupTestIndex(t, 0)

	before := hfreshPrefillCount(t)
	index.warmVersionMap()

	require.Equal(t, before, hfreshPrefillCount(t), "an empty index warms nothing and reports no prefill")
}

// A warmup that panics is recovered by the goroutine wrapper in production,
// so it must not count as a completed prefill.
func TestWarmVersionMapPanicRecordsNoDuration(t *testing.T) {
	index := newWarmupTestIndex(t, 20)

	// A nil version map panics on its first use; put the real one back before
	// the index shuts down.
	orig := index.VersionMap
	index.VersionMap = nil
	t.Cleanup(func() { index.VersionMap = orig })

	before := hfreshPrefillCount(t)
	require.Panics(t, func() { index.warmVersionMap() })

	require.Equal(t, before, hfreshPrefillCount(t), "a warmup that panicked is not a completed prefill")
}
