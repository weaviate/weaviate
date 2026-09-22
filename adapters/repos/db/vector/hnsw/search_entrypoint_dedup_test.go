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

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestSearchEntrypointSeedDuplicate reproduces the duplicate-entrypoint
// hazard on the filtered ACORN path: the allow-list seed loop can enqueue
// the global entrypoint a second time (the level descent already
// contributed it), and insertViableEntrypointsAsCandidatesAndResults must
// not insert both occurrences into the result heap — a filtered search
// would return the same id twice.
//
// Two ingredients make the double-seeding deterministic regardless of
// which node the (randomly leveled) graph elected as entrypoint: the query
// IS the entrypoint's own vector, so the per-level descent can never
// improve on it (distance 0) and the global entrypoint survives to level
// 0; and the allowlist is exactly {entrypoint}, so the seed loop
// re-enqueues it. AcornFilterRatio is pinned high so the strategy-ratio
// check (which samples the entrypoint's per-run-random neighborhood)
// cannot demote ACORN to RRE.
func TestSearchEntrypointSeedDuplicate(t *testing.T) {
	const (
		n  = 300
		k  = 2
		ef = 8
	)

	ctx := context.Background()
	vectors, _ := testinghelpers.RandomVecsFixedSeed(n, 1, 8)

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "ep-dedup-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		AcornFilterRatio:      1000,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        32,
		EF:                    ef,
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategyAcorn,
		FlatSearchCutoff:      1, // force the graph path
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	for id := uint64(0); id < uint64(n); id++ {
		require.NoError(t, index.Add(ctx, id, vectors[id]))
	}

	ep := index.entryPointID
	query := vectors[ep]
	allow := helpers.NewAllowList(ep)
	defer allow.Close()

	ids, _, err := index.SearchByVector(ctx, query, k, allow)
	require.NoError(t, err)
	require.Equal(t, []uint64{ep}, ids,
		"the doubly-seeded entrypoint must appear exactly once in the results")
}
