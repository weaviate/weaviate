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

// TestSearchEntrypointSeedDuplicate_MultiMember pins bug #656 in its realistic,
// non-degenerate shape. The companion TestSearchEntrypointSeedDuplicate uses the
// minimal reproduction (allow-list = {entrypoint}, query = the entrypoint's own
// vector); this one proves the same duplicate survives a realistic filter:
//
//   - the entrypoint is one of SEVERAL allow-list members (not the whole list),
//     so it is still re-enqueued by the seed loop but must coexist with other
//     genuine results, and
//   - the query is a PERTURBED copy of the entrypoint's vector, not literally
//     equal to it — so the duplicate cannot be dismissed as an artifact of a
//     query that happens to sit exactly on a stored point.
//
// Root cause (confirmed by reverting the fix): the entrypoint enters the layer's
// entrypoint queue twice — once from the level descent (search.go ~L1092) and
// once from the ACORN allow-list seed loop (search.go ~L1156) whenever it is an
// allowed member among the first <=10 iterator ids. Before the fix,
// insertViableEntrypointsAsCandidatesAndResults called visitedList.Visit(ep.ID)
// unconditionally and inserted BOTH copies into the result heap, so the search
// returned the id twice. With the fix (CheckAndVisit at insertion) the second
// copy is dropped.
//
// It is NOT the "seeds are never marked visited" mechanism: seeds have always
// been marked visited by insertViable, so a seed reached again through the graph
// is correctly skipped. The duplicate is purely the double-ENQUEUE of the
// entrypoint, independent of graph traversal.
//
// AcornFilterRatio is pinned high so the strategy-ratio check cannot demote
// ACORN to RRE; FlatSearchCutoff=1 forces the graph path.
func TestSearchEntrypointSeedDuplicate_MultiMember(t *testing.T) {
	const (
		n  = 300
		k  = 5
		ef = 8
	)
	ctx := context.Background()
	vectors, _ := testinghelpers.RandomVecsFixedSeed(n, 1, 8)

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "ep-dedup-multimember",
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

	// The entrypoint is one of several allow-list members. Allow-list size <= 10
	// so every member (including the entrypoint) is fed to the seed loop.
	members := []uint64{ep}
	for id := uint64(0); id < uint64(n) && len(members) < 5; id++ {
		if id == ep {
			continue
		}
		members = append(members, id)
	}
	allow := helpers.NewAllowList(members...)
	defer allow.Close()

	// Query is close to — but not equal to — the entrypoint's vector, so the
	// entrypoint stays a genuine top-k result without the query sitting exactly
	// on it.
	query := make([]float32, len(vectors[ep]))
	copy(query, vectors[ep])
	query[0] += 1e-4

	ids, _, err := index.SearchByVector(ctx, query, k, allow)
	require.NoError(t, err)

	seen := map[uint64]bool{}
	for _, id := range ids {
		require.Falsef(t, seen[id], "duplicate id %d in results %v", id, ids)
		seen[id] = true
	}
}
