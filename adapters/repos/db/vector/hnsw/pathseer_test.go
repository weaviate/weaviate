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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func newPathseerTestIndex(t *testing.T, vectors [][]float32, ef int) *hnsw {
	t.Helper()
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "pathseer-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		EF:                    ef,
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategyPathseer,
		// force the graph path: without this, small allow lists route to
		// flat search and no strategy is exercised
		FlatSearchCutoff: 0,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	return index
}

// buildManualLayer0 installs a hand-built single-layer graph. conns maps
// node id -> its layer-0 neighbour list; every id in [0, len(vectors)) must
// be present.
func buildManualLayer0(t *testing.T, index *hnsw, n int, conns map[uint64][]uint64) {
	t.Helper()
	index.entryPointID = 0
	index.currentMaximumLayer = 0
	nodes := make([]*vertex, n)
	for id := 0; id < n; id++ {
		c, err := packedconn.NewWithElements([][]uint64{conns[uint64(id)]})
		require.Nil(t, err)
		nodes[id] = &vertex{level: 0, connections: c}
	}
	index.nodes = nodes
}

// A neighbour first reached from a non-matching candidate is skipped by the
// PathSeer prefilter without a distance computation. It must remain
// reachable from a later matching candidate; otherwise recall depends on
// pop order. Regression test for the visited-before-prefilter ordering.
//
// Geometry (L2, query at origin): 4 < 3 < 1 < 2 < 0 by distance.
// Allow list {0, 2, 4}. Node 3 (non-member) guards the best member 4 and is
// first seen from non-member 1, then again from member 2.
func TestPathseerPrefilterKeepsBurnedPathReachable(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{5, 0}, // 0: entrypoint, member
		{3, 0}, // 1: non-member, closer than 2 so it pops first
		{4, 0}, // 2: member
		{2, 0}, // 3: non-member bridge to 4
		{1, 0}, // 4: member, true top-1
	}
	index := newPathseerTestIndex(t, vectors, 1)
	defer index.Shutdown(ctx)
	buildManualLayer0(t, index, len(vectors), map[uint64][]uint64{
		0: {1, 2},
		1: {0, 3},
		2: {0, 3},
		3: {1, 2, 4},
		4: {3},
	})

	allow := helpers.NewAllowList(0, 2, 4)
	res, _, err := index.SearchByVector(ctx, []float32{0, 0}, 1, allow)
	require.Nil(t, err)
	assert.Equal(t, []uint64{4}, res,
		"member 4 must be found through bridge 3 even though 3 was first "+
			"reached (and prefilter-skipped) from non-member 1")
}

// countingProvider wraps a distancer.Provider and counts every
// Distancer.Distance call — i.e. every node-to-query distance computation,
// independent of vector-cache state.
type countingProvider struct {
	distancer.Provider
	calls *atomic.Int64
}

func (c *countingProvider) New(vec []float32) distancer.Distancer {
	return &countingDistancer{Distancer: c.Provider.New(vec), calls: c.calls}
}

type countingDistancer struct {
	distancer.Distancer
	calls *atomic.Int64
}

func (c *countingDistancer) Distance(vec []float32) (float32, error) {
	c.calls.Add(1)
	return c.Distancer.Distance(vec)
}

// A filter smaller than ef can never fill the result heap, so a prefilter
// gated only on results.Len() >= ef stays inert and the search degenerates
// into a sweep of the entire graph. With the activation threshold capped at
// the allow-list cardinality the traversal stays bounded. Regression test:
// counts distance computations during the search and fails on an ungated
// full-graph sweep.
func TestPathseerSubEfAllowListStaysBounded(t *testing.T) {
	ctx := context.Background()
	const n = 400
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = []float32{float32(i), 0}
	}

	var distCalls atomic.Int64
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "pathseer-subef",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      &countingProvider{Provider: distancer.NewL2SquaredProvider(), calls: &distCalls},
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    64, // far above the 3-member allow list
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategyPathseer,
		FlatSearchCutoff:      0,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	defer index.Shutdown(ctx)

	for i := 0; i < n; i++ {
		require.Nil(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	distCalls.Store(0)
	allow := helpers.NewAllowList(1, 2, 3)
	res, _, err := index.SearchByVector(ctx, []float32{0, 0}, 3, allow)
	require.Nil(t, err)
	assert.ElementsMatch(t, []uint64{1, 2, 3}, res, "all members must be returned")
	// Ungated, the search distance-computes essentially every node in the
	// graph (~n); gated, it touches the members plus a small frontier. The
	// bound is deliberately loose to stay robust to graph-construction
	// details.
	assert.LessOrEqual(t, distCalls.Load(), int64(n/2),
		"sub-ef allow list must not degenerate into a full-graph sweep")
}
