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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// TestDocIDReuse_C2_StaleReadTombstonesLiveNode is a RECONSTRUCTION (the verbatim
// audit file audit_c2_repro_test.go was lost) of finding C2, via its real call
// site flatSearch -> distToNode -> handleDeletedNode.
//
// C2: a node id reused under docID reuse is live and passed CleanForReuse (no
// tombstone, slot occupied). But a stale store read for that node during search
// returns storobj.ErrNotFound — a transient the audit found C1 makes
// deterministic (the corrupted secondary index resolves the UUID to a wrong /
// deleted row). distToNode treats ErrNotFound as "node is gone" and calls
// handleDeletedNode, which ADDS A TOMBSTONE to the live reused node. A read-only
// search therefore deletes a live object.
//
// Reconstruction notes vs the original: this drives the real flatSearch path on
// an uncompressed index. Instead of routing a stale read through a corrupted
// LSM secondary index (that is C1's job; here we isolate C2), it evicts the
// target node from the vector cache — the exact cache-miss a freshly reused node
// takes — and makes the backing thunk return ErrNotFound for that one read, the
// same observable distToNode sees. The pin: a live, un-tombstoned node must not
// acquire a tombstone merely because one search read missed.
//
// Verified RED at tests/docid-reuse-regression head (dec791e549): node is
// tombstoned after the search. If this ever runs GREEN, treat it as SUSPECT
// (reconstruction drifted) before concluding C2 is fixed.
func TestDocIDReuse_C2_StaleReadTombstonesLiveNode(t *testing.T) {
	t.Skip("pins finding C2 (stale read tombstones a live reused node via flatSearch->distToNode->handleDeletedNode); no fix yet — see docid-reuse-jero-audit. RECONSTRUCTION of lost audit_c2_repro_test.go. Verified RED at dec791e549: the live node is tombstoned by a read-only search.")

	ctx := context.Background()

	vectors := [][]float32{
		{0.10, 0.20, 0.30, 0.40},
		{0.20, 0.10, 0.40, 0.30},
		{0.30, 0.40, 0.10, 0.20},
		{0.40, 0.30, 0.20, 0.10},
		{0.15, 0.25, 0.35, 0.45},
	}
	const target = uint64(2) // the live, reused node a stale read will hit

	var staleForTarget atomic.Bool
	vecForID := func(_ context.Context, id uint64) ([]float32, error) {
		if id == target && staleForTarget.Load() {
			// the transient not-found a reused node suffers under C1
			return nil, storobj.NewErrNotFoundf(id, "stale read on reused node")
		}
		if int(id) >= len(vectors) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		return vectors[id], nil
	}

	cfg := Config{
		RootPath:              t.TempDir(),
		ID:                    "docid-reuse-c2",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForID,
		GetViewThunk:          GetViewThunk,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}
	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 16,
		EFConstruction: 32,
		EF:             16,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	ids := []uint64{0, 1, 2, 3, 4}
	require.NoError(t, index.AddBatch(ctx, ids, vectors))
	require.False(t, index.hasTombstone(target), "precondition: reused node is live, CleanForReuse passed")

	// evict the target so the next read takes the store path (the cache-miss a
	// freshly reused node takes), then arm the stale read.
	index.cache.Delete(ctx, target)
	staleForTarget.Store(true)

	// a filtered (flat) search over all candidates — a read-only query.
	allowList := helpers.NewAllowList(ids...)
	_, _, err = index.flatSearch(ctx, vectors[0], 5, 5, allowList)
	require.NoError(t, err)

	require.False(t, index.hasTombstone(target),
		"a live reused node was tombstoned by a read-only search after a single stale store read (C2)")
}
