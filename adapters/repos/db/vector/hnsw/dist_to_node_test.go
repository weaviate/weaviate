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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestDistToNode_ReturnsErrorOnErrNotFound tests that distToNode propagates
// storobj.ErrNotFound as an error rather than swallowing it and returning (0, nil).
//
// This is a prerequisite for the search-side entrypoint fix: callers already
// have ErrNotFound handling code that never triggers because distToNode
// currently returns (0, nil) on deleted nodes.
//
// Callers affected by this behavioral change (code-reading verified):
//   - KnnSearchByVectorMaxDist: properly handles with handleDeletedNode (search_with_max_dist.go:41-46)
//   - knnSearchByVector: properly handles with handleDeletedNode (search.go:736-741)
//   - findBestEntrypointForNode: skips the level (continue) on ErrNotFound (index.go:581-589)
//   - tryEpCandidate: returns false, fallback to other candidates (neighbor_connections.go:535-542)
//   - processNode: returns MaxFloat32, deleted node deprioritized (neighbor_connections.go:107-115)
//     NOTE: processNode is only invoked during tombstone cleanup (tombstoneCleanupNodes=true),
//     not during normal insert/search. Normal insert uses searchLayerByVectorWithDistancer
//     which calls distanceToFloatNode (already propagates errors).
func TestDistToNode_ReturnsErrorOnErrNotFound(t *testing.T) {
	const deletedNodeID = uint64(42)

	// VectorForIDThunk that returns ErrNotFound for deletedNodeID
	vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
		if id == deletedNodeID {
			return nil, storobj.NewErrNotFoundf(id, "node deleted from store")
		}
		return []float32{1.0, 2.0, 3.0}, nil
	}

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "dist-to-node-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk:      vectorForID,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })

	// Call distToNode for the deleted node (uncompressed mode, distancer=nil)
	searchVec := []float32{1.0, 2.0, 3.0}
	_, err = index.distToNode(nil, deletedNodeID, searchVec)

	// The bug: currently returns (0, nil), but should return an error
	assert.Error(t, err, "distToNode should return error for deleted node, not (0, nil)")

	// Verify it's specifically ErrNotFound
	var notFoundErr storobj.ErrNotFound
	assert.ErrorAs(t, err, &notFoundErr, "error should be storobj.ErrNotFound")
	assert.Equal(t, deletedNodeID, notFoundErr.DocID)
}
