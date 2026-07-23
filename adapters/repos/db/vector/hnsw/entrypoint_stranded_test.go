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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestDeleteEntrypoint_CandidatesUnderMaintenance deletes the entrypoint while
// every other node is under maintenance, stranding the entrypoint on the
// tombstoned node: cleanup must not wipe the live nodes and the next search
// must repair the entrypoint.
func TestDeleteEntrypoint_CandidatesUnderMaintenance(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForEntrypointRepairTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "entrypoint-stranded-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return vectors[0], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	for i := 0; i < len(vectors); i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}

	oldEP := index.entryPointID

	for i := 0; i < len(vectors); i++ {
		if uint64(i) == oldEP {
			continue
		}
		node := index.nodeByID(uint64(i))
		require.NotNil(t, node)
		node.markAsMaintenance()
	}

	require.NoError(t, index.Delete(oldEP))

	// cleanup's reassignment unmarks maintenance, so re-mark on every
	// shouldAbort invocation to keep simulating in-flight inserts
	markAllLive := func() bool {
		for i := 0; i < len(vectors); i++ {
			if uint64(i) == oldEP {
				continue
			}
			if node := index.nodeByID(uint64(i)); node != nil {
				node.markAsMaintenance()
			}
		}
		return false
	}
	_, err = index.cleanUpTombstonedNodes(markAllLive)
	require.NoError(t, err)

	for i := 0; i < len(vectors); i++ {
		if uint64(i) == oldEP {
			continue
		}
		if node := index.nodeByID(uint64(i)); node != nil {
			node.unmarkAsMaintenance()
		}
	}

	liveNodes := 0
	for i := 0; i < len(vectors); i++ {
		if uint64(i) == oldEP {
			continue
		}
		if index.nodeByID(uint64(i)) != nil {
			liveNodes++
		}
	}
	assert.Equal(t, len(vectors)-1, liveNodes,
		"tombstone cleanup must not remove live nodes")

	ids, _, err := index.SearchByVector(ctx, vectors[1], 3, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, ids, "search must still find the live nodes")
	assert.NotContains(t, ids, oldEP, "results must not contain the deleted node")

	newEP := index.getEntrypoint()
	assert.NotEqual(t, oldEP, newEP, "search must have repaired the entrypoint")
	assert.NotNil(t, index.nodeByID(newEP), "repaired entrypoint must be a live node")
}
