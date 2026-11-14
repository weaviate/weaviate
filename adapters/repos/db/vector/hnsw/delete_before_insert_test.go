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
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func newMultiVectorIndex(t *testing.T) *hnsw {
	t.Helper()

	store := &mvDeletableStore{docs: map[uint64][][]float32{}}
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "multivector-delete-order",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, errors.New("multivector index must not use VectorForIDThunk")
		},
		MultiVectorForIDThunk: store.multiVectorForID,
		TempMultiVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([][]float32, error) {
			return store.multiVectorForID(ctx, id)
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
		AllocChecker:      memwatch.NewDummyMonitor(),
	}, ent.UserConfig{
		VectorCacheMaxObjects: 100000,
		MaxConnections:        8,
		EFConstruction:        64,
		EF:                    64,
		Multivector:           ent.MultivectorConfig{Enabled: true},
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Drop(context.Background(), false) })
	return index
}

// Doc IDs are never reused, so a delete is final even if it overtakes the insert:
// two concurrent updates of an object can do that, one deleting the doc ID the other
// one is still inserting. This only covers what a tombstone can: the shard takes care
// of the rest, see retractIfSuperseded.
func TestDeleteBeforeInsertKeepsTheDocDeleted(t *testing.T) {
	f := newBootstrapFixture(t, false)

	require.NoError(t, f.index.Delete(5))
	require.NoError(t, f.index.Add(f.ctx, 5, f.vectors[5]))

	require.False(t, f.index.ContainsDoc(5))
}

func TestHasGhost_MultiVector(t *testing.T) {
	ctx := context.Background()
	index := newMultiVectorIndex(t)
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.2, 0.3, 0.4, 0.5}}

	require.NoError(t, index.AddMulti(ctx, 0, vectors))
	require.False(t, index.HasGhost(0))

	require.NoError(t, index.AddMulti(ctx, 1, vectors))
	require.False(t, index.HasGhost(1))

	// an insert that failed after publishing one of the doc's vectors: nobody
	// links back to that node
	index.RLock()
	vecIDs := append([]uint64(nil), index.docIDVectors[1]...)
	index.RUnlock()
	ghost := vecIDs[0]
	if ghost == index.getEntrypoint() {
		ghost = vecIDs[1]
	}
	cutOff := index.nodeByID(ghost)
	cutOff.Lock()
	neighbors := append([]uint64(nil), cutOff.connections.GetLayer(0)...)
	cutOff.Unlock()
	for _, neighborID := range neighbors {
		neighbor := index.nodeByID(neighborID)
		neighbor.Lock()
		kept := slices.DeleteFunc(append([]uint64(nil), neighbor.connections.GetLayer(0)...),
			func(id uint64) bool { return id == ghost })
		neighbor.connections.ReplaceLayer(0, kept)
		neighbor.Unlock()
	}

	require.True(t, index.HasGhost(1))
	require.False(t, index.HasGhost(0))
}
