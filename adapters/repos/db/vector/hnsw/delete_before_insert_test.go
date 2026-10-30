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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
// two concurrent updates of an object can do that, one deleting the doc ID the
// other one is still inserting.
func TestDeleteBeforeInsertKeepsTheDocDeleted(t *testing.T) {
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.2, 0.3, 0.4, 0.5}}

	t.Run("single vector", func(t *testing.T) {
		f := newBootstrapFixture(t, false)

		require.NoError(t, f.index.Delete(5))
		require.NoError(t, f.index.Add(f.ctx, 5, f.vectors[5]))

		require.False(t, f.index.ContainsDoc(5))
	})

	t.Run("multi vector", func(t *testing.T) {
		t.Skip("https://github.com/weaviate/weaviate/issues/13686")

		ctx := context.Background()
		index := newMultiVectorIndex(t)

		require.NoError(t, index.DeleteMulti(5))
		require.NoError(t, index.AddMulti(ctx, 5, vectors))

		require.False(t, index.ContainsDoc(5))
	})
}

func TestHasGhost_MultiVector(t *testing.T) {
	ctx := context.Background()
	index := newMultiVectorIndex(t)
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.2, 0.3, 0.4, 0.5}}

	require.NoError(t, index.AddMulti(ctx, 0, vectors))
	require.False(t, index.HasGhost(0))

	live := []*vertex{index.nodeByID(0), index.nodeByID(1)}
	for _, node := range live {
		node.markAsMaintenance()
	}
	require.ErrorIs(t, index.AddMulti(ctx, 1, vectors), enterrors.ErrNoUsableEntrypoint)
	for _, node := range live {
		node.unmarkAsMaintenance()
	}

	require.True(t, index.HasGhost(1))
	require.False(t, index.HasGhost(0))
}
