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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestAddMultiRejectsNilOrEmptyMultivector pins gh-12959: a document with no inner
// vectors reached code that indexes vectors[i][0] and panicked with an index out of
// range, taking the process down. It must be rejected with an error instead, on both
// the plain and the MUVERA-encoded multivector paths, and the index must remain usable.
func TestAddMultiRejectsNilOrEmptyMultivector(t *testing.T) {
	ctx := context.Background()
	valid := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.2, 0.3, 0.4, 0.5}}

	newIndex := func(t *testing.T, muvera bool) (*hnsw, *mvDeletableStore) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		mvCfg := ent.MultivectorConfig{Enabled: true}
		if muvera {
			mvCfg.MuveraConfig = ent.MuveraConfig{Enabled: true, KSim: 2, DProjections: 4, Repetitions: 1}
		}
		idx, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "multivector-nil-input",
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
			Multivector:           mvCfg,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.NoError(t, err)
		return idx, store
	}

	tests := []struct {
		name   string
		muvera bool
		insert func(idx *hnsw) error
	}{
		{"nil multivector", false, func(idx *hnsw) error { return idx.AddMulti(ctx, 1, nil) }},
		{"empty multivector", false, func(idx *hnsw) error { return idx.AddMulti(ctx, 1, [][]float32{}) }},
		{"nil document inside a batch", false, func(idx *hnsw) error {
			return idx.AddMultiBatch(ctx, []uint64{1, 2}, [][][]float32{valid, nil})
		}},
		{"nil multivector with muvera", true, func(idx *hnsw) error { return idx.AddMulti(ctx, 1, nil) }},
		{"nil document inside a batch with muvera", true, func(idx *hnsw) error {
			return idx.AddMultiBatch(ctx, []uint64{1, 2}, [][][]float32{valid, nil})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, store := newIndex(t, tt.muvera)

			err := tt.insert(idx)
			require.Error(t, err)
			require.ErrorContains(t, err, "nil or empty multivector")

			// The index is left usable: a well-formed document still inserts.
			store.put(7, valid)
			require.NoError(t, idx.AddMulti(ctx, 7, valid))
		})
	}
}
