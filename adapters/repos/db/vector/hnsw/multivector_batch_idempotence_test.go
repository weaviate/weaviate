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
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// cancelAfterNChecksCtx reports itself as canceled once Err has been called n
// times, deterministically aborting AddMultiBatch partway through a task.
type cancelAfterNChecksCtx struct {
	context.Context
	remaining atomic.Int64
}

func newCancelAfterNChecksCtx(n int64) *cancelAfterNChecksCtx {
	ctx := &cancelAfterNChecksCtx{Context: context.Background()}
	ctx.remaining.Store(n)
	return ctx
}

func (c *cancelAfterNChecksCtx) Err() error {
	if c.remaining.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

func newTestMultivectorIndex(t *testing.T) *hnsw {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)

	uc := ent.UserConfig{}
	uc.Multivector.Enabled = true

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "multivector-idempotence",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
		GetViewThunk: func() common.BucketView { return &restoreDocNoopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return nil, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	return index
}

// TestAddMultiBatchRetryIdempotence re-executes the same AddMultiBatch task the
// way the queue worker does after a transient failure (e.g. a READONLY store
// during a vector-index-config update). Re-execution must not duplicate
// _mv_mappings entries or graph nodes: duplicates survive on disk and, after a
// restart, positional relativeID reconstruction turns them into permanent
// "relativeID out of bounds" search failures.
func TestAddMultiBatchRetryIdempotence(t *testing.T) {
	tests := []struct {
		name     string
		docIDs   []uint64
		vectors  [][][]float32
		firstCtx func() context.Context
		firstErr error
		// expected live vectors per doc after the final call
		expected map[uint64]int
	}{
		{
			name:   "re-execution after fully applied first attempt",
			docIDs: []uint64{10, 11, 12},
			vectors: [][][]float32{
				{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				{{2, 3, 4}, {5, 6, 7}},
				{{3, 4, 5}, {6, 7, 8}, {9, 10, 11}, {12, 13, 14}},
			},
			firstCtx: func() context.Context { return context.Background() },
			expected: map[uint64]int{10: 3, 11: 2, 12: 4},
		},
		{
			name:   "re-execution after mid-task failure",
			docIDs: []uint64{10, 11, 12},
			vectors: [][][]float32{
				{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				{{2, 3, 4}, {5, 6, 7}},
				{{3, 4, 5}, {6, 7, 8}, {9, 10, 11}, {12, 13, 14}},
			},
			firstCtx: func() context.Context { return newCancelAfterNChecksCtx(4) },
			firstErr: context.Canceled,
			expected: map[uint64]int{10: 3, 11: 2, 12: 4},
		},
		{
			name:   "duplicate docID within one batch replaces earlier occurrence",
			docIDs: []uint64{10, 11, 10},
			vectors: [][][]float32{
				{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				{{2, 3, 4}, {5, 6, 7}},
				{{9, 9, 9}, {8, 8, 8}},
			},
			firstCtx: func() context.Context { return context.Background() },
			expected: map[uint64]int{10: 2, 11: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := newTestMultivectorIndex(t)
			defer index.Shutdown(context.Background())

			err := index.AddMultiBatch(tt.firstCtx(), tt.docIDs, tt.vectors)
			if tt.firstErr != nil {
				require.ErrorIs(t, err, tt.firstErr)
			} else {
				require.NoError(t, err)
			}

			// the queue worker retries the whole task on transient errors
			require.NoError(t, index.AddMultiBatch(context.Background(), tt.docIDs, tt.vectors))

			assertMultivectorIntegrity(t, index, tt.expected)

			// simulate a restart: rebuild doc mappings from disk like startup does
			index.Lock()
			index.docIDVectors = make(map[uint64][]uint64)
			index.Unlock()
			require.NoError(t, index.restoreDocMappings())
			for docID, count := range tt.expected {
				assert.Len(t, index.docIDVectors[docID], count,
					"restored vector count for doc %d", docID)
			}
		})
	}
}

// assertMultivectorIntegrity verifies the issue's integrity invariant: the
// number of live _mv_mappings entries per doc must equal the doc's vector
// count, and the in-memory docIDVectors view must agree.
func assertMultivectorIntegrity(t *testing.T, index *hnsw, expected map[uint64]int) {
	t.Helper()

	counts := make(map[uint64]int)
	cursor := index.store.Bucket(index.id + "_mv_mappings").Cursor()
	for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
		require.Len(t, val, 8)
		counts[binary.BigEndian.Uint64(val)]++
	}
	cursor.Close()

	index.RLock()
	defer index.RUnlock()
	for docID, count := range expected {
		assert.Equal(t, count, counts[docID],
			"persisted mappings count for doc %d", docID)
		assert.Len(t, index.docIDVectors[docID], count,
			"in-memory vector count for doc %d", docID)
	}
}
