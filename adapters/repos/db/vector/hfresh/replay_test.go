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

package hfresh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// postingCopies reads every posting of the index from its store and counts
// the entries each vector id has in them, and the entries in all.
func postingCopies(t *testing.T, h *HFresh) (copies map[uint64]int, entries int) {
	t.Helper()
	var postings []uint64
	if h.Centroids.Built() {
		for id := range h.PostingMap.Iter() {
			postings = append(postings, id)
		}
	} else {
		postings = []uint64{singlePostingID}
	}
	copies = map[uint64]int{}
	for _, postingID := range postings {
		posting, err := h.PostingStore.Get(t.Context(), postingID)
		require.NoError(t, err, "posting %d", postingID)
		for _, v := range posting {
			copies[v.ID()]++
			entries++
		}
	}
	return copies, entries
}

// The queue of a shard hands a chunk over again whenever the batch it gave
// did not end well: a task of it failed with a transient error (the whole
// chunk is canceled and read again, with the tasks that had succeeded), the
// flush after it failed, or the process died before the chunk was removed.
// The index is then given vectors it holds, under their ids.
//
// A replay must leave the index as it was: every vector in its postings as
// many times as before, the sizes the splits go by unchanged, and the same
// answers. HFresh appends every vector again: its postings hold it twice, and
// their sizes count it twice. A search still returns each id once, since it
// skips the ids it has seen, and a deleted vector stays deleted.
func TestAddBatchOfVectorsItHoldsIsAReplay(t *testing.T) {
	const k = 10
	tests := []struct {
		name string
		n    int
		// single keeps the vectors in the single posting of an index below
		// its threshold; otherwise the index has its centroids and postings.
		single bool
	}{
		{name: "in the single posting", n: singlePostingThreshold / 2, single: true},
		{name: "with its postings", n: 1500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.single {
				setSinglePostingLimit(t, singlePostingThreshold)
			}
			tf := newTestIndex(t, singlePostingUserConfig(), nil)
			vectors, queries := testinghelpers.RandomVecsFixedSeed(tt.n, 20, singlePostingDims)
			ids := make([]uint64, len(vectors))
			for i, v := range vectors {
				ids[i] = uint64(i)
				addVectorToIndex(t, &tf, ids[i], v)
			}
			index := tf.Index
			index.waitForMaintenance(t)
			require.Equal(t, !tt.single, index.Centroids.Built())

			answers := func() [][]uint64 {
				out := make([][]uint64, len(queries))
				for i, q := range queries {
					found, _, err := index.SearchByVector(ctx, q, k, nil)
					require.NoError(t, err, "query %d", i)
					out[i] = found
				}
				return out
			}
			copiesBefore, entriesBefore := postingCopies(t, index)
			require.Len(t, copiesBefore, tt.n, "every vector is in a posting")
			answersBefore := answers()

			// The replay: the same ids with the same vectors, as the queue
			// hands them over again.
			require.NoError(t, index.AddBatch(ctx, ids, vectors), "a replay is taken")
			index.waitForMaintenance(t)

			copiesAfter, entriesAfter := postingCopies(t, index)
			more := 0
			for id, before := range copiesBefore {
				if copiesAfter[id] > before {
					more++
				}
			}
			assert.Zerof(t, more, "%d of %d vectors have more entries in the postings after the replay than before "+
				"(%d entries before, %d after)", more, tt.n, entriesBefore, entriesAfter)
			assert.Equal(t, entriesBefore, entriesAfter, "the entries of the postings, which their sizes and their splits go by")

			// What a search returns: every id once, k of them, and each
			// vector finds itself.
			for i, found := range answers() {
				seen := map[uint64]struct{}{}
				for _, id := range found {
					_, twice := seen[id]
					assert.False(t, twice, "query %d returned id %d twice", i, id)
					seen[id] = struct{}{}
				}
				assert.Len(t, found, len(answersBefore[i]), "query %d returns as many results as before the replay", i)
			}
			for i := 0; i < tt.n; i += 37 {
				found, _, err := index.SearchByVector(ctx, vectors[i], 1, nil)
				require.NoError(t, err)
				assert.Equal(t, []uint64{uint64(i)}, found, "vector %d finds itself", i)
			}

			// A vector deleted after the replay is gone, both of its entries.
			require.NoError(t, index.Delete(3))
			found, _, err := index.SearchByVector(ctx, vectors[3], k, nil)
			require.NoError(t, err)
			assert.NotContains(t, found, uint64(3), "a deleted vector is not returned")
			// And the replay of its insert alone does not bring it back.
			require.NoError(t, index.AddBatch(ctx, []uint64{3}, [][]float32{vectors[3]}))
			index.waitForMaintenance(t)
			found, _, err = index.SearchByVector(ctx, vectors[3], k, nil)
			require.NoError(t, err)
			assert.NotContains(t, found, uint64(3), "a deleted vector stays deleted when its insert is replayed")
		})
	}
}
