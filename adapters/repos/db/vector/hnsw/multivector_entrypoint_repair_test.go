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
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type mvDeletableStore struct {
	sync.Mutex
	docs map[uint64][][]float32
}

func (s *mvDeletableStore) put(docID uint64, vecs [][]float32) {
	s.Lock()
	defer s.Unlock()
	s.docs[docID] = vecs
}

func (s *mvDeletableStore) deleteDoc(docID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.docs, docID)
}

func (s *mvDeletableStore) multiVectorForID(ctx context.Context, docID uint64) ([][]float32, error) {
	s.Lock()
	defer s.Unlock()
	if vecs, ok := s.docs[docID]; ok {
		return vecs, nil
	}
	return nil, storobj.NewErrNotFoundf(docID, "doc deleted from store")
}

// TestMultivectorEntrypointRepair covers entrypoint repair for multivector
// (non-muvera) indexes, where HNSW node ids are vec ids but the store errors
// by docID. docIDs are chosen so each equals a vec id of a different doc:
// insert order assigns vec ids 0-2 to docID 3, 3-5 to docID 6, 6-8 to docID 0,
// so tombstoning an ErrNotFound docID always hits a live vector.
func TestMultivectorEntrypointRepair(t *testing.T) {
	ctx := context.Background()

	docIDs := []uint64{3, 6, 0}
	docVecs := [][][]float32{
		{{0.1, 0.2, 0.3, 0.4}, {0.2, 0.3, 0.4, 0.5}, {0.3, 0.4, 0.5, 0.6}},
		{{0.9, 0.1, 0.2, 0.3}, {0.8, 0.2, 0.3, 0.4}, {0.7, 0.3, 0.4, 0.5}},
		{{0.1, 0.9, 0.8, 0.2}, {0.2, 0.8, 0.7, 0.3}, {0.3, 0.7, 0.6, 0.4}},
	}
	query := [][]float32{{0.5, 0.5, 0.5, 0.5}, {0.4, 0.6, 0.4, 0.6}}

	newIndex := func(t *testing.T, store *mvDeletableStore) *hnsw {
		idx, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "multivector-entrypoint-repair",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return nil, errors.New("multivector index must not use VectorForIDThunk")
			},
			MultiVectorForIDThunk: store.multiVectorForID,
			MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
			GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
			AllocChecker:          memwatch.NewDummyMonitor(),
		}, ent.UserConfig{
			VectorCacheMaxObjects: 100000,
			MaxConnections:        8,
			EFConstruction:        64,
			EF:                    64,
			Multivector:           ent.MultivectorConfig{Enabled: true},
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.NoError(t, err)

		for i, docID := range docIDs {
			store.put(docID, docVecs[i])
			require.NoError(t, idx.AddMulti(ctx, docID, docVecs[i]))
		}
		return idx
	}

	// removes the doc from the store and evicts its vectors from the cache,
	// mimicking a deletion the index was never told about. Delete also wipes
	// the vec→doc mapping, so restore it as a restart would
	deleteDocExternally := func(idx *hnsw, store *mvDeletableStore, docID uint64) {
		store.deleteDoc(docID)
		for rel, vecID := range idx.docIDVectors[docID] {
			idx.cache.Delete(ctx, vecID)
			idx.cache.SetKeys(vecID, docID, uint64(rel))
		}
	}

	t.Run("entrypoint doc deleted externally", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		epVec := idx.entryPointID
		epDoc, _ := idx.cache.GetKeys(epVec)
		deadVecs := append([]uint64{}, idx.docIDVectors[epDoc]...)
		deleteDocExternally(idx, store, epDoc)

		searchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		ids, _, err := idx.SearchByMultiVector(searchCtx, query, 10, nil)
		require.NoError(t, searchCtx.Err(), "search did not terminate — repair loop is stuck")
		require.NoError(t, err)

		wantDocs := make([]uint64, 0, len(docIDs)-1)
		for _, docID := range docIDs {
			if docID != epDoc {
				wantDocs = append(wantDocs, docID)
			}
		}
		assert.ElementsMatch(t, wantDocs, ids, "search must return the live docs")

		// epDoc doubles as a live vec id of another doc by construction
		assert.False(t, idx.hasTombstone(epDoc),
			"tombstoned a live vector id (docID/vec id confusion)")
		for _, vecID := range deadVecs {
			assert.True(t, idx.hasTombstone(vecID),
				"all vectors of the dead doc must be tombstoned, missing %d", vecID)
		}
		assert.NotContains(t, deadVecs, idx.getEntrypoint(),
			"entrypoint must be repaired to a live vector")
	})

	t.Run("dead vec tombstones the whole doc", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		deadDoc := docIDs[1]
		deadVecs := append([]uint64{}, idx.docIDVectors[deadDoc]...)
		deleteDocExternally(idx, store, deadDoc)

		idx.handleDeletedDocOfNode(deadVecs[0], "test")

		for _, vecID := range deadVecs {
			assert.True(t, idx.hasTombstone(vecID),
				"sibling vec %d must be tombstoned", vecID)
		}
		for _, docID := range docIDs {
			if docID == deadDoc {
				continue
			}
			for _, vecID := range idx.docIDVectors[docID] {
				assert.False(t, idx.hasTombstone(vecID),
					"live vec %d must not be tombstoned", vecID)
			}
		}
	})

	t.Run("live doc never loses siblings to expansion", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		// the doc is still live in the store (e.g. a phantom vec slot):
		// expansion must probe siblings and leave the live ones alone
		liveDoc := docIDs[2]
		probed := idx.docIDVectors[liveDoc][0]
		idx.handleDeletedDocOfNode(probed, "test")

		assert.True(t, idx.hasTombstone(probed))
		for _, vecID := range idx.docIDVectors[liveDoc][1:] {
			assert.False(t, idx.hasTombstone(vecID),
				"live sibling %d must not be tombstoned", vecID)
		}
	})

	t.Run("cache miss errors are keyed by the requested vec id", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		deadDoc := docIDs[1]
		deadVec := idx.docIDVectors[deadDoc][0]
		deleteDocExternally(idx, store, deadDoc)

		_, err := idx.cache.Get(ctx, deadVec)
		var e storobj.ErrNotFound
		require.ErrorAs(t, err, &e)
		assert.Equal(t, deadVec, e.DocID,
			"error must carry the requested vec id, not the internal docID")
	})

	t.Run("stale vec to doc mapping only tombstones the probed vec", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		probed := idx.docIDVectors[docIDs[0]][0]
		// corrupt the mapping: point the probed vec at another doc
		idx.cache.SetKeys(probed, docIDs[1], 0)

		idx.handleDeletedDocOfNode(probed, "test")

		assert.True(t, idx.hasTombstone(probed))
		for _, vecID := range idx.docIDVectors[docIDs[1]] {
			assert.False(t, idx.hasTombstone(vecID),
				"vec %d of the wrongly mapped doc must not be tombstoned", vecID)
		}
	})

	t.Run("all docs deleted externally", func(t *testing.T) {
		store := &mvDeletableStore{docs: map[uint64][][]float32{}}
		idx := newIndex(t, store)
		defer idx.Drop(ctx, false)

		for _, docID := range docIDs {
			deleteDocExternally(idx, store, docID)
		}

		searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ids, _, err := idx.SearchByMultiVector(searchCtx, query, 10, nil)
		require.NoError(t, searchCtx.Err(), "search did not terminate — repair loop is stuck")
		require.NoError(t, err)
		require.Empty(t, ids)
	})
}
