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

//go:build integrationTest

package hnsw

import (
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type searchRepairNoopBucketView struct{}

func (n *searchRepairNoopBucketView) ReleaseView() {}

// externalDeleteStore mimics the geo index scenario: objects are deleted from
// the object store without the index being told
type externalDeleteStore struct {
	sync.Mutex
	objects map[uint64][]float32
}

func (s *externalDeleteStore) put(id uint64, vec []float32) {
	s.Lock()
	defer s.Unlock()
	s.objects[id] = vec
}

func (s *externalDeleteStore) deleteAll() {
	s.Lock()
	defer s.Unlock()
	s.objects = map[uint64][]float32{}
}

func (s *externalDeleteStore) vectorForID(ctx context.Context, id uint64) ([]float32, error) {
	s.Lock()
	defer s.Unlock()
	v, ok := s.objects[id]
	if !ok {
		return nil, storobj.NewErrNotFoundf(id, "retrieve object")
	}
	return v, nil
}

// TestSearchRepairsEntrypointDeletedInObjectStore covers the geo filter
// scenario of delete-all + re-ingest + restart: the first search after restart
// hits a stale entrypoint (cold vector cache) and must repair it and succeed
func TestSearchRepairsEntrypointDeletedInObjectStore(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	newIndex := func(t *testing.T, dirName, indexID string, store *externalDeleteStore) *hnsw {
		idx, err := New(Config{
			AllocChecker: memwatch.NewDummyMonitor(),
			RootPath:     dirName,
			ID:           indexID,
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return NewCommitLogger(dirName, indexID, logger,
					cyclemanager.NewCallbackGroupNoop())
			},
			DistanceProvider: distancer.NewGeoProvider(),
			VectorForIDThunk: store.vectorForID,
			GetViewThunk:     func() common.BucketView { return &searchRepairNoopBucketView{} },
		}, ent.UserConfig{
			MaxConnections:         64,
			EFConstruction:         128,
			CleanupIntervalSeconds: ent.DefaultCleanupIntervalSeconds,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.NoError(t, err)
		return idx
	}

	genVec := func(i int) []float32 {
		return []float32{52.52 + float32(i)*0.01, 13.405 + float32(i)*0.01}
	}
	query := []float32{52.52, 13.405}

	t.Run("delete and re-ingest, search self-heals after restart", func(t *testing.T) {
		dirName := t.TempDir()
		store := &externalDeleteStore{objects: map[uint64][]float32{}}
		index := newIndex(t, dirName, "search-repair-reingest", store)

		for i := 0; i < 20; i++ {
			store.put(uint64(i), genVec(i))
			require.NoError(t, index.Add(ctx, uint64(i), genVec(i)))
		}

		res, err := index.KnnSearchByVectorMaxDist(ctx, query, 200_000, 800, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)

		store.deleteAll()

		for i := 20; i < 40; i++ {
			store.put(uint64(i), genVec(i-20))
			require.NoError(t, index.Add(ctx, uint64(i), genVec(i-20)))
		}

		res, err = index.KnnSearchByVectorMaxDist(ctx, query, 200_000, 800, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)

		// restart: the vector cache no longer masks the stale entrypoint
		require.NoError(t, index.Flush())
		require.NoError(t, index.Shutdown(ctx))
		index = newIndex(t, dirName, "search-repair-reingest", store)
		defer index.Shutdown(ctx)

		res, err = index.KnnSearchByVectorMaxDist(ctx, query, 200_000, 800, nil)
		require.NoError(t, err, "first search after restart must repair the entrypoint, not fail")
		require.NotEmpty(t, res)
		for _, id := range res {
			require.GreaterOrEqual(t, id, uint64(20), "results must only contain live docIDs")
		}
		require.GreaterOrEqual(t, index.entryPointID, uint64(20),
			"global entrypoint must be repaired to a live node")
	})

	t.Run("all objects deleted, search returns empty instead of erroring", func(t *testing.T) {
		dirName := t.TempDir()
		store := &externalDeleteStore{objects: map[uint64][]float32{}}
		index := newIndex(t, dirName, "search-repair-all-dead", store)

		for i := 0; i < 20; i++ {
			store.put(uint64(i), genVec(i))
			require.NoError(t, index.Add(ctx, uint64(i), genVec(i)))
		}

		store.deleteAll()

		require.NoError(t, index.Flush())
		require.NoError(t, index.Shutdown(ctx))
		index = newIndex(t, dirName, "search-repair-all-dead", store)
		defer index.Shutdown(ctx)

		// twice: covers the initial repair chain and the already-exhausted state
		for range 2 {
			res, err := index.KnnSearchByVectorMaxDist(ctx, query, 200_000, 800, nil)
			require.NoError(t, err)
			require.Empty(t, res)
		}
	})
}
