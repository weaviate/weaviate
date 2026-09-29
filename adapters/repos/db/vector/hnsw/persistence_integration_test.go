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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type persistenceIntegrationNoopBucketView struct{}

func (n *persistenceIntegrationNoopBucketView) ReleaseView() {}

func TestHnswPersistence_IsolatedNodeSurvivesCompactionAndRestart(t *testing.T) {
	for _, id := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("id_%d", id), func(t *testing.T) {
			dir := t.TempDir()
			const indexID = "isolated"
			logger, _ := test.NewNullLogger()
			store := testinghelpers.NewDummyStore(t)
			cfg := Config{
				AllocChecker: memwatch.NewDummyMonitor(),
				RootPath:     dir,
				ID:           indexID,
				MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
					return NewCommitLogger(dir, indexID, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
				},
				DistanceProvider: distancer.NewCosineDistanceProvider(),
				VectorForIDThunk: testVectorForID,
				GetViewThunk:     func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
			}
			uc := ent.UserConfig{MaxConnections: 30, EFConstruction: 60}
			index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			t.Cleanup(func() {
				if index != nil {
					require.NoError(t, index.Shutdown(context.Background()))
				}
			})
			require.NoError(t, index.Add(t.Context(), id, testVectors[id]))

			for round := 0; round < 3; round++ {
				ids, _, err := index.SearchByVector(t.Context(), testVectors[id], 1, nil)
				require.NoError(t, err)
				require.Equal(t, []uint64{id}, ids, "search after %d compaction/restart rounds", round)
				if round == 2 {
					break
				}

				require.NoError(t, index.Flush())
				require.NoError(t, index.Shutdown(t.Context()))
				index = nil
				logDir := filepath.Join(dir, indexID+".hnsw.commitlog.d")
				// A newer active WAL makes the closed log eligible for compaction,
				// as happens on restart, regardless of the closed log's size.
				activeWAL := filepath.Join(logDir, "999999999999999999")
				require.NoError(t, os.WriteFile(activeWAL, nil, 0o600))
				compactor := compact.NewCompactor(compact.DefaultCompactorConfig(logDir), logger)
				action, err := compactor.RunCycle(nil)
				require.NoError(t, err)
				if round == 0 {
					require.Equal(t, compact.ActionCreateSnapshot, action)
				}
				require.NoError(t, os.Remove(activeWAL))
				index, err = New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
				require.NoError(t, err)
			}
		})
	}
}

func TestHnswPersistence(t *testing.T) {
	dirName := t.TempDir()
	ctx := context.Background()
	indexID := "integrationtest"

	logger, _ := test.NewNullLogger()

	makeCL := func(opts ...CommitlogOption) (CommitLogger, error) {
		return NewCommitLogger(dirName, indexID, logger,
			cyclemanager.NewCallbackGroupNoop(), opts...)
	}

	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	require.Nil(t, index.Flush())

	// see index_test.go for more context
	expectedResults := []uint64{
		3, 5, 4, // cluster 2
		7, 8, 6, // cluster 3
		2, 1, 0, // cluster 1
	}

	t.Run("verify that the results match originally", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// destroy the index
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_CorruptWAL(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	indexID := "integrationtest_corrupt"

	logger, _ := test.NewNullLogger()

	makeCL := func(opts ...CommitlogOption) (CommitLogger, error) {
		return NewCommitLogger(dirName, indexID, logger,
			cyclemanager.NewCallbackGroupNoop(), opts...)
	}

	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	require.Nil(t, index.Flush())

	// see index_test.go for more context
	expectedResults := []uint64{
		3, 5, 4, // cluster 2
		7, 8, 6, // cluster 3
		2, 1, 0, // cluster 1
	}

	t.Run("verify that the results match originally", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// destroy the index
	index.Shutdown(context.Background())
	index = nil
	indexDir := filepath.Join(dirName, "integrationtest_corrupt.hnsw.commitlog.d")

	t.Run("corrupt the commit log on purpose", func(t *testing.T) {
		res, err := os.ReadDir(indexDir)
		require.Nil(t, err)
		require.Len(t, res, 1)
		fName := filepath.Join(indexDir, res[0].Name())
		newFName := filepath.Join(indexDir, fmt.Sprintf("%d", time.Now().Unix()))

		orig, err := os.Open(fName)
		require.Nil(t, err)

		correctLog, err := io.ReadAll(orig)
		require.Nil(t, err)
		err = orig.Close()
		require.Nil(t, err)

		os.Remove(fName)

		corruptLog := correctLog[:len(correctLog)-6]
		corrupt, err := os.Create(newFName)
		require.Nil(t, err)

		_, err = corrupt.Write(corruptLog)
		require.Nil(t, err)

		err = corrupt.Close()
		require.Nil(t, err)

		// double check that we only have one file left (the corrupted one)
		res, err = os.ReadDir(indexDir)
		require.Nil(t, err)
		require.Len(t, res, 1)
	})

	// build a new index from the (uncondensed, corrupted) commit log
	secondIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	// the minor corruption (just one missing link) will most likely not render
	// the index unusable, so we should still expect to retrieve results as
	// normal
	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_WithDeletion_WithoutTombstoneCleanup(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	indexID := "integrationtest_deletion"
	logger, _ := test.NewNullLogger()

	makeCL := func(opts ...CommitlogOption) (CommitLogger, error) {
		return NewCommitLogger(dirName, indexID, logger,
			cyclemanager.NewCallbackGroupNoop(), opts...)
	}

	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("delete some elements", func(t *testing.T) {
		err := index.Delete(6)
		require.Nil(t, err)
		err = index.Delete(8)
		require.Nil(t, err)
	})

	// see index_test.go for more context
	expectedResults := []uint64{
		3, 5, 4, // cluster 2
		7,       // cluster 3 with element 6 and 8 deleted
		2, 1, 0, // cluster 1
	}

	require.Nil(t, index.Flush())

	t.Run("verify that the results match originally", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	dumpIndex(index, "without_cleanup_original_index_before_storage")

	// destroy the index
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	dumpIndex(secondIndex, "without_cleanup_after_rebuild")
	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_WithDeletion_WithTombstoneCleanup(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	indexID := "integrationtest_tombstonecleanup"

	logger, _ := test.NewNullLogger()

	makeCL := func(opts ...CommitlogOption) (CommitLogger, error) {
		return NewCommitLogger(dirName, indexID, logger,
			cyclemanager.NewCallbackGroupNoop(), opts...)
	}

	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}
	dumpIndex(index, "with cleanup after import")
	require.Nil(t, index.Flush())

	t.Run("delete some elements and permanently delete tombstoned elements",
		func(t *testing.T) {
			err := index.Delete(6)
			require.Nil(t, err)
			err = index.Delete(8)
			require.Nil(t, err)

			err = index.CleanUpTombstonedNodes(neverStop)
			require.Nil(t, err)
		})

	dumpIndex(index, "with cleanup after delete")

	require.Nil(t, index.Flush())

	// see index_test.go for more context
	expectedResults := []uint64{
		3, 5, 4, // cluster 2
		7,       // cluster 3 with element 6 and 8 deleted
		2, 1, 0, // cluster 1
	}

	t.Run("verify that the results match originally", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// destroy the index
	index.Shutdown(context.Background())
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	dumpIndex(secondIndex, "with cleanup second index")

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})

	t.Run("further deleting all elements and reimporting one", func(t *testing.T) {
		toDelete := []uint64{0, 1, 2, 3, 4, 5, 7}

		for _, id := range toDelete {
			err := secondIndex.Delete(id)
			require.Nil(t, err)
		}

		err = secondIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)

		err := secondIndex.Add(ctx, 3, testVectors[3])
		require.Nil(t, err)
	})

	require.Nil(t, secondIndex.Flush())

	dumpIndex(secondIndex)

	secondIndex.Shutdown(context.Background())
	secondIndex = nil

	// build a new index from the (uncondensed) commit log
	thirdIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	dumpIndex(thirdIndex)

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := thirdIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, []uint64{3}, res)
		})

	t.Run("delete all elements so the commitlog ends with an empty graph", func(t *testing.T) {
		toDelete := []uint64{3}

		for _, id := range toDelete {
			err := thirdIndex.Delete(id)
			require.Nil(t, err)
		}

		err = thirdIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	require.Nil(t, thirdIndex.Flush())

	thirdIndex.Shutdown(context.Background())
	thirdIndex = nil
	// build a new index from the (uncondensed) commit log
	fourthIndex, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		GetViewThunk:          func() common.BucketView { return &persistenceIntegrationNoopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	t.Run("load from disk and try to insert again", func(t *testing.T) {
		for i, vec := range testVectors {
			err := fourthIndex.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("verify that searching works normally", func(t *testing.T) {
		expectedResults := []uint64{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3 with element 6 and 8 deleted
			2, 1, 0, // cluster 1
		}
		position := 3
		res, _, err := fourthIndex.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	fourthIndex.Shutdown(context.Background())
}
