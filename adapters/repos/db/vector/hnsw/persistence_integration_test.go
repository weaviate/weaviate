//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestHnswPersistence(t *testing.T) {
	dirName := t.TempDir()
	indexID := "integrationtest"

	logger, _ := test.NewNullLogger()
	cl, clErr := NewCommitLogger(dirName, indexID, logger,
		cyclemanager.NewCallbackGroupNoop())
	makeCL := func() (CommitLogger, error) {
		return cl, clErr
	}
	index, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
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
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// destroy the index
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_CorruptWAL(t *testing.T) {
	dirName := t.TempDir()
	indexID := "integrationtest_corrupt"

	logger, _ := test.NewNullLogger()
	cl, clErr := NewCommitLogger(dirName, indexID, logger,
		cyclemanager.NewCallbackGroupNoop())
	makeCL := func() (CommitLogger, error) {
		return cl, clErr
	}
	index, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
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
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
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
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	// the minor corruption (just one missing link) will most likely not render
	// the index unusable, so we should still expect to retrieve results as
	// normal
	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_WithDeletion_WithoutTombstoneCleanup(t *testing.T) {
	dirName := t.TempDir()
	indexID := "integrationtest_deletion"
	logger, _ := test.NewNullLogger()
	cl, clErr := NewCommitLogger(dirName, indexID, logger,
		cyclemanager.NewCallbackGroupNoop())
	makeCL := func() (CommitLogger, error) {
		return cl, clErr
	}
	index, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
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
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	dumpIndex(index, "without_cleanup_original_index_before_storage")

	// destroy the index
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	dumpIndex(secondIndex, "without_cleanup_after_rebuild")
	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
			require.Nil(t, err)
			assert.Equal(t, expectedResults, res)
		})
}

func TestHnswPersistence_WithDeletion_WithTombstoneCleanup(t *testing.T) {
	dirName := t.TempDir()
	indexID := "integrationtest_tombstonecleanup"

	logger, _ := test.NewNullLogger()
	makeCL := func() (CommitLogger, error) {
		return NewCommitLogger(dirName, indexID, logger,
			cyclemanager.NewCallbackGroupNoop())
	}
	index, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
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
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// destroy the index
	index.Shutdown(context.Background())
	index = nil

	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	dumpIndex(secondIndex, "with cleanup second index")

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := secondIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
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

		err := secondIndex.Add(3, testVectors[3])
		require.Nil(t, err)
	})

	require.Nil(t, secondIndex.Flush())

	dumpIndex(secondIndex)

	secondIndex.Shutdown(context.Background())
	secondIndex = nil

	// build a new index from the (uncondensed) commit log
	thirdIndex, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	dumpIndex(thirdIndex)

	t.Run("verify that the results match after rebuilding from disk",
		func(t *testing.T) {
			position := 3
			res, _, err := thirdIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
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
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	t.Run("load from disk and try to insert again", func(t *testing.T) {
		for i, vec := range testVectors {
			err := fourthIndex.Add(uint64(i), vec)
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
		res, _, err := fourthIndex.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	fourthIndex.Shutdown(context.Background())
}
