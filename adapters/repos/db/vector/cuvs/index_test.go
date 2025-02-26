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

//go:build cuvs

package cuvs_index

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

const (
	dims          = 1536
	numVectors    = 1000 // Default number of vectors for tests
	numDeleted    = 100  // Number of vectors to delete
	k             = 10   // How many neighbors to retrieve
	minRecallRate = 0.9  // Minimum acceptable recall rate (98%)
)

// calculateRecall computes the recall rate for search results
func calculateRecall(expected, actual []uint64) float64 {
	if len(actual) == 0 {
		return 0.0
	}

	matches := 0
	for _, id := range actual {
		if id == expected[0] { // For now just checking exact match
			matches++
			break
		}
	}
	return float64(matches)
}

// generateRandomVector creates a random vector of specified dimension
func generateRandomVector(dim int) []float32 {
	vector := make([]float32, dim)
	for i := range vector {
		vector[i] = float32(rand.NormFloat64())
	}
	return vector
}

func setupTestIndex(t *testing.T) (*cuvs_index, string, func(), *lsmkv.Store) {
	t.Helper()

	tempDir := t.TempDir()
	store, err := lsmkv.New(filepath.Join(tempDir, "store"), tempDir, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := Config{
		ID:       "test-index",
		RootPath: tempDir,
		Logger:   logger,
	}

	userCfg := cuvs.UserConfig{}

	index, err := New(cfg, userCfg, store)
	require.NoError(t, err)

	cleanup := func() {
		store.Shutdown(context.Background())
		os.RemoveAll(tempDir)
	}

	return index, tempDir, cleanup, store
}

func TestPersistence(t *testing.T) {
	index, tempDir, cleanup, store := setupTestIndex(t)
	defer cleanup()

	// Create vectors
	ids := make([]uint64, numVectors)
	vectors := make([][]float32, numVectors)
	for i := range ids {
		ids[i] = uint64(i + 1)
		vectors[i] = generateRandomVector(dims)
	}

	// Add vectors
	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	// Test recall before shutdown
	totalRecall := 0.0
	for i := 0; i < numVectors; i++ {
		results, _, err := index.SearchByVector(context.Background(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall
	}

	recallRate := totalRecall / float64(numVectors)
	t.Logf("Pre-shutdown recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Pre-shutdown recall rate below threshold")

	// Shutdown and recreate index
	err = index.Shutdown(context.Background())
	require.NoError(t, err)

	println(tempDir)
	logger, _ := test.NewNullLogger()

	store.Shutdown(context.Background())

	store, err = lsmkv.New(filepath.Join(tempDir, "store"), tempDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	cfg := Config{
		ID:       "test-index",
		RootPath: t.TempDir(),
		Logger:   logger,
	}

	userCfg := cuvs.UserConfig{}

	newIndex, err := New(cfg, userCfg, store)
	require.NoError(t, err)
	newIndex.PostStartup()

	// Test recall after restore
	totalRecall = 0.0
	for i := 0; i < numVectors; i++ {
		results, _, err := newIndex.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall
	}

	recallRate = totalRecall / float64(numVectors)
	t.Logf("Post-restore recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Post-restore recall rate below threshold")
}

func TestBatchAddAndQuery(t *testing.T) {
	index, _, cleanup, _ := setupTestIndex(t)
	defer cleanup()

	// Create vectors
	ids := make([]uint64, numVectors)
	vectors := make([][]float32, numVectors)
	for i := range ids {
		ids[i] = uint64(i + 1)
		vectors[i] = generateRandomVector(dims)
	}

	// Add vectors
	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	// Test individual queries recall
	totalRecall := 0.0
	for i := 0; i < numVectors; i++ {
		results, _, err := index.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall

	}

	recallRate := totalRecall / float64(numVectors)
	t.Logf("Individual queries recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Individual queries recall rate below threshold")

	// Test batch query recall
	batchResults, _, err := index.SearchByVectorBatch(vectors, k, nil)
	require.NoError(t, err)

	totalBatchRecall := 0.0

	for i := range batchResults {
		rc := calculateRecall([]uint64{ids[i]}, batchResults[i])
		totalBatchRecall += rc
	}

	batchRecallRate := totalBatchRecall / float64(numVectors)
	t.Logf("Batch queries recall rate: %.4f", batchRecallRate)
	assert.GreaterOrEqual(t, batchRecallRate, minRecallRate, "Batch queries recall rate below threshold")
}

func TestDeleteWithoutPersistence(t *testing.T) {
	index, _, cleanup, _ := setupTestIndex(t)
	defer cleanup()

	// Create vectors
	ids := make([]uint64, numVectors)
	vectors := make([][]float32, numVectors)
	for i := range ids {
		ids[i] = uint64(i + 1)
		vectors[i] = generateRandomVector(dims)
	}

	// Add vectors
	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	for i := 0; i < numDeleted; i++ {
		// Delete vectors
		err = index.Delete(ids[i])
		require.NoError(t, err)
	}

	// Test individual queries recall
	totalRecall := 0.0
	for i := numDeleted; i < numVectors; i++ {
		results, _, err := index.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall

	}

	recallRate := totalRecall / float64(numVectors-numDeleted)
	t.Logf("Individual queries recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Individual queries recall rate below threshold")

	// Test that deleted vectors don't appear in search results
	for i := 0; i < numDeleted; i++ {
		results, _, err := index.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)

		// Verify no deleted vectors appear in results
		for _, resultId := range results {
			assert.Greater(t, resultId, uint64(numDeleted),
				"Found deleted vector %d in search results", resultId)
		}
	}
	t.Log("Verified deleted vectors do not appear in search results")

	// Test batch query recall
	batchResults, _, err := index.SearchByVectorBatch(vectors, k, nil)
	require.NoError(t, err)

	totalBatchRecall := 0.0

	for i := range batchResults {
		rc := calculateRecall([]uint64{ids[i]}, batchResults[i])
		totalBatchRecall += rc
	}

	batchRecallRate := totalBatchRecall / float64(numVectors)
	t.Logf("Batch queries recall rate: %.4f", batchRecallRate)
	assert.GreaterOrEqual(t, batchRecallRate, minRecallRate, "Batch queries recall rate below threshold")
}

func TestDeleteWithPersistence(t *testing.T) {
	index, tempDir, cleanup, store := setupTestIndex(t)
	defer cleanup()

	// Create vectors
	ids := make([]uint64, numVectors)
	vectors := make([][]float32, numVectors)
	for i := range ids {
		ids[i] = uint64(i + 1)
		vectors[i] = generateRandomVector(dims)
	}

	// Add vectors
	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	// Delete vectors
	for i := 0; i < numDeleted; i++ {
		err = index.Delete(ids[i])
		require.NoError(t, err)
	}

	// Test recall before shutdown
	totalRecall := 0.0
	for i := numDeleted; i < numVectors; i++ {
		results, _, err := index.SearchByVector(context.Background(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall
	}
	recallRate := totalRecall / float64(numVectors-numDeleted)
	t.Logf("Pre-shutdown recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Pre-shutdown recall rate below threshold")

	// Verify deleted vectors don't appear in results before shutdown
	for i := 0; i < numDeleted; i++ {
		results, _, err := index.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		for _, resultId := range results {
			assert.Greater(t, resultId, uint64(numDeleted),
				"Found deleted vector %d in search results before shutdown", resultId)
		}
	}

	// Shutdown and recreate index
	err = index.Shutdown(context.Background())
	require.NoError(t, err)

	logger, _ := test.NewNullLogger()
	store.Shutdown(context.Background())

	store, err = lsmkv.New(filepath.Join(tempDir, "store"), tempDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	cfg := Config{
		ID:       "test-index",
		RootPath: t.TempDir(),
		Logger:   logger,
	}

	userCfg := cuvs.UserConfig{}

	newIndex, err := New(cfg, userCfg, store)
	require.NoError(t, err)
	newIndex.PostStartup()

	// Test recall after restore
	totalRecall = 0.0
	for i := numDeleted; i < numVectors; i++ {
		results, _, err := newIndex.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{ids[i]}, results)
		totalRecall += recall
	}
	recallRate = totalRecall / float64(numVectors-numDeleted)
	t.Logf("Post-restore recall rate: %.4f", recallRate)
	assert.GreaterOrEqual(t, recallRate, minRecallRate, "Post-restore recall rate below threshold")

	// Verify deleted vectors still don't appear in results after restore
	for i := 0; i < numDeleted; i++ {
		results, _, err := newIndex.SearchByVector(context.TODO(), vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		for _, resultId := range results {
			assert.Greater(t, resultId, uint64(numDeleted),
				"Found deleted vector %d in search results after restore", resultId)
		}
	}
	t.Log("Verified deleted vectors remain excluded after persistence")
}

func TestCombinedAddAndDelete(t *testing.T) {
	index, _, cleanup, _ := setupTestIndex(t)
	defer cleanup()

	// First batch: Create initial vectors
	initialIds := make([]uint64, numVectors)
	initialVectors := make([][]float32, numVectors)
	for i := range initialIds {
		initialIds[i] = uint64(i + 1)
		initialVectors[i] = generateRandomVector(dims)
	}

	// Add initial vectors
	err := index.AddBatch(context.Background(), initialIds, initialVectors)
	require.NoError(t, err)

	// Delete some vectors from the initial batch
	for i := 0; i < numDeleted; i++ {
		err = index.Delete(initialIds[i])
		require.NoError(t, err)
	}

	// Second batch: Add new vectors
	newVectorsCount := numDeleted // Replace the deleted ones
	newIds := make([]uint64, newVectorsCount)
	newVectors := make([][]float32, newVectorsCount)
	for i := range newIds {
		newIds[i] = uint64(numVectors + i + 1) // Ensure unique IDs
		newVectors[i] = generateRandomVector(dims)
	}

	// Add new vectors
	err = index.AddBatch(context.Background(), newIds, newVectors)
	require.NoError(t, err)

	// Test recall for remaining original vectors
	t.Log("Testing recall for remaining original vectors...")
	totalRecallOriginal := 0.0
	for i := numDeleted; i < numVectors; i++ {
		results, _, err := index.SearchByVector(context.TODO(), initialVectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{initialIds[i]}, results)
		totalRecallOriginal += recall
	}

	recallRateOriginal := totalRecallOriginal / float64(numVectors-numDeleted)
	t.Logf("Recall rate for remaining original vectors: %.4f", recallRateOriginal)
	assert.GreaterOrEqual(t, recallRateOriginal, minRecallRate, "Recall rate for original vectors below threshold")

	// Test recall for newly added vectors
	t.Log("Testing recall for newly added vectors...")
	totalRecallNew := 0.0
	for i := 0; i < newVectorsCount; i++ {
		results, _, err := index.SearchByVector(context.TODO(), newVectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		recall := calculateRecall([]uint64{newIds[i]}, results)
		totalRecallNew += recall
	}

	recallRateNew := totalRecallNew / float64(newVectorsCount)
	t.Logf("Recall rate for new vectors: %.4f", recallRateNew)
	assert.GreaterOrEqual(t, recallRateNew, minRecallRate, "Recall rate for new vectors below threshold")

	// Verify deleted vectors don't appear in results
	t.Log("Verifying deleted vectors are not in search results...")
	for i := 0; i < numDeleted; i++ {
		results, _, err := index.SearchByVector(context.TODO(), initialVectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)

		// Check no deleted IDs appear in results
		for _, resultId := range results {
			assert.NotEqual(t, initialIds[i], resultId,
				"Found deleted vector %d in search results", initialIds[i])
		}
	}

	// Test batch queries for all vectors (remaining original + new)
	t.Log("Testing batch queries for all vectors...")
	allVectors := append(initialVectors[numDeleted:], newVectors...)
	batchResults, _, err := index.SearchByVectorBatch(allVectors, k, nil)
	require.NoError(t, err)

	totalBatchRecall := 0.0
	expectedIds := append(initialIds[numDeleted:], newIds...)

	for i := range batchResults {
		recall := calculateRecall([]uint64{expectedIds[i]}, batchResults[i])
		totalBatchRecall += recall
	}

	batchRecallRate := totalBatchRecall / float64(len(allVectors))
	t.Logf("Batch queries recall rate: %.4f", batchRecallRate)
	assert.GreaterOrEqual(t, batchRecallRate, minRecallRate, "Batch queries recall rate below threshold")
}
