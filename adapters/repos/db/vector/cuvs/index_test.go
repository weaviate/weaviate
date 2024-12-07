package cuvs_index

import (
	"context"
	"math/rand"
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
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
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
		results, _, err := index.SearchByVector(vectors[i], k, nil)
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
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
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
		results, _, err := newIndex.SearchByVector(vectors[i], k, nil)
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
		results, _, err := index.SearchByVector(vectors[i], k, nil)
		require.NoError(t, err)
		require.Len(t, results, k)
		t.Log("results: ", results)
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
		// log batchResults as int
		t.Log("batchResults: ", batchResults[i])
		rc := calculateRecall([]uint64{ids[i]}, batchResults[i])
		totalBatchRecall += rc
	}

	batchRecallRate := totalBatchRecall / float64(numVectors)
	t.Logf("Batch queries recall rate: %.4f", batchRecallRate)
	assert.GreaterOrEqual(t, batchRecallRate, minRecallRate, "Batch queries recall rate below threshold")
}
