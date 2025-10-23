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

package hnsw

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type mockAllocChecker struct {
	mu            sync.Mutex
	shouldFail    bool
	failAfter     int
	callCount     int
	allocRequests []int64
}

func newMockAllocChecker() *mockAllocChecker {
	return &mockAllocChecker{
		allocRequests: make([]int64, 0),
	}
}

func (m *mockAllocChecker) CheckAlloc(sizeInBytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.allocRequests = append(m.allocRequests, sizeInBytes)
	m.callCount++

	if m.shouldFail && (m.failAfter == 0 || m.callCount >= m.failAfter) {
		return enterrors.NewNotEnoughMemory("mock OOM")
	}
	return nil
}

func (m *mockAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (m *mockAllocChecker) Refresh(updateMappings bool) {}

func (m *mockAllocChecker) getAllocRequests() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]int64, len(m.allocRequests))
	copy(result, m.allocRequests)
	return result
}

func TestAddBatch_EmptyBatch(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	err := index.AddBatch(context.Background(), []uint64{}, [][]float32{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty lists")
}

func TestAddBatch_MismatchedLengths(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{{0.1, 0.2}, {0.3, 0.4}}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sizes does not match")
}

func TestAddBatch_InconsistentVectorDimensions(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5},
		{0.6, 0.7, 0.8},
	}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different lengths")
}

func TestAddBatch_EmptyVector(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 2}
	vectors := [][]float32{{0.1, 0.2}, {}}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.Error(t, err)
}

func TestAddBatch_ContextCancellation(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ids := []uint64{1, 2}
	vectors := [][]float32{{0.1, 0.2}, {0.3, 0.4}}

	err := index.AddBatch(ctx, ids, vectors)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestAddBatch_SingleVector(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1}
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	assert.True(t, index.ContainsDoc(1))
}

func TestAddBatch_MultipleBatches(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	batch1IDs := []uint64{1, 2, 3}
	batch1Vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	err := index.AddBatch(context.Background(), batch1IDs, batch1Vectors)
	require.NoError(t, err)

	batch2IDs := []uint64{4, 5, 6}
	batch2Vectors := [][]float32{
		{0.4, 0.5, 0.6, 0.7},
		{0.5, 0.6, 0.7, 0.8},
		{0.6, 0.7, 0.8, 0.9},
	}

	err = index.AddBatch(context.Background(), batch2IDs, batch2Vectors)
	require.NoError(t, err)

	for _, id := range append(batch1IDs, batch2IDs...) {
		assert.True(t, index.ContainsDoc(id), "missing doc %d", id)
	}
}

func TestAddBatch_WrongDimensionsAfterInitialBatch(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	batch1IDs := []uint64{1, 2}
	batch1Vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
	}

	err := index.AddBatch(context.Background(), batch1IDs, batch1Vectors)
	require.NoError(t, err)

	batch2IDs := []uint64{3, 4}
	batch2Vectors := [][]float32{
		{0.3, 0.4, 0.5},
		{0.4, 0.5, 0.6},
	}

	err = index.AddBatch(context.Background(), batch2IDs, batch2Vectors)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vector lengths don't match")
}

func TestAddBatch_MemoryAllocationFailure(t *testing.T) {
	allocChecker := newMockAllocChecker()
	allocChecker.shouldFail = true

	index := testHNSWWithAllocChecker(t, allocChecker)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enough memory")

	requests := allocChecker.getAllocRequests()
	require.Len(t, requests, 1)

	expectedMemory := int64(3 * (4*4 + 30))
	assert.Equal(t, expectedMemory, requests[0])
}

func TestAddBatch_LargeBatch(t *testing.T) {
	batchSize := 1000
	dims := 128

	vectors := make(map[uint64][]float32)
	for i := 0; i < batchSize; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims+j) / float32(batchSize*dims)
		}
		vectors[uint64(i)] = vec
	}

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vectors[id], nil
	}

	cfg := Config{
		RootPath:              t.TempDir(),
		ID:                    "test-hnsw-large",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	defer index.Shutdown(context.Background())

	ids := make([]uint64, batchSize)
	vecs := make([][]float32, batchSize)
	for i := 0; i < batchSize; i++ {
		ids[i] = uint64(i)
		vecs[i] = vectors[uint64(i)]
	}

	err = index.AddBatch(context.Background(), ids, vecs)
	require.NoError(t, err)

	for _, id := range ids {
		assert.True(t, index.ContainsDoc(id))
	}
}

func TestAddBatch_WithGapInIDs(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 100, 1000}
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	err := index.AddBatch(context.Background(), ids, vectors)
	require.NoError(t, err)

	for _, id := range ids {
		assert.True(t, index.ContainsDoc(id))
	}
}

func TestAddBatch_UpdateExistingVectors(t *testing.T) {
	index := testHNSW(t)
	defer index.Shutdown(context.Background())

	ids := []uint64{1, 2, 3}
	initialVectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	err := index.AddBatch(context.Background(), ids, initialVectors)
	require.NoError(t, err)

	updatedVectors := [][]float32{
		{0.9, 0.8, 0.7, 0.6},
		{0.8, 0.7, 0.6, 0.5},
		{0.7, 0.6, 0.5, 0.4},
	}

	err = index.AddBatch(context.Background(), ids, updatedVectors)
	require.NoError(t, err)

	for _, id := range ids {
		assert.True(t, index.ContainsDoc(id))
	}
}

func TestAddBatch_ConcurrentAddBatchCalls(t *testing.T) {
	vectorStore := &sync.Map{}
	vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
		if vec, ok := vectorStore.Load(id); ok {
			return vec.([]float32), nil
		}
		return nil, fmt.Errorf("vector not found: %d", id)
	}

	index := testHNSWWithCustomVectorFunc(t, vectorForID)
	defer func(index *hnsw, ctx context.Context) {
		err := index.Shutdown(ctx)
		if err != nil {
			t.Errorf("shutdown error: %v", err)
		}
	}(index, context.Background())

	numGoroutines := 10
	vectorsPerGoroutine := 100
	dims := 64

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errs := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			offset := uint64(goroutineID * vectorsPerGoroutine)
			ids := make([]uint64, vectorsPerGoroutine)
			vectors := make([][]float32, vectorsPerGoroutine)

			for i := 0; i < vectorsPerGoroutine; i++ {
				ids[i] = offset + uint64(i)
				vectors[i] = make([]float32, dims)
				for j := 0; j < dims; j++ {
					vectors[i][j] = float32(goroutineID*vectorsPerGoroutine+i*dims+j) / 10000.0
				}
				vectorStore.Store(ids[i], vectors[i])
			}

			err := index.AddBatch(context.Background(), ids, vectors)
			if err != nil {
				errs <- err
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}

	expectedTotal := numGoroutines * vectorsPerGoroutine
	for i := 0; i < expectedTotal; i++ {
		assert.True(t, index.ContainsDoc(uint64(i)), "missing doc %d", i)
	}
}

func testHNSW(t *testing.T) *hnsw {
	return testHNSWWithAllocChecker(t, memwatch.NewDummyMonitor())
}

func testHNSWWithAllocChecker(t *testing.T, allocChecker memwatch.AllocChecker) *hnsw {
	cfg := Config{
		RootPath:              t.TempDir(),
		ID:                    "test-hnsw",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		AllocChecker:          allocChecker,
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	return index
}

func testHNSWWithCustomVectorFunc(t *testing.T, vectorForID func(context.Context, uint64) ([]float32, error)) *hnsw {
	return testHNSWWithCustomVectorFuncAndAllocChecker(t, vectorForID, memwatch.NewDummyMonitor())
}

func testHNSWWithCustomVectorFuncAndAllocChecker(
	t *testing.T,
	vectorForID func(context.Context, uint64) ([]float32, error),
	allocChecker memwatch.AllocChecker,
) *hnsw {
	cfg := Config{
		RootPath:              t.TempDir(),
		ID:                    "test-hnsw",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vectorForID,
		AllocChecker:          allocChecker,
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	return index
}
