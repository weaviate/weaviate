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

package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type vectorIndexMock struct {
	mu                 sync.Mutex
	addBatchCalls      int32
	addBatchIDs        [][]uint64
	addBatchVectors    [][][]float32
	addBatchErrors     []error
	addBatchCallTimes  []time.Time
	shouldFailAddBatch bool
	failAddBatchAfter  int
	addBatchDelay      time.Duration
	dims               int
}

func newVectorIndexMock() *vectorIndexMock {
	return &vectorIndexMock{
		addBatchIDs:       make([][]uint64, 0),
		addBatchVectors:   make([][][]float32, 0),
		addBatchErrors:    make([]error, 0),
		addBatchCallTimes: make([]time.Time, 0),
		dims:              4,
	}
}

func (v *vectorIndexMock) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if v.addBatchDelay > 0 {
		time.Sleep(v.addBatchDelay)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	callNum := int(atomic.AddInt32(&v.addBatchCalls, 1))

	idsCopy := make([]uint64, len(ids))
	copy(idsCopy, ids)
	v.addBatchIDs = append(v.addBatchIDs, idsCopy)

	vectorsCopy := make([][]float32, len(vectors))
	for i, vec := range vectors {
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)
		vectorsCopy[i] = vecCopy
	}
	v.addBatchVectors = append(v.addBatchVectors, vectorsCopy)
	v.addBatchCallTimes = append(v.addBatchCallTimes, time.Now())

	if v.shouldFailAddBatch && (v.failAddBatchAfter == 0 || callNum >= v.failAddBatchAfter) {
		err := enterrors.NewNotEnoughMemory("mock OOM in AddBatch")
		v.addBatchErrors = append(v.addBatchErrors, err)
		return err
	}

	v.addBatchErrors = append(v.addBatchErrors, nil)
	return nil
}

func (v *vectorIndexMock) AddMultiBatch(ctx context.Context, ids []uint64, vectors [][][]float32) error {
	return errors.New("not implemented")
}

func (v *vectorIndexMock) Add(ctx context.Context, id uint64, vector []float32) error {
	return v.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

func (v *vectorIndexMock) AddMulti(ctx context.Context, id uint64, vectors [][]float32) error {
	return errors.New("not implemented")
}

func (v *vectorIndexMock) Delete(ids ...uint64) error {
	return nil
}

func (v *vectorIndexMock) Multivector() bool {
	return false
}

func (v *vectorIndexMock) Flush() error {
	return nil
}

func (v *vectorIndexMock) getAddBatchCalls() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return int(atomic.LoadInt32(&v.addBatchCalls))
}

func (v *vectorIndexMock) getAddBatchCallsData() ([][]uint64, [][][]float32, []error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.addBatchIDs, v.addBatchVectors, v.addBatchErrors
}

func TestTaskGroup_AddBatchExecution(t *testing.T) {
	mockIndex := newVectorIndexMock()

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	tasks := make([]Task, len(ids))
	for i := range ids {
		tasks[i] = &mockBatchTask{
			id:     ids[i],
			vector: vectors[i],
			idx:    mockIndex,
		}
	}

	group := tasks[0].(*mockBatchTask).NewGroup(1, tasks...)

	err := group.Execute(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, mockIndex.getAddBatchCalls())

	callIDs, callVectors, _ := mockIndex.getAddBatchCallsData()
	require.Len(t, callIDs, 1)
	require.Len(t, callVectors, 1)

	assert.Equal(t, ids, callIDs[0])
	assert.Equal(t, vectors, callVectors[0])
}

func TestTaskGroup_AddBatchWithTransientError(t *testing.T) {
	mockIndex := newVectorIndexMock()
	mockIndex.shouldFailAddBatch = true

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	group := &mockBatchTaskGroup{
		op:      1,
		ids:     ids,
		vectors: vectors,
		idx:     mockIndex,
	}

	err := group.Execute(context.Background())
	require.Error(t, err)
	assert.True(t, enterrors.IsTransient(err))
}

func TestTaskGroup_AddBatchWithPermanentError(t *testing.T) {
	mockIndex := &vectorIndexMockWithPermanentError{}

	ids := []uint64{1, 2, 3}
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.2, 0.3, 0.4, 0.5},
		{0.3, 0.4, 0.5, 0.6},
	}

	group := &mockBatchTaskGroup{
		op:      1,
		ids:     ids,
		vectors: vectors,
		idx:     mockIndex,
	}

	err := group.Execute(context.Background())

	require.Error(t, err, "should return permanent error")
	assert.Equal(t, 1, mockIndex.addBatchCalls, "should execute once")
}

func TestTaskGroup_MixedInsertOperations(t *testing.T) {
	mockIndex := newVectorIndexMock()

	insertTasks := make([]Task, 3)
	for i := range insertTasks {
		insertTasks[i] = &mockBatchTask{
			id:     uint64(i),
			vector: []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)},
			idx:    mockIndex,
		}
	}

	group := insertTasks[0].(*mockBatchTask).NewGroup(1, insertTasks...)

	err := group.Execute(context.Background())
	require.NoError(t, err)

	callIDs, callVectors, _ := mockIndex.getAddBatchCallsData()
	require.Len(t, callIDs, 1)

	assert.Len(t, callIDs[0], 3)
	assert.Len(t, callVectors[0], 3)
}

func TestTaskGroup_ContextCancellation(t *testing.T) {
	mockIndex := newVectorIndexMock()

	tasks := make([]Task, 5)
	for i := range tasks {
		tasks[i] = &mockBatchTask{
			id:     uint64(i),
			vector: []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)},
			idx:    mockIndex,
		}
	}

	group := tasks[0].(*mockBatchTask).NewGroup(1, tasks...)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := group.Execute(ctx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestTaskGroup_LargeBatch(t *testing.T) {
	mockIndex := newVectorIndexMock()

	batchSize := 1000
	tasks := make([]Task, batchSize)
	for i := range tasks {
		tasks[i] = &mockBatchTask{
			id:     uint64(i),
			vector: []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)},
			idx:    mockIndex,
		}
	}

	group := tasks[0].(*mockBatchTask).NewGroup(1, tasks...)

	err := group.Execute(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, mockIndex.getAddBatchCalls())

	callIDs, callVectors, _ := mockIndex.getAddBatchCallsData()
	require.Len(t, callIDs, 1)
	assert.Len(t, callIDs[0], batchSize)
	assert.Len(t, callVectors[0], batchSize)
}

func TestTaskGroup_BatchProcessingOrder(t *testing.T) {
	mockIndex := newVectorIndexMock()

	tasks := make([]Task, 10)
	expectedIDs := make([]uint64, 10)
	for i := range tasks {
		id := uint64(i * 10)
		expectedIDs[i] = id
		tasks[i] = &mockBatchTask{
			id:     id,
			vector: []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)},
			idx:    mockIndex,
		}
	}

	group := tasks[0].(*mockBatchTask).NewGroup(1, tasks...)

	err := group.Execute(context.Background())
	require.NoError(t, err)

	callIDs, _, _ := mockIndex.getAddBatchCallsData()
	require.Len(t, callIDs, 1)
	assert.Equal(t, expectedIDs, callIDs[0])
}

type mockBatchTask struct {
	id     uint64
	vector []float32
	idx    VectorIndex
}

func (m *mockBatchTask) Key() uint64 {
	return m.id
}

func (m *mockBatchTask) Op() uint8 {
	return 1
}

func (m *mockBatchTask) Execute(ctx context.Context) error {
	return m.idx.Add(ctx, m.id, m.vector)
}

func (m *mockBatchTask) NewGroup(op uint8, tasks ...Task) Task {
	ids := make([]uint64, len(tasks))
	vectors := make([][]float32, len(tasks))

	for i, task := range tasks {
		t := task.(*mockBatchTask)
		ids[i] = t.id
		vectors[i] = t.vector
	}

	return &mockBatchTaskGroup{
		op:      op,
		ids:     ids,
		vectors: vectors,
		idx:     m.idx,
	}
}

type mockBatchTaskGroup struct {
	op      uint8
	ids     []uint64
	vectors [][]float32
	idx     VectorIndex
}

func (t *mockBatchTaskGroup) Op() uint8 {
	return t.op
}

func (t *mockBatchTaskGroup) Key() uint64 {
	if len(t.ids) > 0 {
		return t.ids[0]
	}
	return 0
}

func (t *mockBatchTaskGroup) Execute(ctx context.Context) error {
	return t.idx.AddBatch(ctx, t.ids, t.vectors)
}

type vectorIndexMockWithPermanentError struct {
	addBatchCalls int
}

func (v *vectorIndexMockWithPermanentError) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	v.addBatchCalls++
	return errors.New("permanent error: disk full")
}

func (v *vectorIndexMockWithPermanentError) AddMultiBatch(ctx context.Context, ids []uint64, vectors [][][]float32) error {
	return errors.New("not implemented")
}

func (v *vectorIndexMockWithPermanentError) Add(ctx context.Context, id uint64, vector []float32) error {
	return errors.New("permanent error: disk full")
}

func (v *vectorIndexMockWithPermanentError) AddMulti(ctx context.Context, id uint64, vectors [][]float32) error {
	return errors.New("not implemented")
}

func (v *vectorIndexMockWithPermanentError) Delete(ids ...uint64) error {
	return nil
}

func (v *vectorIndexMockWithPermanentError) Multivector() bool {
	return false
}

func (v *vectorIndexMockWithPermanentError) Flush() error {
	return nil
}

type VectorIndex interface {
	AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error
	AddMultiBatch(ctx context.Context, ids []uint64, vectors [][][]float32) error
	Add(ctx context.Context, id uint64, vector []float32) error
	AddMulti(ctx context.Context, id uint64, vectors [][]float32) error
	Delete(ids ...uint64) error
	Multivector() bool
	Flush() error
}
