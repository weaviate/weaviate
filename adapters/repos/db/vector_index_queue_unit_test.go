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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
)

func TestVectorIndexQueueDeleteUsesDeleteMultiForSyncMultivectorIndex(t *testing.T) {
	index := newRecordingMultiVectorIndex()
	queue := &VectorIndexQueue{vectorIndex: index}

	err := queue.Delete(10, 11)

	require.NoError(t, err)
	require.Empty(t, index.deleteCalls)
	require.Equal(t, [][]uint64{{10, 11}}, index.deleteMultiCalls)
}

func TestVectorIndexQueueMultiDeleteTaskUsesDeleteMulti(t *testing.T) {
	index := newRecordingMultiVectorIndex()
	task := &Task[[][]float32]{
		op:  vectorIndexQueueMultiDeleteOp,
		id:  12,
		idx: index,
	}

	err := task.Execute(context.Background())

	require.NoError(t, err)
	require.Empty(t, index.deleteCalls)
	require.Equal(t, [][]uint64{{12}}, index.deleteMultiCalls)
}

func TestVectorIndexQueueMultiDeleteTaskGroupUsesDeleteMulti(t *testing.T) {
	index := newRecordingMultiVectorIndex()
	taskGroup := &TaskGroup[[][]float32]{
		op:  vectorIndexQueueMultiDeleteOp,
		ids: []uint64{13, 14},
		idx: index,
	}

	err := taskGroup.Execute(context.Background())

	require.NoError(t, err)
	require.Empty(t, index.deleteCalls)
	require.Equal(t, [][]uint64{{13, 14}}, index.deleteMultiCalls)
}

type recordingMultiVectorIndex struct {
	*noop.Index

	deleteCalls      [][]uint64
	deleteMultiCalls [][]uint64
}

func newRecordingMultiVectorIndex() *recordingMultiVectorIndex {
	return &recordingMultiVectorIndex{Index: noop.NewIndex()}
}

func (r *recordingMultiVectorIndex) Delete(ids ...uint64) error {
	r.deleteCalls = append(r.deleteCalls, append([]uint64(nil), ids...))
	return nil
}

func (r *recordingMultiVectorIndex) DeleteMulti(ids ...uint64) error {
	r.deleteMultiCalls = append(r.deleteMultiCalls, append([]uint64(nil), ids...))
	return nil
}

func (r *recordingMultiVectorIndex) Multivector() bool {
	return true
}

type mockDimensionIndex struct {
	*noop.Index
	dim int
}

func (m *mockDimensionIndex) Dimension() int {
	return m.dim
}

func newAsyncTestQueue(t *testing.T, vi VectorIndex) *VectorIndexQueue {
	t.Helper()

	s := queue.NewScheduler(queue.SchedulerOptions{ScheduleInterval: time.Hour})
	s.Start()
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	viq := &VectorIndexQueue{
		scheduler:    s,
		shard:        &Shard{},
		vectorIndex:  vi,
		asyncEnabled: true,
		metrics:      NewVectorIndexQueueMetrics(logrus.New(), nil, "test", "test", ""),
	}

	dq, err := queue.NewDiskQueue(queue.DiskQueueOptions{
		ID:          "vector_index_queue_test",
		Scheduler:   s,
		Dir:         t.TempDir(),
		TaskDecoder: &vectorIndexQueueDecoder{q: viq},
	})
	require.NoError(t, err)
	require.NoError(t, dq.Init())
	t.Cleanup(func() { _ = dq.Close(context.Background()) })
	viq.DiskQueue = dq

	s.RegisterQueue(viq)
	return viq
}

func TestVectorIndexQueue_Insert_RejectsInconsistentVectors(t *testing.T) {
	ctx := context.Background()
	index := &mockDimensionIndex{Index: noop.NewIndex(), dim: 0}
	viq := newAsyncTestQueue(t, index)

	v1 := &common.Vector[[]float32]{ID: 1, Vector: []float32{1.0, 2.0, 3.0}}
	v2 := &common.Vector[[]float32]{ID: 2, Vector: []float32{1.0, 2.0, 3.0, 4.0}}

	// Inconsistent batch: first vector is 3D, second vector is 4D
	err := viq.Insert(ctx, v1, v2)
	require.Error(t, err)
	require.ErrorIs(t, err, common.ErrWrongDimensions)
	require.ErrorContains(t, err, "inconsistent vector lengths: 4 != 3")

	// Verify nothing was queued in disk queue
	require.Zero(t, viq.Size())

	// Verify dims was not poisoned, subsequent valid batch of 4D vectors can be inserted
	v3 := &common.Vector[[]float32]{ID: 3, Vector: []float32{0.1, 0.2, 0.3, 0.4}}
	v4 := &common.Vector[[]float32]{ID: 4, Vector: []float32{0.5, 0.6, 0.7, 0.8}}
	err = viq.Insert(ctx, v3, v4)
	require.NoError(t, err)
	require.Equal(t, int64(2), viq.Size())
}

func TestVectorIndexQueue_Insert_RejectsEstablishedDimensionMismatch(t *testing.T) {
	ctx := context.Background()
	index := &mockDimensionIndex{Index: noop.NewIndex(), dim: 4}
	viq := newAsyncTestQueue(t, index)

	// Single vector with length 3 when established dimension is 4
	v1 := &common.Vector[[]float32]{ID: 1, Vector: []float32{1.0, 2.0, 3.0}}

	err := viq.Insert(ctx, v1)
	require.Error(t, err)
	require.ErrorIs(t, err, common.ErrWrongDimensions)
	require.ErrorContains(t, err, "inconsistent vector lengths: 3 != 4")

	// Verify nothing was queued
	require.Zero(t, viq.Size())

	// Valid vector matching established dimension succeeds
	vValid := &common.Vector[[]float32]{ID: 2, Vector: []float32{1.0, 2.0, 3.0, 4.0}}
	err = viq.Insert(ctx, vValid)
	require.NoError(t, err)
	require.Equal(t, int64(1), viq.Size())
}
