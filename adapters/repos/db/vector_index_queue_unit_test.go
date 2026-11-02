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

	"github.com/stretchr/testify/require"
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

// ValidateDims must not leave dimensions reserved behind a rejection when it is
// used for several targets of one object: a write supplying vectors for two
// targets, where the second is inconsistent with the queue, must not fix the
// first target at the rejected write's length.
func TestVectorIndexQueueCheckThenReserveKeepsReservationAllOrNothing(t *testing.T) {
	queue := &VectorIndexQueue{}
	queue.asyncEnabled = true

	// A first legitimate 16-dimensional vector takes the dimensions.
	require.NoError(t, queue.ValidateDims(16))

	// A later write supplies 3 dimensions for this target and is rejected.
	require.Error(t, queue.CheckDims(3))

	// The rejection must not have changed the reserved dimensions, so the
	// original 16 is still what the queue accepts.
	require.NoError(t, queue.CheckDims(16))
	require.Equal(t, int32(16), queue.dims.Load())
}

// CheckDims reserves nothing on its own, so a caller can validate every target
// of one object before committing any of them.
func TestVectorIndexQueueCheckDimsDoesNotReserve(t *testing.T) {
	queue := &VectorIndexQueue{}
	queue.asyncEnabled = true

	require.NoError(t, queue.CheckDims(16))
	require.Equal(t, int32(0), queue.dims.Load(), "CheckDims must not reserve dimensions")

	queue.ReserveDims(16)
	require.Equal(t, int32(16), queue.dims.Load())
}

// ReserveDims is a no-op once dimensions are taken, so committing after a
// successful check cannot overwrite an established length.
func TestVectorIndexQueueReserveDimsKeepsFirstLength(t *testing.T) {
	queue := &VectorIndexQueue{}
	queue.asyncEnabled = true

	queue.ReserveDims(16)
	queue.ReserveDims(3)

	require.Equal(t, int32(16), queue.dims.Load())
}
