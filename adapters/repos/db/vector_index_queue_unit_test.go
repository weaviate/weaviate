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
