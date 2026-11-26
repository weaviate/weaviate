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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
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

// The index keeps part of its data in the shard's buckets (e.g. multivector
// mappings), so the hook writes their logs out before flushing the index, and
// before the queue removes the chunk.
func TestVectorIndexQueueOnBatchProcessedWritesLSMLogs(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	require.NoError(t, store.CreateOrLoadBucket(t.Context(), "mappings", lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	bucket := store.Bucket("mappings")
	// small enough to stay in the log's buffer
	require.NoError(t, bucket.Put([]byte("node"), []byte("doc")))
	before := walSize(t, bucket.GetDir())

	var atFlush int64
	index := &flushRecordingIndex{Index: noop.NewIndex(), onFlush: func() {
		atFlush = walSize(t, bucket.GetDir())
	}}
	queue := &VectorIndexQueue{shard: &Shard{store: store}, vectorIndex: index}

	require.NoError(t, queue.OnBatchProcessed())
	require.Greater(t, atFlush, before, "the bucket log was not written out before the index flush")
}

type flushRecordingIndex struct {
	*noop.Index

	onFlush func()
}

func (f *flushRecordingIndex) Flush() error {
	f.onFlush()
	return nil
}

// walSize returns the size on disk of the write-ahead logs in a bucket dir.
func walSize(t *testing.T, dir string) int64 {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)

	var size int64
	for _, p := range paths {
		fi, err := os.Stat(p)
		require.NoError(t, err)
		size += fi.Size()
	}
	return size
}
