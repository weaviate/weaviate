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

package hfresh

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Reassign a vector that has been deleted
func TestReassignDeletedVector(t *testing.T) {
	tf := createHFreshIndex(t)

	vector := []float32{1.0, 0.0, 0.0, 0.0}
	vectorID := uint64(1000)
	addVectorToIndex(t, &tf, vectorID, vector)

	_, err := tf.Index.VersionMap.MarkDeleted(t.Context(), vectorID)
	require.NoError(t, err)
	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))

	op := reassignOperation{
		PostingID: 1,
		VectorID:  vectorID,
	}

	err = tf.Index.doReassign(t.Context(), op)
	require.NoError(t, err)

	deleted, err := tf.Index.VersionMap.IsDeleted(t.Context(), vectorID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))
}

// Reassign a vector that doesn't exist
func TestReassignVectorNotFound(t *testing.T) {
	tf := createHFreshIndex(t)

	initVector := []float32{1.0, 0.0, 0.0, 0.0}
	addVectorToIndex(t, &tf, 1, initVector)

	tf.Index.config.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, errors.New("vector not found")
	}

	op := reassignOperation{
		PostingID: 1,
		VectorID:  9999,
	}

	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(op.VectorID))
	err := tf.Index.doReassign(t.Context(), op)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get vector by index ID")
	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(op.VectorID))
}

// Basic in-memory deduplicator functionality
func TestReassignDeduplicatorBasic(t *testing.T) {
	tf := createHFreshIndex(t)

	dedup := tf.Index.taskQueue.reassignList

	added := dedup.tryAdd(100)
	require.True(t, added, "first add should succeed")

	added = dedup.tryAdd(100)
	require.False(t, added, "duplicate add should fail")

	added = dedup.tryAdd(100)
	require.False(t, added, "update should fail (already exists)")
}

// Done removes entry
func TestReassignDeduplicatorDone(t *testing.T) {
	tf := createHFreshIndex(t)

	dedup := tf.Index.taskQueue.reassignList

	added := dedup.tryAdd(200)
	require.True(t, added)

	dedup.done(200)

	added = dedup.tryAdd(200)
	require.True(t, added, "should be able to add again after done")
}

func TestReassignDeduplicatorDoesNotPersistOnClose(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.taskQueue.EnqueueReassign(1, 300)
	require.NoError(t, err)

	err = tf.Index.taskQueue.Close(t.Context())
	require.NoError(t, err)

	data, err := tf.Index.IndexMetadata.bucket.Get(reassignBucketKey)
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestReassignEnqueuePushFailureClearsDedup(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorID := uint64(400)
	err := tf.Index.taskQueue.reassignQueue.Close(t.Context())
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueReassign(1, vectorID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to push reassign operation to queue")
	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))
}

// Reassign a vector whose version was concurrently changed should be skipped
func TestReassignConcurrentVersionChange(t *testing.T) {
	tf := createHFreshIndex(t)

	vector := []float32{1.0, 0.0, 0.0, 0.0}
	vectorID := uint64(1000)
	addVectorToIndex(t, &tf, vectorID, vector)

	// Override VectorForIDThunk to increment the version as a side effect,
	// simulating a concurrent reassign between Get and Increment.
	tf.Index.config.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		v, err := tf.Index.VersionMap.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		_, err = tf.Index.VersionMap.Increment(ctx, id, v)
		if err != nil {
			return nil, err
		}
		return vector, nil
	}

	op := reassignOperation{
		PostingID: 1,
		VectorID:  vectorID,
	}

	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))
	err := tf.Index.doReassign(t.Context(), op)
	require.NoError(t, err)
	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))
}

func TestReassignReenqueuesWhenSelectedPostingDisappears(t *testing.T) {
	tf := createHFreshIndex(t)

	vector := []float32{1.0, 0.0, 0.0, 0.0}
	vectorID := uint64(1000)
	addVectorToIndex(t, &tf, vectorID, vector)

	version, err := tf.Index.VersionMap.Get(t.Context(), vectorID)
	require.NoError(t, err)

	missingPostingID := uint64(4242)
	replicas := NewResultSet(1)
	replicas.data = append(replicas.data, Result{ID: missingPostingID})

	require.True(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))

	requeued, err := tf.Index.appendReassignReplicas(
		t.Context(),
		NewVector(vectorID, version, nil),
		replicas,
	)
	require.NoError(t, err)
	require.True(t, requeued)
	require.Equal(t, int64(1), tf.Index.taskQueue.reassignQueue.Size())
	require.False(t, tf.Index.taskQueue.reassignList.tryAdd(vectorID))
}

// Reassign properly manages task queue
func TestReassignTaskQueueOperations(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorID := uint64(600)
	postingID := uint64(1)

	err := tf.Index.taskQueue.EnqueueReassign(postingID, vectorID)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueReassign(postingID, vectorID)
	require.NoError(t, err)

	tf.Index.taskQueue.ReassignDone(vectorID)

	err = tf.Index.taskQueue.EnqueueReassign(postingID, vectorID)
	require.NoError(t, err)
}
