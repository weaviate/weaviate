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

	op := reassignOperation{
		PostingID: 1,
		VectorID:  vectorID,
	}

	err = tf.Index.doReassign(t.Context(), op)
	require.NoError(t, err)

	deleted, err := tf.Index.VersionMap.IsDeleted(t.Context(), vectorID)
	require.NoError(t, err)
	require.True(t, deleted)
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

	err := tf.Index.doReassign(t.Context(), op)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get vector by index ID")
}

// Basic deduplicator functionality
func TestReassignDeduplicatorBasic(t *testing.T) {
	tf := createHFreshIndex(t)

	dedup := tf.Index.taskQueue.reassignList

	added := dedup.tryAdd(100, 1)
	require.True(t, added, "first add should succeed")

	added = dedup.tryAdd(100, 2)
	require.False(t, added, "duplicate add should fail")

	postingID := dedup.getLastKnownPostingID(100)
	require.Equal(t, uint64(2), postingID)

	added = dedup.tryAdd(100, 3)
	require.False(t, added, "update should fail (already exists)")

	postingID = dedup.getLastKnownPostingID(100)
	require.Equal(t, uint64(3), postingID)
}

// Done removes entry
func TestReassignDeduplicatorDone(t *testing.T) {
	tf := createHFreshIndex(t)

	dedup := tf.Index.taskQueue.reassignList

	added := dedup.tryAdd(200, 1)
	require.True(t, added)

	dedup.done(200)

	added = dedup.tryAdd(200, 2)
	require.True(t, added, "should be able to add again after done")

	postingID := dedup.getLastKnownPostingID(200)
	require.Equal(t, uint64(2), postingID)
}

// Flushing to persistent store
func TestReassignDeduplicatorFlush(t *testing.T) {
	tf := createHFreshIndex(t)

	dedup := tf.Index.taskQueue.reassignList

	dedup.tryAdd(300, 1)
	dedup.tryAdd(301, 2)
	dedup.tryAdd(302, 3)

	err := dedup.flush()
	require.NoError(t, err)

	newDedup, err := newReassignDeduplicator(dedup.bucket)
	require.NoError(t, err)

	require.Equal(t, uint64(1), newDedup.getLastKnownPostingID(300))
	require.Equal(t, uint64(2), newDedup.getLastKnownPostingID(301))
	require.Equal(t, uint64(3), newDedup.getLastKnownPostingID(302))
}

// Reassign properly manages task queue
func TestReassignTaskQueueOperations(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorID := uint64(600)
	postingID := uint64(1)
	version := VectorVersion(1)

	err := tf.Index.taskQueue.EnqueueReassign(postingID, vectorID, version)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueReassign(postingID, vectorID, version)
	require.NoError(t, err)

	tf.Index.taskQueue.ReassignDone(vectorID)

	err = tf.Index.taskQueue.EnqueueReassign(postingID, vectorID, version)
	require.NoError(t, err)
}
