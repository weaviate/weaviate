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

package replication_test

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/cluster/replication"
)

func randomInt(t *testing.T, min, max int) int {
	t.Helper()
	var randValue [1]byte
	_, err := rand.Read(randValue[:])
	if err != nil {
		t.Fatal("error generating random value:", err)
	}
	return min + int(randValue[0])%(max-min+1)
}

func randomOpId(t *testing.T) uint64 {
	t.Helper()
	var opId uint64
	err := binary.Read(rand.Reader, binary.BigEndian, &opId)
	if err != nil {
		t.Fatal("failed to generate random opId")
	}
	return opId
}

func randomBool(t *testing.T) bool {
	t.Helper()
	var buf [1]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		t.Fatal("failed to generate random bool")
	}
	return buf[0]%2 == 0
}

func TestOpTracker(t *testing.T) {
	t.Run("add a new operation", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId := randomOpId(t)

		assert.False(t, tracker.IsOpInProgress(opId), "operation should not be in progress initially")
		assert.False(t, tracker.IsOpCompleted(opId), "operation should not be completed initially")

		tracker.AddOp(opId)

		assert.True(t, tracker.IsOpInProgress(opId), "operation should be in progress after adding")
		assert.False(t, tracker.IsOpCompleted(opId), "operation should not be completed after adding")
	})

	t.Run("mark operation as completed", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId := randomOpId(t)

		tracker.AddOp(opId)
		tracker.CompleteOp(opId)

		assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress")
		assert.True(t, tracker.IsOpCompleted(opId), "operation should be completed")
	})

	t.Run("remove completed operation", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId := randomOpId(t)

		tracker.AddOp(opId)
		tracker.CompleteOp(opId)
		tracker.CleanUpOp(opId)

		assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress after cleanup")
		assert.False(t, tracker.IsOpCompleted(opId), "operation should no longer be completed after cleanup")
	})

	t.Run("do not add operation if it already exists", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId := randomOpId(t)
		tracker.AddOp(opId)

		assert.True(t, tracker.IsOpInProgress(opId), "operation should be in progress after adding again")

		tracker.CompleteOp(opId)

		assert.True(t, tracker.IsOpCompleted(opId), "operation should still be marked as completed after re-adding")
	})

	t.Run("track multiple operations independently", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId1 := randomOpId(t)
		opId2 := randomOpId(t)

		tracker.AddOp(opId1)
		tracker.AddOp(opId2)

		assert.True(t, tracker.IsOpInProgress(opId1), "operation 1 should be in progress")
		assert.True(t, tracker.IsOpInProgress(opId2), "operation 2 should be in progress")

		tracker.CompleteOp(opId1)

		assert.True(t, tracker.IsOpCompleted(opId1), "operation 1 should be completed")
		assert.True(t, tracker.IsOpInProgress(opId2), "operation 2 should still be in progress")

		tracker.CompleteOp(opId2)

		assert.True(t, tracker.IsOpCompleted(opId2), "operation 2 should be completed")
	})

	t.Run("cleanup completed operations", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})
		opId := randomOpId(t)

		tracker.AddOp(opId)
		tracker.CompleteOp(opId)

		assert.True(t, tracker.IsOpCompleted(opId), "operation should be completed")

		tracker.CleanUpOp(opId)

		assert.False(t, tracker.IsOpInProgress(opId), "operation should not be in progress after cleanup")
		assert.False(t, tracker.IsOpCompleted(opId), "operation should not be completed after cleanup")
	})

	t.Run("random sequence of completed operations", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})

		for i := 0; i < 100; i++ {
			opId := randomOpId(t)

			tracker.AddOp(opId)
			assert.True(t, tracker.IsOpInProgress(opId), "operation should be in progress after adding")

			if randomBool(t) {
				tracker.CompleteOp(opId)
				assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress after completing")
				assert.True(t, tracker.IsOpCompleted(opId), "operation should be completed")
			}
		}
	})

	t.Run("random sequence of cleanup operations", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})

		for i := 0; i < randomInt(t, 100, 200); i++ {
			opId := randomOpId(t)

			assert.False(t, tracker.IsOpInProgress(opId), "operation should not be in progress after adding")
			tracker.AddOp(opId)
			assert.True(t, tracker.IsOpInProgress(opId), "operation should be in progress after adding")

			if randomBool(t) {
				tracker.CleanUpOp(opId)
				assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress after completing")
				assert.False(t, tracker.IsOpCompleted(opId), "operation should not be completed")
			}
		}
	})

	t.Run("random sequence of operations (complete or cleanup)", func(t *testing.T) {
		tracker := replication.NewOpTracker(replication.RealTimeProvider{})

		for i := 0; i < randomInt(t, 100, 200); i++ {
			opId := randomOpId(t)

			assert.False(t, tracker.IsOpInProgress(opId), "operation should not be in progress after adding")
			tracker.AddOp(opId)
			assert.True(t, tracker.IsOpInProgress(opId), "operation should be in progress after adding")

			if randomBool(t) {
				tracker.CompleteOp(opId)
				assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress after completing")
				assert.True(t, tracker.IsOpCompleted(opId), "operation should be completed")
			} else {
				tracker.CleanUpOp(opId)
				assert.False(t, tracker.IsOpInProgress(opId), "operation should no longer be in progress after cleanup")
				assert.False(t, tracker.IsOpCompleted(opId), "operation should not be completed")
			}
		}
	})
}
