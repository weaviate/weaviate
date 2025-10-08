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

//go:build integrationTest

package db

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func createDataForLockTests(t *testing.T, shd ShardLike, className string, amount int) []strfmt.UUID {
	ctx := testCtx()
	ids := make([]strfmt.UUID, amount)
	t.Run("insert data into shard", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)
			err := shd.PutObject(ctx, obj)
			ids[i] = obj.ID()
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})
	return ids
}

func deleteDataForLockTests(t *testing.T, shd ShardLike, ids []strfmt.UUID) {
	ctx := testCtx()
	for i := 0; i < len(ids); i++ {
		err := shd.batchDeleteObject(ctx, ids[i], time.Now())
		require.Nil(t, err)
	}

	objs, err := shd.ObjectList(ctx, len(ids), nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
	require.Nil(t, err)
	require.Equal(t, 0, len(objs))
}

func testLock(t *testing.T, finished chan struct{}, testLock func() (bool, error)) (timeLocked int, changes int) {
	lastStatus := false
	lastChanged := time.Now()
	for {
		select {
		case <-finished:
			return timeLocked, changes
		default:

			status, err := testLock()
			if lastStatus != status {
				// was locked, now unlocked
				// count time locked
				if !status {
					timeLocked += int(time.Since(lastChanged).Nanoseconds())
				}
				lastStatus = status
				changes++
				lastChanged = time.Now()
			}
			require.Nil(t, err)
		}
	}
}

func TestShardLock_DocIdDeadlock(t *testing.T) {
	amount := 1000
	ctx := testCtx()
	className := "TestClass"
	shd, _ := testShard(t, ctx, className)

	ids := createDataForLockTests(t, shd, className, amount)
	eg := enterrors.NewErrorGroupWrapper(logrus.New())

	finished := make(chan struct{})
	eg.Go(func() error {
		t.Run("delete data from shard", func(t *testing.T) {
			deleteDataForLockTests(t, shd, ids)
		})
		close(finished)
		return nil
	})

	changes := 0
	timeLocked := 0
	eg.Go(func() error {
		timeLocked, changes = testLock(t, finished, shd.DebugGetDocIdLockStatus)
		return nil
	})

	err := eg.Wait()
	require.Nil(t, err)
	t.Logf("total time docID lock was held: %fms, count of lock changes: %d", float64(timeLocked)/1e6, changes)
	require.Greater(t, changes, 0, "expected at least one lock status change")
}

// this method tests the bucket_lock_debug.go methods using a real shard
// it is set here instead of lsmkv to reuse shard creation methods dependencies
func TestShardLock_ObjectsMaintenanceLock(t *testing.T) {
	amount := 1000
	ctx := testCtx()
	className := "TestClass"
	shd, _ := testShard(t, ctx, className)

	ids := createDataForLockTests(t, shd, className, amount)
	eg := enterrors.NewErrorGroupWrapper(logrus.New())

	finished := make(chan struct{})
	eg.Go(func() error {
		t.Run("delete data from shard", func(t *testing.T) {
			deleteDataForLockTests(t, shd, ids)
		})
		close(finished)
		return nil
	})

	changes := 0
	timeLocked := 0
	objectsBucket := shd.Store().Bucket(helpers.ObjectsBucketLSM)
	eg.Go(func() error {
		timeLocked, changes = testLock(t, finished, objectsBucket.DebugGetSegmentGroupLockStatus)
		return nil
	})

	err := eg.Wait()
	require.Nil(t, err)
	t.Logf("total time objects bucket lock was held: %fms, count of lock changes: %d", float64(timeLocked)/1e6, changes)
	require.Greater(t, changes, 0, "expected at least one lock status change")
}
