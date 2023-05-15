//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestStoreBackup_PauseCompaction(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert that context timeout works for long compactions", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		canceledCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()

		err = store.PauseCompaction(canceledCtx)
		require.NotNil(t, err)
		assert.Equal(t, "long-running compaction in progress: context deadline exceeded", err.Error())

		err = store.Shutdown(ctx)
		require.Nil(t, err)
	})

	t.Run("assert compaction is successfully paused", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		cancelableCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		t.Run("insert contents into bucket", func(t *testing.T) {
			bucket := store.Bucket("test_bucket")
			for i := 0; i < 10; i++ {
				err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				require.Nil(t, err)
			}
		})

		err = store.PauseCompaction(cancelableCtx)
		assert.Nil(t, err)

		err = store.Shutdown(context.Background())
		require.Nil(t, err)
	})
}

func TestStoreBackup_ResumeCompaction(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert compaction restarts after pausing", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		t.Run("insert contents into bucket", func(t *testing.T) {
			bucket := store.Bucket("test_bucket")
			for i := 0; i < 10; i++ {
				err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				require.Nil(t, err)
			}
		})

		cancelableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		err = store.PauseCompaction(cancelableCtx)
		require.Nil(t, err)

		err = store.ResumeCompaction(ctx)
		require.Nil(t, err)

		assert.True(t, store.compactionCycle.Running())

		err = store.Shutdown(ctx)
		require.Nil(t, err)
	})
}

func TestStoreBackup_FlushMemtable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert that context timeout works for long flushes", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		canceledCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()

		err = store.FlushMemtables(canceledCtx)
		require.NotNil(t, err)
		assert.Equal(t, "long-running memtable flush in progress: context deadline exceeded", err.Error())

		err = store.Shutdown(ctx)
		require.Nil(t, err)
	})

	t.Run("assert that flushes run successfully", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		t.Run("insert contents into bucket", func(t *testing.T) {
			bucket := store.Bucket("test_bucket")
			for i := 0; i < 10; i++ {
				err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				require.Nil(t, err)
			}
		})

		cancelableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		err = store.FlushMemtables(cancelableCtx)
		assert.Nil(t, err)

		err = store.Shutdown(ctx)
		require.Nil(t, err)
	})

	t.Run("assert that readonly bucket fails to flush", func(t *testing.T) {
		dirName := t.TempDir()

		store, err := New(dirName, dirName, logger, nil)
		require.Nil(t, err)

		err = store.CreateOrLoadBucket(ctx, "test_bucket")
		require.Nil(t, err)

		store.UpdateBucketsStatus(storagestate.StatusReadOnly)

		cancelableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		err = store.FlushMemtables(cancelableCtx)
		require.NotNil(t, err)
		expectedErr := errors.Wrap(storagestate.ErrStatusReadOnly, "flush memtable")
		assert.EqualError(t, expectedErr, err.Error())

		err = store.Shutdown(ctx)
		require.Nil(t, err)
	})
}
