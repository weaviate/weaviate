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
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestStoreBackup_PauseCompaction(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert that context timeout works for long compactions", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacks("classCompaction", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCycleCallbacksNoop()

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)
				}

				expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()

				err = store.PauseCompaction(expiredCtx)
				require.NotNil(t, err)
				assert.Equal(t, "long-running compaction in progress:"+
					" deactivating callback 'store/compaction/.' of 'classCompaction' failed:"+
					" context deadline exceeded", err.Error())

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})

	t.Run("assert compaction is successfully paused", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacks("classCompaction", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCycleCallbacksNoop()

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)

					t.Run("insert contents into bucket", func(t *testing.T) {
						bucket := store.Bucket(bucket)
						for i := 0; i < 10; i++ {
							err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
							require.Nil(t, err)
						}
					})
				}

				expirableCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()

				err = store.PauseCompaction(expirableCtx)
				assert.Nil(t, err)

				err = store.Shutdown(context.Background())
				require.Nil(t, err)
			})
		}
	})
}

func TestStoreBackup_ResumeCompaction(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert compaction restarts after pausing", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacks("classCompaction", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCycleCallbacksNoop()

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)

					t.Run("insert contents into bucket", func(t *testing.T) {
						bucket := store.Bucket(bucket)
						for i := 0; i < 10; i++ {
							err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
							require.Nil(t, err)
						}
					})
				}

				expirableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				err = store.PauseCompaction(expirableCtx)
				require.Nil(t, err)

				err = store.ResumeCompaction(expirableCtx)
				require.Nil(t, err)

				assert.True(t, store.cycleCallbacks.compactionCallbacksCtrl.IsActive())

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})
}

func TestStoreBackup_FlushMemtable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("assert that context timeout works for long flushes", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacksNoop()
				shardFlushCallbacks := cyclemanager.NewCycleCallbacks("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)
				}

				expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()

				err = store.FlushMemtables(expiredCtx)
				require.NotNil(t, err)
				assert.Equal(t, "long-running memtable flush in progress:"+
					" deactivating callback 'store/flush/.' of 'classFlush' failed:"+
					" context deadline exceeded", err.Error())

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})

	t.Run("assert that flushes run successfully", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacksNoop()
				shardFlushCallbacks := cyclemanager.NewCycleCallbacks("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				err = store.CreateOrLoadBucket(ctx, "test_bucket")
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)

					t.Run("insert contents into bucket", func(t *testing.T) {
						bucket := store.Bucket(bucket)
						for i := 0; i < 10; i++ {
							err := bucket.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
							require.Nil(t, err)
						}
					})
				}

				expirableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				err = store.FlushMemtables(expirableCtx)
				assert.Nil(t, err)

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})

	t.Run("assert that readonly bucket fails to flush", func(t *testing.T) {
		singleErr := errors.Wrap(storagestate.ErrStatusReadOnly, "flush memtable")
		expectedErr := func(bucketsCount int) error {
			ec := &errorcompounder.ErrorCompounder{}
			for i := 0; i < bucketsCount; i++ {
				ec.Add(singleErr)
			}
			return ec.ToError()
		}

		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCycleCallbacksNoop()
				shardFlushCallbacks := cyclemanager.NewCycleCallbacks("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, shardCompactionCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket)
					require.Nil(t, err)
				}

				store.UpdateBucketsStatus(storagestate.StatusReadOnly)

				expirableCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				err = store.FlushMemtables(expirableCtx)
				require.NotNil(t, err)
				assert.EqualError(t, expectedErr(len(buckets)), err.Error())

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})
}
