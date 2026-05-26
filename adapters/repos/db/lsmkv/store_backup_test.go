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

func TestStoreBackup(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "pauseCompaction",
			f:    pauseCompaction,
			opts: []BucketOption{WithStrategy(StrategyReplace)},
		},
		{
			name: "resumeCompaction",
			f:    resumeCompaction,
			opts: []BucketOption{WithStrategy(StrategyReplace)},
		},
		{
			name: "flushMemtable",
			f:    flushMemtable,
			opts: []BucketOption{WithStrategy(StrategyReplace)},
		},
	}
	tests.run(ctx, t)
}

func pauseCompaction(ctx context.Context, t *testing.T, opts []BucketOption) {
	logger, _ := test.NewNullLogger()

	t.Run("expired context does not block the deactivate step when no compaction is in flight", func(t *testing.T) {
		// Mirrors the FlushMemtables case: with no compaction in flight,
		// Deactivate applies the mutation and returns nil even on expired
		// ctx. The previous assertion was pinning the buggy "always error
		// on expired ctx" behaviour that weaviate/0-weaviate-issues#250
		// fixes for the delete path.
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1)
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCallbackGroupNoop()

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
					require.Nil(t, err)
				}

				expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()

				err = store.PauseCompaction(expiredCtx)
				require.Nil(t, err)

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

				shardCompactionCallbacks := cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1)
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCallbackGroupNoop()

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
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

func resumeCompaction(ctx context.Context, t *testing.T, opts []BucketOption) {
	logger, _ := test.NewNullLogger()

	t.Run("assert compaction restarts after pausing", func(t *testing.T) {
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1)
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1)
				shardFlushCallbacks := cyclemanager.NewCallbackGroupNoop()

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
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
				assert.True(t, store.cycleCallbacks.compactionAuxCallbacksCtrl.IsActive())

				err = store.Shutdown(ctx)
				require.Nil(t, err)
			})
		}
	})
}

func flushMemtable(ctx context.Context, t *testing.T, opts []BucketOption) {
	logger, _ := test.NewNullLogger()

	t.Run("expired context does not block the deactivate step when no flush is in flight", func(t *testing.T) {
		// Under the new cyclemanager semantics, ctx.Err() != nil no longer
		// fails the Deactivate mutation when no callback is currently
		// running — the mutation applies and the call returns nil. The
		// previous assertion (always error on expired ctx, even with
		// nothing in flight) was pinning the buggy behaviour that the
		// delete-path fix on weaviate/0-weaviate-issues#250 removes.
		for _, buckets := range [][]string{
			{"test_bucket"},
			{"test_bucket1", "test_bucket2"},
			{"test_bucket1", "test_bucket2", "test_bucket3", "test_bucket4", "test_bucket5"},
		} {
			t.Run(fmt.Sprintf("with %d buckets", len(buckets)), func(t *testing.T) {
				dirName := t.TempDir()

				shardCompactionCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardFlushCallbacks := cyclemanager.NewCallbackGroup("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
					require.Nil(t, err)
				}

				expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()

				err = store.FlushMemtables(expiredCtx)
				require.Nil(t, err)

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

				shardCompactionCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardFlushCallbacks := cyclemanager.NewCallbackGroup("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				err = store.CreateOrLoadBucket(ctx, "test_bucket", opts...)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
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
			ec := errorcompounder.New()
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

				shardCompactionCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardCompactionAuxCallbacks := cyclemanager.NewCallbackGroupNoop()
				shardFlushCallbacks := cyclemanager.NewCallbackGroup("classFlush", logger, 1)

				store, err := New(dirName, dirName, logger, nil, nil,
					shardCompactionCallbacks, shardCompactionAuxCallbacks, shardFlushCallbacks)
				require.Nil(t, err)

				for _, bucket := range buckets {
					err = store.CreateOrLoadBucket(ctx, bucket, opts...)
					require.Nil(t, err)
				}

				err = store.UpdateBucketsStatus(storagestate.StatusReadOnly)
				require.NoError(t, err)

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
