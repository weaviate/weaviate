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
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// pinTestShard builds a single-property shard holding objectCount objects and
// the task that migrates it, with the migration already started so the next
// OnAfterLsmInitAsync call enters the iteration.
func pinTestShard(t *testing.T, ctx context.Context, className string, objectCount int,
) (*Shard, *Index, *ShardReindexTaskGeneric) {
	t.Helper()

	shd, idx := testShardWithSettings(t, ctx, newTestClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)

	for i := 0; i < objectCount; i++ {
		require.NoError(t, shard.PutObject(ctx,
			createTestObjectWithText(className, "hello world "+uuid.NewString())))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	return shard, idx, task
}

func pinTestReindexBucketName() string {
	return helpers.BucketSearchableFromPropNameLSM("title") + "__blockmax_reindex_1"
}

// TestReindexBucketPinHoldsOffTeardown proves the reindex-bucket pins are real:
// a shutdown that lands while the iteration is mid-chunk has to wait for the
// writes to finish, because tearing the bucket down under them would unmap the
// segments they are writing into.
func TestReindexBucketPinHoldsOffTeardown(t *testing.T) {
	ctx := testCtx()
	shard, idx, task := pinTestShard(t, ctx, "TestReindexPinDrain", 3)

	parked := make(chan struct{})
	resume := make(chan struct{})

	// Emits one row, then parks the iteration mid-chunk. Ending without the
	// completion sentinel keeps the chunk incomplete, so the pins drop on
	// return rather than at the explicit release the completion path takes.
	task.objectsIteratorAsync = func(logger logrus.FieldLogger, _ ShardLike, _ indexKey,
		_ func([]byte) indexKey, _ *storobj.PropertyExtraction, _ time.Time,
		breakCh <-chan bool, _ map[string]inverted.PropertyOverlay,
	) (time.Time, <-chan *migrationData) {
		mdCh := make(chan *migrationData)
		enterrors.GoWrapper(func() {
			defer close(mdCh)
			<-breakCh
			key, err := uuid.New().MarshalBinary()
			require.NoError(t, err)
			mdCh <- &migrationData{key: uuidBytes(key)}
			close(parked)
			<-resume
		}, logger)
		return time.Now(), mdCh
	}

	iterated := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := task.OnAfterLsmInitAsync(ctx, shard)
		iterated <- err
	}, idx.logger)

	select {
	case <-parked:
	case err := <-iterated:
		t.Fatalf("iteration finished without reaching the write loop: %v", err)
	}

	torn := make(chan error, 1)
	enterrors.GoWrapper(func() {
		torn <- shard.store.ShutdownBucket(ctx, pinTestReindexBucketName())
	}, idx.logger)

	select {
	case err := <-torn:
		t.Fatalf("teardown completed while the iteration still held the reindex bucket: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	close(resume)
	require.NoError(t, <-iterated)

	select {
	case err := <-torn:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("teardown never completed after the iteration released its pins")
	}
}

// TestReindexIterationFailsOnDeregisteredReindexBucket covers the read that
// arrives once the reindex bucket is already gone: the iteration must report
// it instead of writing every posting through a nil bucket.
func TestReindexIterationFailsOnDeregisteredReindexBucket(t *testing.T) {
	ctx := testCtx()
	shard, _, task := pinTestShard(t, ctx, "TestReindexPinMissing", 3)

	require.NoError(t, shard.store.ShutdownBucket(ctx, pinTestReindexBucketName()))

	_, err := task.OnAfterLsmInitAsync(ctx, shard)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}

// TestObjectsIteratorFailsOnDeregisteredObjectsBucket covers the scan that
// starts once the objects bucket is already gone: it must report the missing
// bucket on its channel instead of dereferencing nil, which the goroutine's
// panic recovery would turn into a scan that never starts and never returns.
func TestObjectsIteratorFailsOnDeregisteredObjectsBucket(t *testing.T) {
	ctx := testCtx()
	shard, idx, _ := pinTestShard(t, ctx, "TestObjectsPinMissing", 3)

	require.NoError(t, shard.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))

	breakCh := make(chan bool, 1)
	breakCh <- false

	reported := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, mdCh := uuidObjectsIteratorAsync(idx.logger, shard, nil, (&UuidKeyParser{}).FromBytes,
			storobj.NewPropExtraction(), time.Now(), breakCh, nil)
		md := <-mdCh
		require.NotNil(t, md)
		reported <- md.err
		for range mdCh {
		}
	}, idx.logger)

	select {
	case err := <-reported:
		require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
	case <-time.After(30 * time.Second):
		t.Fatal("the scan neither started nor reported the missing objects bucket")
	}
}
