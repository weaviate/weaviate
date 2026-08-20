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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShardShutdownWhenIdle(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	// use shard
	shard, release1, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, shard)
	require.NotNil(t, release1)

	// use same shard
	sameShard, release2, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, sameShard)
	require.NotNil(t, release2)

	// sanity check, no flags marked
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, false)

	// release shard 2x
	release1()
	release2()

	// shutdown succeeds, shard idle
	err = shard.Shutdown(context.Background())
	require.NoError(t, err)
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, true)
}

func TestShardShutdownWhenIdleEventually(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	// use shard
	shard, release1, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, shard)
	require.NotNil(t, release1)

	// use same shard
	sameShard, release2, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, sameShard)
	require.NotNil(t, release2)

	// sanity check, no flags marked
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, false)

	// shutdown fails, shard in use 2x
	err = shard.Shutdown(context.Background())
	require.ErrorContains(t, err, "still in use")
	requireShardShutdownRequested(t, shard, true)
	requireShardShut(t, shard, false)

	// getting shard fails, shutdown in progress
	sameShardAgain, _, err := index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errShutdownInProgress)
	require.Nil(t, sameShardAgain)

	// release shard 1x
	release1()

	// shutdown still in progress, shard in use 1x
	requireShardShutdownRequested(t, shard, true)
	requireShardShut(t, shard, false)

	// release shard 1x
	release2()

	// shutdown eventually completed, shard idle
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, true)

	// getting shard fails, shutdown completed
	sameShardYetAgain, _, err := index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errAlreadyShutdown)
	require.Nil(t, sameShardYetAgain)
}

func initIndexAndPopulate(t *testing.T, dirName string) (index *Index, cleanup func()) {
	logger, _ := test.NewNullLogger()
	className := "Test"

	// create db
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		EnableLazyLoadShards:      boolPtr(true),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	err = repo.WaitForStartup(testCtx())
	require.NoError(t, err)

	cleanup = func() { repo.Shutdown(context.Background()) }
	runCleanup := true // run cleanup if method fails
	defer func() {
		if runCleanup {
			cleanup()
		}
	}()

	// set schema
	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	migrator := NewMigrator(repo, logger, "node1")
	err = migrator.AddClass(context.Background(), class)
	require.NoError(t, err)
	schemaGetter.schema = sch

	// import objects
	for i := 0; i < 10; i++ {
		v := float32(i)
		vec := []float32{v, v + 1, v + 2, v + 3}

		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
		obj := &models.Object{Class: className, ID: id}
		err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
		require.NoError(t, err)
	}

	index = repo.GetIndex(schema.ClassName(className))
	runCleanup = false // all good, let caller cleanup
	return index, cleanup
}

func requireShardShutdownRequested(t *testing.T, shard ShardLike, expected bool) {
	if expected {
		require.True(t, shard.(*LazyLoadShard).shard.shutdownRequested.Load(), "shard should be marked for shut down")
	} else {
		require.False(t, shard.(*LazyLoadShard).shard.shutdownRequested.Load(), "shard should not be marked for shut down")
	}
}

func requireShardShut(t *testing.T, shard ShardLike, expected bool) {
	if expected {
		require.True(t, shard.(*LazyLoadShard).shard.shut.Load(), "shard should be marked as shut down")
	} else {
		require.False(t, shard.(*LazyLoadShard).shard.shut.Load(), "shard should not be marked as shut down")
	}
}

// TestShardReinitAfterDeferredShutdown pins the write-path reactivation belt:
// once the deferred ref-drain shutdown COMPLETES, the map still holds the shut
// instance — the read path keeps surfacing errAlreadyShutdown (the
// eventual-shutdown contract above), but getOrInitShard must evict the
// known-shut entry and re-initialize instead of pinning the tenant on
// errAlreadyShutdown until restart.
func TestShardReinitAfterDeferredShutdown(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	_, release, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)

	shard := index.shards.Load(shardName)
	require.ErrorContains(t, shard.Shutdown(context.Background()), "still in use")
	release() // deferred completion fires here

	requireShardShut(t, shard, true)

	// Read path: terminal error, per the eventual-shutdown contract.
	_, _, err = index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errAlreadyShutdown)

	// Write path: evict + re-init, fresh usable shard.
	fresh, freshRelease, err := index.getOrInitShard(context.Background(), shardName)
	require.NoError(t, err, "a known-shut map entry must be re-initialized, not served terminally")
	require.NotNil(t, fresh)
	freshRelease()

	// And the read path works again through the fresh instance.
	_, release2, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	release2()
}

// TestShutdownOrRestoreShard_ConcurrentCompletionIsNotAFailure pins the race
// between an explicit Shutdown and the deferred ref-drain completion: when
// the deferred completion wins while the explicit attempt times out, the
// stale attempt error must not surface — the shard IS shut, and reporting
// failure would fail e.g. a whole cold-tenant batch on one racy tenant.
func TestShutdownOrRestoreShard_ConcurrentCompletionIsNotAFailure(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	_, release, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)

	shard, _ := index.shards.LoadAndDelete(shardName)
	require.NotNil(t, shard)

	// Attempt 1 sees the shard in use; the ctx dies before attempt 2
	// (backoff 200ms); the release at ~50ms lets the deferred completion
	// finish the shutdown cleanly in between.
	go func() {
		time.Sleep(50 * time.Millisecond)
		release()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err = shutdownOrRestoreShard(ctx, &index.shards, shardName, shard, index.logger)
	require.ErrorIs(t, err, errAlreadyShutdown,
		"a concurrently-completed shutdown is the requested outcome, not a failure")
	require.Nil(t, index.shards.Load(shardName), "a cleanly shut shard is not restored")
}

// TestUnloadLocalShard_ErrorClassification pins which unload outcomes reach the
// caller as failures. shutdownOrRestoreShard reports a shard that is already
// shut as errAlreadyShutdown, which is the outcome an unload asked for, so it
// must not compound into a caller's error list. A refused unload, a torn shard
// and a closing index still must. Each of those leaves the shard loaded.
func TestUnloadLocalShard_ErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		// setup arranges the shard state and returns the name to unload plus
		// the ctx the unload runs under.
		setup          func(t *testing.T, index *Index, shardName string) (string, context.Context)
		wantErr        error
		wantStillInMap bool
	}{
		{
			name: "idle shard unloads cleanly",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				return shardName, context.Background()
			},
		},
		{
			name: "shard already gone from the map is a no-op",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				return "no-such-shard", context.Background()
			},
		},
		{
			name: "concurrently completed shutdown reports success",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				_, release, err := index.GetShard(context.Background(), shardName)
				require.NoError(t, err)
				// The release lands between attempt 1 (shard in use) and the
				// ctx deadline, so the deferred completion shuts it cleanly
				// while the explicit attempt is still timing out.
				enterrors.GoWrapper(func() {
					time.Sleep(50 * time.Millisecond)
					release()
				}, index.logger)
				ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
				t.Cleanup(cancel)
				return shardName, ctx
			},
		},
		{
			name: "shard still in use stays refused and loaded",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				_, release, err := index.GetShard(context.Background(), shardName)
				require.NoError(t, err)
				t.Cleanup(release)
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				t.Cleanup(cancel)
				return shardName, ctx
			},
			// Shutdown retries under backoff while the ref is held, so the
			// refusal surfaces as the exhausted deadline.
			wantErr:        context.DeadlineExceeded,
			wantStillInMap: true,
		},
		{
			// The one state that must never read as already-shut: the shard is
			// shut but still holds the handles a failed teardown left open.
			name: "torn shard stays a failure",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				tearShard(t, index.shards.Load(shardName))
				return shardName, context.Background()
			},
			wantErr:        errTeardownFailed,
			wantStillInMap: true,
		},
		{
			name: "closing index refuses the unload",
			setup: func(t *testing.T, index *Index, shardName string) (string, context.Context) {
				require.NoError(t, index.beginClose())
				return shardName, context.Background()
			},
			wantErr:        errAlreadyShutdown,
			wantStillInMap: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			index, cleanup := initIndexAndPopulate(t, t.TempDir())
			defer cleanup()

			var shardName string
			index.shards.Range(func(name string, _ ShardLike) error {
				shardName = name
				return nil
			})

			target, ctx := tc.setup(t, index, shardName)

			err := index.UnloadLocalShard(ctx, target)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tc.wantStillInMap {
				require.NotNil(t, index.shards.Load(target),
					"a shard that was not unloaded must stay served")
			} else {
				require.Nil(t, index.shards.Load(target))
			}
		})
	}
}

// tearShard puts a loaded shard into the state a shutdown that failed
// mid-teardown leaves behind: marked shut, but still holding the handles the
// failure never released.
func tearShard(t *testing.T, s ShardLike) {
	t.Helper()

	var inner *Shard
	switch sh := s.(type) {
	case *Shard:
		inner = sh
	case *LazyLoadShard:
		require.True(t, sh.isLoaded(), "only a loaded shard can be torn")
		inner = sh.shard
	default:
		t.Fatalf("cannot tear a %T", s)
	}

	inner.shut.Store(true)
	inner.teardownErr = errors.New("bucket close failed")
}
