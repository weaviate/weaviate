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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newEmptyMTIndex builds a multi-tenant index with no local shards. dropShards
// for any tenant name therefore takes the "unloaded" (!ok) branch, which
// deletes the on-disk subtree without a Store to Shutdown each bucket.
func newEmptyMTIndex(t *testing.T) *Index {
	t.Helper()
	rootPath := t.TempDir()
	logger := logrus.New()

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1").Maybe()

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	ss := &sharding.State{
		Physical:            map[string]sharding.Physical{},
		PartitioningEnabled: true,
	}
	ss.SetLocalName("node1")

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})
	shardResolver := resolver.NewShardResolver(class.Class, true, mockSchemaGetter)

	index, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          rootPath,
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	return index
}

func tenantIDBucketPath(idx *Index, tenant string) string {
	return filepath.Join(shardPathLSM(idx.path(), tenant),
		helpers.BucketFromPropNameLSM(filters.InternalPropID))
}

// The dropShards unloaded (!ok) branch deletes a tenant's on-disk subtree with
// no Store to shut each bucket down, so it must also purge the subtree's
// registry keys by prefix: a bucket entry stranded there by an earlier
// incomplete teardown would otherwise survive the delete and fail a same-name
// restore's TryAdd. The batch row additionally pins the plumbing: every
// unloaded name dropShards collects must reach the single RemoveByPrefixes
// scan (a lost name passes both the single-tenant row and the registry unit
// tests).
func TestDropShardsRegistry_StrandedEntryPurged(t *testing.T) {
	tests := []struct {
		name        string
		drop        []string
		leak        []string // tenants whose property__id we pre-register (simulated strand)
		wantPurged  []string
		wantSurvive []string // prefix-boundary guard: a sibling must NOT be purged
	}{
		{
			name:       "purges the dropped tenant's stranded id bucket",
			drop:       []string{"t1"},
			leak:       []string{"t1"},
			wantPurged: []string{"t1"},
		},
		{
			name:        "a batch purges every dropped tenant in one scan, never a sibling",
			drop:        []string{"t1", "t2", "t3"},
			leak:        []string{"t1", "t2", "t3", "t10", "t20"},
			wantPurged:  []string{"t1", "t2", "t3"},
			wantSurvive: []string{"t10", "t20"}, // t1 !⊑ t10, t2 !⊑ t20
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newEmptyMTIndex(t)

			for _, tenant := range tt.leak {
				p := tenantIDBucketPath(idx, tenant)
				require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p))
				t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })
			}

			require.NoError(t, idx.dropShards(tt.drop))

			for _, tenant := range tt.wantPurged {
				p := tenantIDBucketPath(idx, tenant)
				require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
					"dropShards must leave no registry residue for the deleted tenant's lsm subtree")
				lsmkv.GlobalBucketRegistry.Remove(p)
			}
			for _, tenant := range tt.wantSurvive {
				p := tenantIDBucketPath(idx, tenant)
				require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(p), lsmkv.ErrBucketAlreadyRegistered,
					"dropShards must not purge a sibling tenant's registry entry (prefix over-match)")
			}
		})
	}
}

// In the unloaded (!ok) branch the shard variable is always nil, so the
// os.RemoveAll error path must not touch it: a shard.ID() call there panics on
// a nil-interface deref. dropShards must surface the RemoveAll failure as an
// error instead.
//
// The errgroup wrapper recovers panics by default and dropShards returns its
// error-compounder (not the errgroup error), which would mask such a panic as
// a plain error. Recovery is disabled so a deref is observable as a crash
// while the correct path returns the RemoveAll error cleanly.
func TestDropShardsRegistry_UnloadedRemoveAllFailureDoesNotPanic(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: filesystem permissions are ignored, cannot inject a RemoveAll failure")
	}
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")

	idx := newEmptyMTIndex(t)

	const tenant = "t1"
	// Materialise the cold tenant's dir so os.RemoveAll has something to remove,
	// then revoke write on its parent so the removal fails with EACCES.
	require.NoError(t, os.MkdirAll(shardPath(idx.path(), tenant), 0o700))
	t.Cleanup(func() { _ = os.Chmod(idx.path(), 0o700) })
	require.NoError(t, os.Chmod(idx.path(), 0o500))

	err := idx.dropShards([]string{tenant})
	require.Error(t, err, "dropShards must surface the RemoveAll failure rather than panic")
}

// dropShards fans its per-tenant work across errgroup goroutines that each call
// ec.Add on failure. With a non-thread-safe errorcompounder.New(), >=2 tenants
// failing concurrently is a data race and a potentially lost error; NewSafe()
// makes ec.Add safe. Force several cold tenants to fail their os.RemoveAll at
// once and run under -race.
func TestDropShardsRegistry_ConcurrentCompounderNoRace(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: filesystem permissions are ignored, cannot inject a RemoveAll failure")
	}

	idx := newEmptyMTIndex(t)

	tenants := []string{"t1", "t2", "t3", "t4", "t5", "t6"}
	for _, tenant := range tenants {
		require.NoError(t, os.MkdirAll(shardPath(idx.path(), tenant), 0o700))
	}
	// Revoke write on the class dir so every child os.RemoveAll fails with EACCES
	// concurrently, driving concurrent ec.Add from the errgroup goroutines.
	t.Cleanup(func() { _ = os.Chmod(idx.path(), 0o700) })
	require.NoError(t, os.Chmod(idx.path(), 0o500))

	err := idx.dropShards(tenants)
	require.Error(t, err, "dropShards must compound the concurrent RemoveAll failures")
}

// A concurrent init that already published a shard for a just-dropped tenant must
// NOT have its live registry entry purged. The realistic adversary is an Incoming*
// replica-write ensure-init or a lazy/on-demand init, both of which register
// buckets under shardCreateLocks(name). purgeUnloadedShardRegistry holds each
// name's shardCreateLocks across the gate and the scan; seeding i.shards before
// the call pins the "init completed first" branch, where the purge takes t1's
// lock uncontended and the gate skips it (the TryAdd-before-Store interleave is
// pinned by TestDropShardsRegistry_BatchPurgeGate_InFlightInit).
func TestDropShardsRegistry_BatchPurgeGate(t *testing.T) {
	idx := newEmptyMTIndex(t)

	// t1: a shard is present (a concurrent init won the race after the drop
	// goroutine released shardCreateLocks). t2: absent — residue to purge.
	idx.shards.Store("t1", &LazyLoadShard{})
	t.Cleanup(func() { idx.shards.LoadAndDelete("t1") }) // LIFO: before index.Shutdown

	p1 := tenantIDBucketPath(idx, "t1")
	p2 := tenantIDBucketPath(idx, "t2")
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p1))
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p2))
	t.Cleanup(func() {
		lsmkv.GlobalBucketRegistry.Remove(p1)
		lsmkv.GlobalBucketRegistry.Remove(p2)
	})

	idx.purgeUnloadedShardRegistry([]string{"t1", "t2"})

	require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(p1), lsmkv.ErrBucketAlreadyRegistered,
		"gate must skip a tenant present in i.shards, preserving its live registration")
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p2),
		"an absent tenant's residue must still be purged in the batch")
	lsmkv.GlobalBucketRegistry.Remove(p2) // re-added by the probe above
}

// The i.shards gate alone is not enough: getOptInitLocalShard registers a
// shard's buckets (GlobalBucketRegistry.TryAdd in NewBucket) BEFORE it publishes
// the shard via i.shards.Store, both under shardCreateLocks.Lock(name). A purge
// that read only the gate, holding no per-name lock, could observe the
// TryAdd'd-but-unpublished bucket as absent and remove a LIVE registration —
// disabling the registry's sole double-open guard (a later open of the same path
// would TryAdd successfully and yield two *Bucket on the same segment files).
// purgeUnloadedShardRegistry therefore holds shardCreateLocks(N) across BOTH the
// gate and the RemoveByPrefixes scan, making the purge mutually exclusive with
// the init's TryAdd→Store window.
//
// The test pins that mutual exclusion deterministically: an in-flight init holds
// Lock(N) with N's id bucket TryAdd'd but not yet published; the purge is
// spawned, then the init is released to publish N and drop the lock. The purge
// cannot pass Lock(N) until then, so its gate necessarily runs AFTER Store(N)
// and skips N — N's live registration survives. The handoff cannot deadlock.
// (A testing/synctest variant is impossible: synctest does not treat mutex
// acquisition as durably blocking, so synctest.Wait hangs once the purge parks
// on shardCreateLocks.)
func TestDropShardsRegistry_BatchPurgeGate_InFlightInit(t *testing.T) {
	idx := newEmptyMTIndex(t)

	const tenant = "t1"
	// LIFO, before newEmptyMTIndex's Shutdown cleanup: drop the shard the init
	// goroutine publishes so Shutdown does not tear down a zero-value shard.
	t.Cleanup(func() { idx.shards.LoadAndDelete(tenant) })

	p := tenantIDBucketPath(idx, tenant)
	t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })

	locked := make(chan struct{})  // closed once the init holds Lock(N) and has TryAdd'd
	publish := make(chan struct{}) // signals the init to Store + Unlock
	purgeDone := make(chan struct{})

	// In-flight init: hold shardCreateLocks(N) across the TryAdd→Store window,
	// exactly as getOptInitLocalShard does.
	go func() {
		idx.shardCreateLocks.Lock(tenant)
		_ = lsmkv.GlobalBucketRegistry.TryAdd(p)
		close(locked)
		<-publish
		idx.shards.Store(tenant, &LazyLoadShard{})
		idx.shardCreateLocks.Unlock(tenant)
	}()

	<-locked // init now holds Lock(N) with N's bucket registered but unpublished

	go func() {
		idx.purgeUnloadedShardRegistry([]string{tenant})
		close(purgeDone)
	}()

	// Release the init: it publishes N and drops the lock. The purge is blocked
	// on Lock(N) until here, so its gate runs after Store(N) and skips N.
	close(publish)
	<-purgeDone

	require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(p), lsmkv.ErrBucketAlreadyRegistered,
		"the purge must block on shardCreateLocks(N) until the init publishes N, "+
			"then skip it — leaving N's live registration intact")
	lsmkv.GlobalBucketRegistry.Remove(p) // re-added by the probe above
}
