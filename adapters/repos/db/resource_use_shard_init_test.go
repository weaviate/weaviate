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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// resourcePressureFixture is a lazy-load repo with one cold shard, wired so the
// resource scan can be driven by hand through the real disk-usage path.
type resourcePressureFixture struct {
	repo         *DB
	migrator     *Migrator
	schemaGetter *fakeSchemaGetter
	index        *Index
	class        *models.Class
	className    string
}

func newResourcePressureFixture(t *testing.T) *resourcePressureFixture {
	t.Helper()
	ctx := testCtx()
	className := "ResourcePressureLoad"

	repo, migrator, schemaGetter := newLazyLoadRepo(t, singleShardState())
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	class := newClassWithWarmProp(className)
	require.NoError(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)

	// The scan is a no-op unless the threshold is configured.
	repo.config.ResourceUsage.DiskUse.ReadOnlyPercentage = 90

	return &resourcePressureFixture{
		repo: repo, migrator: migrator, schemaGetter: schemaGetter,
		index: index, class: class, className: className,
	}
}

// coldShard returns the fixture's single shard, asserting it has not been
// loaded yet.
func (f *resourcePressureFixture) coldShard(t *testing.T) *LazyLoadShard {
	t.Helper()
	var cold *LazyLoadShard
	f.index.shards.Range(func(name string, shard ShardLike) error {
		lazyShard, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard %q should be a LazyLoadShard", name)
		require.False(t, lazyShard.isLoaded(), "shard %q should start cold", name)
		cold = lazyShard
		return nil
	})
	require.NotNil(t, cold)
	return cold
}

// A shard that loads while the resource scan holds the DB read-only must come
// up READONLY. The scan sweeps loaded shards only, so without inheritance a
// cold shard would come up READY and take the very writes the scan is stopping.
func TestLazyLoadShard_InheritsResourcePressureOnLoad(t *testing.T) {
	tests := []struct {
		name         string
		du           diskUse
		wantStatus   storagestate.Status
		wantReason   string
		wantReadOnly bool
	}{
		{
			name:         "disk over readonly threshold at load time",
			du:           diskUse{total: 100, free: 5, avail: 5},
			wantStatus:   storagestate.StatusReadOnly,
			wantReason:   statusReasonResourcePressure,
			wantReadOnly: true,
		},
		{
			name:       "disk below readonly threshold at load time",
			du:         diskUse{total: 100, free: 50, avail: 50},
			wantStatus: storagestate.StatusReady,
			wantReason: statusReasonNotifyReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			f := newResourcePressureFixture(t)
			cold := f.coldShard(t)

			f.repo.diskUseReadonly(tt.du)
			require.NoError(t, cold.Load(ctx))

			assert.Equal(t, tt.wantStatus, cold.GetStatus())
			assert.Equal(t, tt.wantReason, cold.GetStatusReason())

			err := cold.PutObject(ctx, testObject(f.className))
			if tt.wantReadOnly {
				require.ErrorContains(t, err, "store is read-only",
					"a shard loaded under resource pressure must reject writes")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// A tenant created while the resource scan holds the DB read-only must not come
// up writable either. The scan only ever sweeps the shards that exist when it
// runs, so a tenant added afterwards learns about the pressure when its shard is
// built - cold with lazy loading on (the default), eagerly with it off.
func TestNewTenant_InheritsResourcePressure(t *testing.T) {
	tests := []struct {
		name                 string
		lazyLoadShards       bool
		wantStatusOnCreation storagestate.Status
	}{
		{
			name:                 "lazy load shards",
			lazyLoadShards:       true,
			wantStatusOnCreation: storagestate.StatusLazyLoading,
		},
		{
			name:                 "eager shard creation",
			lazyLoadShards:       false,
			wantStatusOnCreation: storagestate.StatusReadOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			f := newResourcePressureFixture(t)
			f.index.Config.EnableLazyLoadShards = tt.lazyLoadShards

			// 95% disk usage, threshold is 90%
			f.repo.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

			const tenant = "tenant_added_under_pressure"
			require.NoError(t, f.migrator.NewTenants(ctx, f.class, []*schemaUC.CreateTenantPayload{
				{Name: tenant, Status: models.TenantActivityStatusHOT},
			}))

			shard := f.index.shards.Load(tenant)
			require.NotNil(t, shard)
			assert.Equal(t, tt.wantStatusOnCreation, shard.GetStatus())

			// A cold shard loads on the write, and must be read-only by the time
			// the write reaches the shard.
			require.ErrorContains(t, shard.PutObject(ctx, testObject(f.className)), "store is read-only",
				"a tenant created under resource pressure must not take writes")
			assert.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
			assert.Equal(t, statusReasonResourcePressure, shard.GetStatusReason())
		})
	}
}

// The status a shard inherits when it is built must carry the reason the
// recovery sweep looks for, or the shard would stay READONLY after the pressure
// clears.
func TestLazyLoadShard_InheritedReadOnlyRecovers(t *testing.T) {
	ctx := testCtx()
	f := newResourcePressureFixture(t)
	cold := f.coldShard(t)

	f.repo.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})
	require.NoError(t, cold.Load(ctx))
	require.Equal(t, storagestate.StatusReadOnly, cold.GetStatus())

	mon := newTestMemMonitor(0, 100)
	f.repo.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})

	assert.False(t, f.repo.resourceScanState.isReadOnly.Load())
	assert.Equal(t, storagestate.StatusReady, cold.GetStatus())
	require.NoError(t, cold.PutObject(ctx, testObject(f.className)),
		"a shard recovered from resource pressure must take writes again")
}

// testResourcePressureIndex returns a DB holding one empty index that knows its
// owning DB, i.e. an index that has already been published.
func testResourcePressureIndex(t *testing.T, readOnly bool) (*DB, *Index) {
	t.Helper()
	db := testResourceDB(t, 90, 0, nil)
	index := db.indices["TestIndex"]
	index.db = db
	db.resourceScanState.isReadOnly.Store(readOnly)
	return db, index
}

// An eagerly built shard reads the read-only flag before it reaches the shard
// map, so a transition in between reaches it through neither path: the sweep
// cannot see a shard that is not published yet, and neither sweep runs a second
// time within the same pressure episode. It has to settle when it is published.
func TestPublishShard_ReconcilesAgainstPressureTransition(t *testing.T) {
	tests := []struct {
		name           string
		statusAtBuild  ShardStatus
		readOnlyAtPush bool
		noOwningDB     bool
		wantStatus     storagestate.Status
		wantReason     string
	}{
		{
			name:           "built before pressure, published after",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady},
			readOnlyAtPush: true,
			wantStatus:     storagestate.StatusReadOnly,
			wantReason:     statusReasonResourcePressure,
		},
		{
			name:           "built under pressure, published after recovery",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure},
			readOnlyAtPush: false,
			wantStatus:     storagestate.StatusReady,
			wantReason:     statusReasonResourceRecovery,
		},
		{
			name:           "built and published under pressure",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure},
			readOnlyAtPush: true,
			wantStatus:     storagestate.StatusReadOnly,
			wantReason:     statusReasonResourcePressure,
		},
		{
			name:           "built and published without pressure",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady},
			readOnlyAtPush: false,
			wantStatus:     storagestate.StatusReady,
			wantReason:     statusReasonNotifyReady,
		},
		{
			name:           "read-only for a vector index update is not recovered",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate},
			readOnlyAtPush: false,
			wantStatus:     storagestate.StatusReadOnly,
			wantReason:     statusReasonVectorIndexUpdate,
		},
		{
			name:           "read-only for a vector index update keeps its reason under pressure",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate},
			readOnlyAtPush: true,
			wantStatus:     storagestate.StatusReadOnly,
			wantReason:     statusReasonVectorIndexUpdate,
		},
		{
			name:           "index without an owning DB leaves the shard alone",
			statusAtBuild:  ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady},
			readOnlyAtPush: true,
			noOwningDB:     true,
			wantStatus:     storagestate.StatusReady,
			wantReason:     statusReasonNotifyReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, index := testResourcePressureIndex(t, tt.readOnlyAtPush)
			if tt.noOwningDB {
				index.db = nil
			}
			shard, status, mu := newStatefulShardMock(t, tt.statusAtBuild)

			index.publishShard("shard1", shard)

			require.NotNil(t, index.shards.Load("shard1"), "the shard must be published")
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tt.wantStatus, status.Status)
			assert.Equal(t, tt.wantReason, status.Reason)
		})
	}
}

// A shard whose store closes while it is being published takes no writes
// either way, so the reconcile is routine there. Anything else is a real
// failure that must be logged - and either way the shard stays published and
// the publish path stays on its feet.
func TestPublishShard_ReconcileFailure(t *testing.T) {
	tests := []struct {
		name           string
		shardErr       error
		wantErrorLevel bool
	}{
		{
			name:     "store closed concurrently",
			shardErr: fmt.Errorf("%w: updating buckets state in store %q", lsmkv.ErrAlreadyClosed, "/data/shard"),
		},
		{
			name:           "unexpected error",
			shardErr:       fmt.Errorf("disk I/O error"),
			wantErrorLevel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, index := testResourcePressureIndex(t, true)
			hook := test.NewLocal(db.logger.(*logrus.Logger))

			shard := NewMockShardLike(t)
			shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
			shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(tt.shardErr)

			assert.NotPanics(t, func() { index.publishShard("shard1", shard) })

			require.NotNil(t, index.shards.Load("shard1"), "the shard must stay published")
			errLine := firstErrorEntry(hook)
			if tt.wantErrorLevel {
				require.NotNil(t, errLine, "an unexpected error must be logged")
				assert.Contains(t, errLine.Message, "shard1")
			} else {
				assert.Nil(t, errLine, "a shard closing concurrently is routine, not an error")
			}
		})
	}
}

// Publishing a cold shard must not reconcile it: a status change would force it
// to load, costing the very memory the scan may be reacting to. It reads the
// flag itself when it loads.
func TestPublishShard_LeavesColdShardUnloaded(t *testing.T) {
	_, index := testResourcePressureIndex(t, true)
	cold := &LazyLoadShard{shardOpts: &deferredShardOpts{name: "cold_shard"}}

	assert.NotPanics(t, func() { index.publishShard("cold_shard", cold) })

	assert.False(t, cold.isLoaded(), "publishing a cold shard must not load it")
}

// Shards built inside NewIndex are out of the scan's reach, so they settle when
// the index is published - however many of them there are, and whether or not
// some of them are still cold.
func TestReconcileIndexResourcePressure_ShardCounts(t *testing.T) {
	tests := []struct {
		name        string
		loadedCount int
	}{
		{name: "no loaded shards", loadedCount: 0},
		{name: "one loaded shard", loadedCount: 1},
		{name: "several loaded shards", loadedCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, index := testResourcePressureIndex(t, true)

			statuses := make([]*ShardStatus, 0, tt.loadedCount)
			locks := make([]*sync.Mutex, 0, tt.loadedCount)
			for n := range tt.loadedCount {
				shard, status, mu := newStatefulShardMock(t,
					ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady})
				index.shards.Store(fmt.Sprintf("loaded_shard_%d", n), shard)
				statuses = append(statuses, status)
				locks = append(locks, mu)
			}
			cold := &LazyLoadShard{shardOpts: &deferredShardOpts{name: "cold_shard"}}
			index.shards.Store("cold_shard", cold)

			db.reconcileIndexResourcePressure(index)

			for n, status := range statuses {
				locks[n].Lock()
				assert.Equal(t, storagestate.StatusReadOnly, status.Status)
				assert.Equal(t, statusReasonResourcePressure, status.Reason)
				locks[n].Unlock()
			}
			assert.False(t, cold.isLoaded(), "a cold shard must not be loaded when its index is published")
		})
	}
}

// The flag must not flip while a shard settles against it: the shard would
// apply the value it read before the flip, and the sweep following the flip has
// already walked the shard map.
func TestReconcileShardResourcePressure_HoldsOffFlagFlip(t *testing.T) {
	db, index := testResourcePressureIndex(t, true)

	settling := make(chan struct{})
	release := make(chan struct{})
	var flagDuringSettle atomic.Bool

	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).RunAndReturn(func(string) error {
		close(settling)
		<-release
		flagDuringSettle.Store(db.resourceScanState.isReadOnly.Load())
		return nil
	})

	published := make(chan struct{})
	enterrors.GoWrapper(func() {
		index.publishShard("shard1", shard)
		close(published)
	}, db.logger)
	<-settling

	flipped := make(chan struct{})
	enterrors.GoWrapper(func() {
		db.setReadOnlyFlag(false)
		close(flipped)
	}, db.logger)

	select {
	case <-flipped:
		require.Fail(t, "the read-only flag flipped while a shard was settling against it")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-published
	<-flipped

	assert.True(t, flagDuringSettle.Load(),
		"a settling shard must see one flag value for the whole transition")
}

// The eager publish path must reconcile the shard it publishes against the
// flag, not just hand it to the shard map. Holding the transition lock stalls
// the reconcile, opening the window a real transition would land in.
func TestLoadLocalShard_ReconcilesAfterPublishing(t *testing.T) {
	ctx := testCtx()
	f := newResourcePressureFixture(t)

	// 95% disk usage, threshold is 90%
	f.repo.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

	const shardName = "shard_published_across_recovery"
	published := make(chan error, 1)
	f.repo.resourceScanState.transition.Lock()
	enterrors.GoWrapper(func() {
		published <- f.index.LoadLocalShard(ctx, shardName, false)
	}, f.repo.logger)

	// publishShard reaches the shard map before it reconciles, so the shard is
	// now built and READONLY, waiting to reconcile.
	require.Eventually(t, func() bool { return f.index.shards.Load(shardName) != nil },
		10*time.Second, time.Millisecond, "the shard was never published")

	// The pressure clears while the shard sits in that window, i.e. at the point
	// a recovery sweep would already have walked past it.
	f.repo.resourceScanState.isReadOnly.Store(false)
	f.repo.resourceScanState.transition.Unlock()
	require.NoError(t, <-published)

	shard := f.index.shards.Load(shardName)
	require.NotNil(t, shard)
	assert.Equal(t, storagestate.StatusReady, shard.GetStatus())
	assert.Equal(t, statusReasonResourceRecovery, shard.GetStatusReason())
	require.NoError(t, shard.PutObject(ctx, testObject(f.className)),
		"a shard published after the pressure cleared must take writes")
}

// A collection created while the scan holds the DB read-only must not come up
// writable. Its shards are built inside NewIndex, where the sweep cannot reach
// them - the index is not in db.indices yet - so they reconcile when it is.
func TestNewCollection_InheritsResourcePressure(t *testing.T) {
	ctx := testCtx()
	f := newResourcePressureFixture(t)

	// Eager shards, the default for a collection without multi-tenancy.
	f.repo.config.EnableLazyLoadShards = boolPtr(false)

	// 95% disk usage, threshold is 90%
	f.repo.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

	const className = "CollectionAddedUnderPressure"
	class := newClassWithWarmProp(className)
	require.NoError(t, f.migrator.AddClass(ctx, class))
	f.schemaGetter.schema = schema.Schema{Objects: &models.Schema{
		Classes: []*models.Class{f.class, class},
	}}

	index := f.repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)

	eagerShards := 0
	require.NoError(t, index.ForEachLoadedShard(func(name string, shard ShardLike) error {
		eagerShards++
		assert.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
		assert.Equal(t, statusReasonResourcePressure, shard.GetStatusReason())
		require.ErrorContains(t, shard.PutObject(ctx, testObject(className)), "store is read-only",
			"a collection created under resource pressure must not take writes")
		return nil
	}))
	require.Positive(t, eagerShards, "the collection must have brought up at least one eager shard")
}

// A shard loading after the pressure cleared must come up READY: the scan drops
// its flag before the recovery sweep, so a shard that loads at any point around
// that sweep is either flipped by it or never marked in the first place.
func TestLazyLoadShard_NoInheritanceAfterRecovery(t *testing.T) {
	ctx := testCtx()
	f := newResourcePressureFixture(t)
	cold := f.coldShard(t)

	f.repo.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})
	mon := newTestMemMonitor(0, 100)
	f.repo.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})

	require.NoError(t, cold.Load(ctx))

	assert.Equal(t, storagestate.StatusReady, cold.GetStatus())
	require.NoError(t, cold.PutObject(ctx, testObject(f.className)))
}
