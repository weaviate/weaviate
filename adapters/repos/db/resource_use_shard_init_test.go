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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// resourcePressureFixture is a lazy-load repo with one cold shard, wired so the
// resource scan can be driven by hand through the real disk-usage path.
type resourcePressureFixture struct {
	repo      *DB
	migrator  *Migrator
	index     *Index
	class     *models.Class
	className string
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
		repo: repo, migrator: migrator, index: index, class: class, className: className,
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
