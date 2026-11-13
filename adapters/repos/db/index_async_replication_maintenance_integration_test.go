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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	entreplication "github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type emptyTenantMaintenanceCase struct {
	name         string
	maintenance  func() bool
	globalConfig *entreplication.GlobalConfig
	wantLoaded   bool
}

func emptyTenantMaintenanceCases() []emptyTenantMaintenanceCase {
	on := func() bool { return true }
	off := func() bool { return false }
	withFlag := func(v bool) *entreplication.GlobalConfig {
		return &entreplication.GlobalConfig{AsyncReplicationInMaintenanceEnabled: configRuntime.NewDynamicValue(v)}
	}
	return []emptyTenantMaintenanceCase{
		{name: "no maintenance func, flag on", maintenance: nil, globalConfig: withFlag(true)},
		{name: "maintenance off, flag off", maintenance: off, globalConfig: withFlag(false)},
		{name: "maintenance off, flag on", maintenance: off, globalConfig: withFlag(true)},
		{name: "maintenance on, nil global config", maintenance: on, globalConfig: nil},
		{name: "maintenance on, nil flag", maintenance: on, globalConfig: &entreplication.GlobalConfig{}},
		{name: "maintenance on, flag off", maintenance: on, globalConfig: withFlag(false)},
		{name: "maintenance on, flag on", maintenance: on, globalConfig: withFlag(true), wantLoaded: true},
	}
}

// emptyTenantDiskStates covers a tenant written empty and one whose dir was lost with the disk.
var emptyTenantDiskStates = []struct {
	name   string
	onDisk bool
}{
	{name: "empty dir on disk", onDisk: true},
	{name: "dir lost", onDisk: false},
}

func seedEmptyTenant(t *testing.T, dirName, tenant string, onDisk bool) {
	t.Helper()
	if onDisk {
		seedWarmupTenants(t, dirName, map[string]warmupSeed{tenant: {}})
	}
}

func TestEmptyTenantLoadsAtStartupOnlyWhenAsyncReplicationRunsInMaintenance(t *testing.T) {
	const tenant = "empty-tenant"
	for _, disk := range emptyTenantDiskStates {
		for _, tt := range emptyTenantMaintenanceCases() {
			t.Run(disk.name+"/"+tt.name, func(t *testing.T) {
				dirName := t.TempDir()
				seedEmptyTenant(t, dirName, tenant, disk.onDisk)

				index, _ := newHotTenantsIndex(t, dirName,
					IndexConfig{MaintenanceModeEnabled: tt.maintenance}, tt.globalConfig, nil, tenant)
				defer func() { require.NoError(t, index.Shutdown(context.Background())) }()

				stored := index.shards.Load(tenant)
				require.NotNil(t, stored)
				lazy, isLazy := stored.(*LazyLoadShard)
				if tt.wantLoaded {
					require.False(t, isLazy, "want a loaded shard, got %T", stored)
					return
				}
				require.True(t, isLazy, "want a lazy shard, got %T", stored)
				require.False(t, lazy.isLoaded())
			})
		}
	}
}

func TestEmptyTenantWarmsUpOnlyWhenAsyncReplicationRunsInMaintenance(t *testing.T) {
	const tenant = "empty-tenant"
	for _, disk := range emptyTenantDiskStates {
		for _, tt := range emptyTenantMaintenanceCases() {
			t.Run(disk.name+"/"+tt.name, func(t *testing.T) {
				dirName := t.TempDir()
				seedEmptyTenant(t, dirName, tenant, disk.onDisk)

				index, _ := newHotTenantsIndex(t, dirName, IndexConfig{
					EnableLazyLoadShards:          true,
					LazyLoadShardWarmupMinObjects: -1,
					MaintenanceModeEnabled:        tt.maintenance,
				}, tt.globalConfig, nil, tenant)
				defer func() { require.NoError(t, index.Shutdown(context.Background())) }()

				lazy := coldWarmupShard(t, index, tenant)

				shouldWarm, candidateOutcome := index.warmupCandidate(tenant)
				outcome, err := index.loadLocalShardIfActive(tenant)
				require.NoError(t, err)

				if tt.wantLoaded {
					require.True(t, shouldWarm, "outcome %q", candidateOutcome)
					require.Equal(t, monitoring.WarmupLoaded, outcome)
					require.True(t, lazy.isLoaded())
					return
				}
				require.False(t, shouldWarm)
				require.Equal(t, monitoring.WarmupSkippedEmpty, candidateOutcome)
				require.Equal(t, monitoring.WarmupSkippedEmpty, outcome)
				require.False(t, lazy.isLoaded())
			})
		}
	}
}

func TestEmptyTenantStartupSweepOnlyWarmsWhenAsyncReplicationRunsInMaintenance(t *testing.T) {
	const tenant = "empty-tenant"
	for _, tt := range emptyTenantMaintenanceCases() {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			seedEmptyTenant(t, dirName, tenant, true)

			index, hook := newHotTenantsIndex(t, dirName, IndexConfig{
				EnableLazyLoadShards:   true,
				MaintenanceModeEnabled: tt.maintenance,
			}, tt.globalConfig, nil, tenant)
			defer func() { require.NoError(t, index.Shutdown(context.Background())) }()

			lazy := coldWarmupShard(t, index, tenant)
			require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond)

			if tt.wantLoaded {
				require.True(t, lazy.isLoaded())
				requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{monitoring.WarmupLoaded: 1})
				return
			}
			require.False(t, lazy.isLoaded())
			requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{monitoring.WarmupSkippedEmpty: 1})
		})
	}
}
