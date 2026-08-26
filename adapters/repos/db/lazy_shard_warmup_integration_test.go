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
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	warmupClassName = "TestWarmupClass"
	warmupNodeName  = "test-node"
)

// newWarmupIndex opens a multi-tenant index over dirName, one lazy shard per
// tenant, with the startup sweep gated at minObjects.
func newWarmupIndex(t *testing.T, dirName string, minObjects int64, tenants ...string) *Index {
	t.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	class := newClassWithWarmProp(warmupClassName)
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	fakeSchema := schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	shardState := &sharding.State{
		Physical:            map[string]sharding.Physical{},
		PartitioningEnabled: true,
	}
	tenantStatus := map[string]string{}
	for _, tenant := range tenants {
		shardState.Physical[tenant] = sharding.Physical{
			Name:           tenant,
			BelongsToNodes: []string{warmupNodeName},
			Status:         models.TenantActivityStatusHOT,
		}
		tenantStatus[tenant] = models.TenantActivityStatusHOT
	}
	shardState.SetLocalName(warmupNodeName)

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, readerFunc func(*models.Class, *sharding.State) error) error {
			return readerFunc(class, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
	mockSchema.EXPECT().ReadOnlyClass(warmupClassName).Maybe().Return(class)
	mockSchema.EXPECT().NodeName().Maybe().Return(warmupNodeName)
	mockSchema.EXPECT().TenantsShards(mock.Anything, warmupClassName, mock.Anything).Maybe().
		Return(tenantStatus, nil)

	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().GetWriteReplicasLocation(warmupClassName, mock.Anything, mock.Anything).
		RunAndReturn(func(_, _, shard string) (types.WriteReplicaSet, error) {
			return types.WriteReplicaSet{Replicas: []types.Replica{
				{NodeName: warmupNodeName, ShardName: shard, HostAddr: "10.0.0.1"},
			}}, nil
		}).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(warmupClassName, mock.Anything, mock.Anything).
		RunAndReturn(func(_, _, shard string) (types.ReadReplicaSet, error) {
			return types.ReadReplicaSet{Replicas: []types.Replica{
				{NodeName: warmupNodeName, ShardName: shard, HostAddr: "10.0.0.1"},
			}}, nil
		}).Maybe()

	schemaGetter := &fakeSchemaGetter{schema: fakeSchema, shardState: shardState}
	shardResolver := resolver.NewShardResolver(warmupClassName, true, schemaGetter)

	index, err := NewIndex(ctx, nil, IndexConfig{
		RootPath:                      dirName,
		ClassName:                     schema.ClassName(warmupClassName),
		ReplicationFactor:             1,
		ShardLoadLimiter:              loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		EnableLazyLoadShards:          true,
		LazyLoadShardWarmupMinObjects: minObjects,
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
		mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil,
		class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)

	return index
}

// coldWarmupShard returns a tenant's shard, asserting it is an unloaded lazy one.
func coldWarmupShard(t *testing.T, index *Index, tenant string) *LazyLoadShard {
	t.Helper()
	stored := index.shards.Load(tenant)
	require.NotNil(t, stored, "shard of tenant %q must be registered after init", tenant)
	lazy, ok := stored.(*LazyLoadShard)
	require.True(t, ok, "lazy mode should store a wrapper, got %T", stored)
	require.False(t, lazy.isLoaded(), "shard of tenant %q must not be loaded during init", tenant)
	return lazy
}

// warmupSeed is what a tenant holds on disk before the sweep runs.
type warmupSeed struct {
	// counted objects sit in a segment with a count sidecar, which
	// LazyLoadShard.ObjectCountAsync reads without loading the shard.
	counted int
	// uncounted objects sit in a segment whose sidecar only the next load writes,
	// which is what an ordinary shutdown leaves behind.
	uncounted int
}

// seedWarmupTenants gives each tenant the objects the map asks for and leaves
// every shard cold, so a later index sweeps over what is really on disk.
func seedWarmupTenants(t *testing.T, dirName string, seeds map[string]warmupSeed) []string {
	t.Helper()
	tenants := make([]string, 0, len(seeds))
	for tenant := range seeds {
		tenants = append(tenants, tenant)
	}

	index := newWarmupIndex(t, dirName, -1, tenants...)
	for tenant, seed := range seeds {
		writeCountedObjects(t, coldWarmupShard(t, index, tenant), warmupClassName, seed.counted)
		if seed.uncounted > 0 {
			writeUncountedObjects(t, coldWarmupShard(t, index, tenant), warmupClassName, seed.uncounted)
		}
	}
	require.NoError(t, index.Shutdown(context.Background()))

	return tenants
}

// TestLazyShardBackgroundWarmup pins which shards the startup sweep materializes
// for each range of LazyLoadShardWarmupMinObjects, and that a shard it leaves out
// still loads on demand.
func TestLazyShardBackgroundWarmup(t *testing.T) {
	ctx := context.Background()

	const tenant = "busy-tenant"

	tests := []struct {
		name       string
		seed       warmupSeed
		minObjects int64
		// unreadableCount removes the shard's objects bucket directory, so
		// LazyLoadShard.ObjectCountAsync fails instead of returning a count.
		unreadableCount bool
		wantLoaded      bool
	}{
		{name: "a negative threshold warms nothing", seed: warmupSeed{counted: 3}, minObjects: -1},
		{name: "the default warms a non-empty shard", seed: warmupSeed{counted: 3}, minObjects: 0, wantLoaded: true},
		{name: "the default skips an empty shard", minObjects: 0},
		{name: "a threshold above the object count skips the shard", seed: warmupSeed{counted: 3}, minObjects: 5},
		{name: "a threshold at the object count skips the shard", seed: warmupSeed{counted: 3}, minObjects: 3},
		{
			name: "a threshold below the object count warms the shard",
			seed: warmupSeed{counted: 3}, minObjects: 2, wantLoaded: true,
		},
		{
			name: "an unreadable object count warms the shard", seed: warmupSeed{counted: 3}, minObjects: 5,
			unreadableCount: true, wantLoaded: true,
		},
		// Segments a shutdown flush wrote carry no count sidecar, so the cold count
		// reads zero for them and only the doc-id counter sees the objects.
		{
			name: "a threshold below the unflushed object count warms the shard",
			seed: warmupSeed{uncounted: 5}, minObjects: 3, wantLoaded: true,
		},
		{
			name: "a threshold above the unflushed object count skips the shard",
			seed: warmupSeed{uncounted: 2}, minObjects: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			seedWarmupTenants(t, dirName, map[string]warmupSeed{tenant: tt.seed})
			if tt.unreadableCount {
				indexPath := path.Join(dirName, indexID(schema.ClassName(warmupClassName)))
				require.NoError(t, os.RemoveAll(
					path.Join(shardPathLSM(indexPath, tenant), helpers.ObjectsBucketLSM)))
			}

			index := newWarmupIndex(t, dirName, tt.minObjects, tenant)
			defer index.Shutdown(ctx)

			lazy := coldWarmupShard(t, index, tenant)

			if tt.wantLoaded {
				// The sweep ticks once per second, so allow generous slack.
				require.Eventually(t, lazy.isLoaded, 30*time.Second, 50*time.Millisecond,
					"background warmup should materialize the shard")
				require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond,
					"allShardsReady should be published once the sweep completes")
				return
			}

			if tt.minObjects < 0 {
				require.True(t, index.allShardsReady.Load(),
					"allShardsReady must be published immediately when no sweep runs")
				require.Never(t, lazy.isLoaded, 2*time.Second, 100*time.Millisecond,
					"no sweep should run at all")
			} else {
				require.Eventually(t, index.allShardsReady.Load, 10*time.Second, 50*time.Millisecond,
					"allShardsReady should be published once the sweep has skipped every shard")
				// allShardsReady is published from the sweep goroutine's last deferred
				// call, so it implies the sweep is over and no later load is possible.
				require.False(t, lazy.isLoaded(), "a shard the sweep leaves out must stay cold")
			}

			// Leaving a shard out of the sweep must keep it loadable on demand.
			require.NoError(t, lazy.Load(ctx))
			require.True(t, lazy.isLoaded())
		})
	}
}

// The sweep paces itself at one shard per second, and decides whether to warm a
// shard before spending that second. With a single shard above the threshold
// among many below it, the sweep finishes in about a second — spending the tick
// before the decision would take a second per tenant instead.
func TestLazyShardBackgroundWarmupSkipsSpendNoTick(t *testing.T) {
	ctx := context.Background()

	const (
		smallTenantCount = 7
		bigTenant        = "big-tenant"
		minObjects       = 3
	)

	seeds := map[string]warmupSeed{bigTenant: {counted: minObjects + 1}}
	for i := range smallTenantCount {
		seeds[fmt.Sprintf("small-tenant-%d", i)] = warmupSeed{counted: 1}
	}

	dirName := t.TempDir()
	tenants := seedWarmupTenants(t, dirName, seeds)

	index := newWarmupIndex(t, dirName, minObjects, tenants...)
	defer index.Shutdown(ctx)

	// The green path is one tick, so the budget only separates it from a tick per
	// shard while there are at least three tenants.
	sweepBudget := time.Duration(len(tenants)) * time.Second / 2
	require.Eventually(t, index.allShardsReady.Load, sweepBudget, 50*time.Millisecond,
		"the sweep must not spend its per-shard tick on the shards it skips")

	for tenant := range seeds {
		stored := index.shards.Load(tenant)
		require.NotNil(t, stored)
		lazy, ok := stored.(*LazyLoadShard)
		require.True(t, ok, "lazy mode should store a wrapper, got %T", stored)
		require.Equal(t, tenant == bigTenant, lazy.isLoaded(),
			"only the tenant above the threshold should be warmed, checked %q", tenant)
	}
}
