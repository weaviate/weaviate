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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	warmupClassName = "TestWarmupClass"
	warmupNodeName  = "test-node"
)

// newWarmupIndex opens a multi-tenant index over dirName with one hot lazy shard
// per tenant. A nil allocChecker falls back to the dummy monitor, which allows
// every load. The returned hook captures the index log, the startup sweep included.
func newWarmupIndex(t *testing.T, dirName string,
	allocChecker memwatch.AllocChecker, tenants ...string,
) (*Index, *test.Hook) {
	t.Helper()
	ctx := context.Background()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

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
		RootPath:             dirName,
		ClassName:            schema.ClassName(warmupClassName),
		ReplicationFactor:    1,
		ShardLoadLimiter:     loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		EnableLazyLoadShards: true,
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
		mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{},
		monitoring.GetMetrics(),
		class, nil, scheduler, nil, allocChecker,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)

	return index, hook
}

// seedWarmupTenants writes one object per tenant and shuts the index down again,
// leaving the shards on disk. The sweep skips a tenant shard that was never
// written to, so a shard has to hold something to be a load candidate.
func seedWarmupTenants(t *testing.T, dirName string, tenants ...string) {
	t.Helper()
	ctx := context.Background()

	index, _ := newWarmupIndex(t, dirName, nil, tenants...)
	for _, tenant := range tenants {
		stored := index.shards.Load(tenant)
		require.NotNil(t, stored, "shard of tenant %q must be registered", tenant)
		require.NoError(t, stored.PutObject(ctx, coldTestObject(warmupClassName)))
	}
	require.NoError(t, index.Shutdown(ctx))
}

// refusingAllocChecker refuses the load attempts listed in refuse, counted from
// one, and allows the rest. LazyLoadShard.Load asks it before it builds the shard,
// so this fails a load without having to damage anything on disk.
type refusingAllocChecker struct {
	mu     sync.Mutex
	refuse map[int]bool
	calls  int
}

func newRefusingAllocChecker(refuse []int) *refusingAllocChecker {
	refused := make(map[int]bool, len(refuse))
	for _, call := range refuse {
		refused[call] = true
	}
	return &refusingAllocChecker{refuse: refused}
}

func (c *refusingAllocChecker) CheckAlloc(int64) error { return nil }

func (c *refusingAllocChecker) CheckMappingAndReserve(int64, int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++
	if c.refuse[c.calls] {
		return fmt.Errorf("no mappings left for load attempt %d", c.calls)
	}
	return nil
}

func (c *refusingAllocChecker) Refresh(bool) {}

// attempts reports how many shards the sweep tried to load.
func (c *refusingAllocChecker) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// requireSweepFailures waits for the sweep to log the end of its walk and asserts
// how many loads it counted as failed.
func requireSweepFailures(t *testing.T, hook *test.Hook, want int) {
	t.Helper()

	var failed any
	require.Eventually(t, func() bool {
		for _, entry := range hook.AllEntries() {
			if entry.Message != "finished loading all shards" {
				continue
			}
			failed = entry.Data["failed"]
			return true
		}
		return false
	}, 30*time.Second, 50*time.Millisecond,
		"the sweep should report the end of its walk, whatever a load did")

	require.Equal(t, want, failed)
}

// TestLazyShardBackgroundWarmupContinuesAfterFailedLoad pins that a shard failing
// to load does not end the startup sweep: every shard behind it is still attempted
// and loaded, and the sweep still reports how many loads failed.
func TestLazyShardBackgroundWarmupContinuesAfterFailedLoad(t *testing.T) {
	ctx := context.Background()

	const tenantCount = 3

	tests := []struct {
		name string
		// refuse names the load attempts the memory guard fails, counted from one.
		// The sweep walks the shards in map order, so a case can pin which attempt
		// fails, never which tenant.
		refuse     []int
		wantLoaded int
	}{
		{name: "a failure on the first shard leaves the rest to load", refuse: []int{1}, wantLoaded: 2},
		{name: "a failure in the middle leaves the rest to load", refuse: []int{2}, wantLoaded: 2},
		{name: "a failure on the last shard leaves the ones before it loaded", refuse: []int{3}, wantLoaded: 2},
		{name: "every shard failing still leaves every shard attempted", refuse: []int{1, 2, 3}},
		{name: "no failure loads every shard", wantLoaded: tenantCount},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenants := make([]string, tenantCount)
			for i := range tenants {
				tenants[i] = fmt.Sprintf("tenant-%d", i)
			}

			dirName := t.TempDir()
			seedWarmupTenants(t, dirName, tenants...)

			checker := newRefusingAllocChecker(tt.refuse)
			index, hook := newWarmupIndex(t, dirName, checker, tenants...)
			defer index.Shutdown(ctx)

			// The sweep goroutine sets this in a deferred call, so it flips however
			// the sweep ended, an early return included.
			require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond,
				"allShardsReady should be published once the sweep is over")

			loaded := 0
			for _, tenant := range tenants {
				stored := index.shards.Load(tenant)
				require.NotNil(t, stored, "shard of tenant %q must be registered", tenant)
				lazy, ok := stored.(*LazyLoadShard)
				require.True(t, ok, "lazy mode should store a wrapper, got %T", stored)
				if lazy.isLoaded() {
					loaded++
				}
			}

			require.Equal(t, tenantCount, checker.attempts(),
				"the sweep must attempt every shard, whatever the ones before it did")
			require.Equal(t, tt.wantLoaded, loaded,
				"every shard the guard allowed must end up loaded")
			requireSweepFailures(t, hook, len(tt.refuse))
		})
	}
}
