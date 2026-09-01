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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	warmupClassName = "TestWarmupClass"
	warmupNodeName  = "test-node"
)

// newWarmupIndex opens a multi-tenant index over dirName, one lazy shard per
// tenant, with the startup sweep gated at minObjects. A nil allocChecker allows
// every load. The returned hook holds the sweep's log of what it did per shard.
func newWarmupIndex(t *testing.T, dirName string, minObjects int64,
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
		RootPath:                      dirName,
		ClassName:                     schema.ClassName(warmupClassName),
		ReplicationFactor:             1,
		ShardLoadLimiter:              loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		EnableLazyLoadShards:          true,
		LazyLoadShardWarmupMinObjects: minObjects,
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
		mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{},
		monitoring.GetMetrics(),
		class, nil, scheduler, nil, allocChecker,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)

	return index, hook
}

// sweepDoneMessage is what the startup sweep logs once it has walked every shard.
const sweepDoneMessage = "finished loading all shards"

// requireSweepTally asserts how many shards the startup sweep reported under
// each outcome, waiting for it to finish. An outcome absent from want must have
// counted nothing.
func requireSweepTally(t *testing.T, hook *test.Hook, want map[monitoring.WarmupOutcome]int) {
	t.Helper()

	var tally logrus.Fields
	require.Eventually(t, func() bool {
		for _, entry := range hook.AllEntries() {
			if entry.Message != sweepDoneMessage {
				continue
			}
			tally = entry.Data
			return true
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "the sweep should log what it did with every shard")

	for _, outcome := range []monitoring.WarmupOutcome{
		monitoring.WarmupLoaded,
		monitoring.WarmupFailed,
		monitoring.WarmupSkippedShardGone,
		monitoring.WarmupSkippedAlreadyLoaded,
		monitoring.WarmupSkippedEmpty,
		monitoring.WarmupSkippedBelowThreshold,
	} {
		require.Equal(t, want[outcome], tally[string(outcome)], "shards reported as %q", outcome)
	}
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

	index, _ := newWarmupIndex(t, dirName, -1, nil, tenants...)
	for tenant, seed := range seeds {
		writeCountedObjects(t, coldWarmupShard(t, index, tenant), warmupClassName, seed.counted)
		if seed.uncounted > 0 {
			writeUncountedObjects(t, coldWarmupShard(t, index, tenant), warmupClassName, seed.uncounted)
		}
	}
	require.NoError(t, index.Shutdown(context.Background()))

	return tenants
}

// TestLazyShardBackgroundWarmup pins which shards the startup sweep loads for each
// range of LazyLoadShardWarmupMinObjects, and that a shard it leaves out still
// loads on demand.
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
		// wantOutcome is what the sweep should report for the shard. It is empty
		// where a negative threshold means no sweep runs.
		wantOutcome monitoring.WarmupOutcome
	}{
		{name: "a negative threshold warms nothing", seed: warmupSeed{counted: 3}, minObjects: -1},
		{
			name: "the default warms a non-empty shard",
			seed: warmupSeed{counted: 3}, minObjects: 0, wantOutcome: monitoring.WarmupLoaded,
		},
		{
			name:       "the default skips an empty shard",
			minObjects: 0, wantOutcome: monitoring.WarmupSkippedEmpty,
		},
		{
			name: "a threshold above the object count skips the shard",
			seed: warmupSeed{counted: 3}, minObjects: 5,
			wantOutcome: monitoring.WarmupSkippedBelowThreshold,
		},
		{
			name: "a threshold at the object count skips the shard",
			seed: warmupSeed{counted: 3}, minObjects: 3,
			wantOutcome: monitoring.WarmupSkippedBelowThreshold,
		},
		{
			name: "a threshold below the object count warms the shard",
			seed: warmupSeed{counted: 3}, minObjects: 2, wantOutcome: monitoring.WarmupLoaded,
		},
		{
			name: "an unreadable object count warms the shard", seed: warmupSeed{counted: 3}, minObjects: 5,
			unreadableCount: true, wantOutcome: monitoring.WarmupLoaded,
		},
		// Segments a shutdown flush wrote carry no count sidecar, and a shard left
		// cold never gets one derived, so the sweep weighs a shard by what was
		// flushed while it last ran and nothing else.
		{
			name: "unflushed objects do not count towards the threshold",
			seed: warmupSeed{uncounted: 5}, minObjects: 3,
			wantOutcome: monitoring.WarmupSkippedBelowThreshold,
		},
		{
			name: "a threshold above the flushed count alone skips the shard",
			seed: warmupSeed{counted: 2, uncounted: 4}, minObjects: 5,
			wantOutcome: monitoring.WarmupSkippedBelowThreshold,
		},
		{
			name: "a threshold below the flushed count alone warms the shard",
			seed: warmupSeed{counted: 6, uncounted: 4}, minObjects: 5, wantOutcome: monitoring.WarmupLoaded,
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

			index, hook := newWarmupIndex(t, dirName, tt.minObjects, nil, tenant)
			defer index.Shutdown(ctx)

			lazy := coldWarmupShard(t, index, tenant)

			if tt.wantOutcome == monitoring.WarmupLoaded {
				// The sweep ticks once per second, so allow generous slack.
				require.Eventually(t, lazy.isLoaded, 30*time.Second, 50*time.Millisecond,
					"background warmup should load the shard")
				require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond,
					"allShardsReady should be published once the sweep completes")
				requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{tt.wantOutcome: 1})
				return
			}

			if tt.minObjects < 0 {
				require.True(t, index.allShardsReady.Load(),
					"allShardsReady must be published immediately when no sweep runs")
				require.Never(t, lazy.isLoaded, 2*time.Second, 100*time.Millisecond,
					"no sweep should run at all")
				for _, entry := range hook.AllEntries() {
					require.NotEqual(t, sweepDoneMessage, entry.Message,
						"a sweep that never runs must report no tally")
				}
			} else {
				require.Eventually(t, index.allShardsReady.Load, 10*time.Second, 50*time.Millisecond,
					"allShardsReady should be published once the sweep has skipped every shard")
				// allShardsReady is published from the sweep goroutine's last deferred
				// call, so it implies the sweep is over and no later load is possible.
				require.False(t, lazy.isLoaded(), "a shard the sweep leaves out must stay cold")
				requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{tt.wantOutcome: 1})
			}

			// Leaving a shard out of the sweep must keep it loadable on demand.
			require.NoError(t, lazy.Load(ctx))
			require.True(t, lazy.isLoaded())
		})
	}
}

// The sweep paces itself at one shard per second and decides before spending it.
// With one shard above the threshold among many below, the sweep finishes in about
// a second. Spending the tick first would take a second per tenant.
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

	index, _ := newWarmupIndex(t, dirName, minObjects, nil, tenants...)
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

// TestLazyShardBackgroundWarmupContinuesAfterFailedLoad pins that a shard failing
// to load does not end the startup sweep: every shard behind it is still attempted
// and loaded, and the sweep still reports how many loads failed.
func TestLazyShardBackgroundWarmupContinuesAfterFailedLoad(t *testing.T) {
	ctx := context.Background()

	// Seeded either side of the threshold the skip case uses, so warm tenants are
	// swept and cold ones are passed over before a load is ever attempted.
	const (
		warmObjects = 5
		coldObjects = 1
		skipAtFour  = 4
	)

	tests := []struct {
		name string
		// warm tenants sit above the threshold and are load candidates. Cold ones
		// sit below it, and the sweep skips them without attempting a load.
		warm       int
		cold       int
		minObjects int64
		// refuse names the load attempts the memory guard fails, counted from one.
		// The sweep walks the shards in map order, so a case can pin which attempt
		// fails, never which tenant.
		refuse       []int
		wantLoaded   int
		wantAttempts int
	}{
		{
			name: "a failure on the first shard leaves the rest to load",
			warm: 3, refuse: []int{1}, wantLoaded: 2, wantAttempts: 3,
		},
		{
			name: "a failure in the middle leaves the rest to load",
			warm: 3, refuse: []int{2}, wantLoaded: 2, wantAttempts: 3,
		},
		{
			name: "every shard failing still leaves every shard attempted",
			warm: 3, refuse: []int{1, 2, 3}, wantLoaded: 0, wantAttempts: 3,
		},
		{
			name: "no failure loads every shard",
			warm: 3, wantLoaded: 3, wantAttempts: 3,
		},
		{
			name: "a failure leaves the shards below the threshold skipped, not attempted",
			warm: 2, cold: 1, minObjects: skipAtFour, refuse: []int{1},
			wantLoaded: 1, wantAttempts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seeds := map[string]warmupSeed{}
			for i := range tt.warm {
				seeds[fmt.Sprintf("warm-tenant-%d", i)] = warmupSeed{counted: warmObjects}
			}
			for i := range tt.cold {
				seeds[fmt.Sprintf("cold-tenant-%d", i)] = warmupSeed{counted: coldObjects}
			}

			dirName := t.TempDir()
			tenants := seedWarmupTenants(t, dirName, seeds)

			checker := newRefusingAllocChecker(tt.refuse)
			index, hook := newWarmupIndex(t, dirName, tt.minObjects, checker, tenants...)
			defer index.Shutdown(ctx)

			// Published from the sweep goroutine's last deferred call, so it means
			// the sweep is over however it ended — including the abandoning return
			// this test exists to catch.
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

			require.Equal(t, tt.wantAttempts, checker.attempts(),
				"the sweep must attempt every candidate shard, whatever the ones before it did")
			require.Equal(t, tt.wantLoaded, loaded,
				"every candidate the guard allowed must end up loaded")
			requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{
				monitoring.WarmupLoaded:                tt.wantLoaded,
				monitoring.WarmupFailed:                len(tt.refuse),
				monitoring.WarmupSkippedBelowThreshold: tt.cold,
			})
		})
	}
}

// TestLazyShardWarmupSkipsShardWithNothingToWarm pins what the sweep reports for a
// shard it listed but found nothing to warm, telling a shard a request took apart
// from a tenant that is gone. A negative threshold keeps a sweep from racing it.
func TestLazyShardWarmupSkipsShardWithNothingToWarm(t *testing.T) {
	ctx := context.Background()

	const tenant = "busy-tenant"

	tests := []struct {
		name string
		// asked is the shard name the sweep would ask about.
		asked string
		// loadFirst loads the tenant's shard before the decision is read.
		loadFirst   bool
		wantOutcome monitoring.WarmupOutcome
	}{
		{
			name: "a shard a request loaded first", asked: tenant, loadFirst: true,
			wantOutcome: monitoring.WarmupSkippedAlreadyLoaded,
		},
		{
			name: "a name the index no longer holds", asked: "vanished-tenant",
			wantOutcome: monitoring.WarmupSkippedShardGone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			seedWarmupTenants(t, dirName, map[string]warmupSeed{tenant: {counted: 3}})

			index, _ := newWarmupIndex(t, dirName, -1, nil, tenant)
			defer index.Shutdown(ctx)

			if tt.loadFirst {
				require.NoError(t, coldWarmupShard(t, index, tenant).Load(ctx))
			}

			shouldWarm, outcome := index.warmupCandidate(tt.asked)

			require.False(t, shouldWarm, "a shard with nothing left to warm is no candidate")
			require.Equal(t, tt.wantOutcome, outcome)
		})
	}
}

// TestLazyShardWarmupLoadOutcome pins what a load reports back to the startup
// sweep, so a load it performed counts apart from one that found nothing to do.
// A negative threshold keeps a sweep from racing it.
func TestLazyShardWarmupLoadOutcome(t *testing.T) {
	ctx := context.Background()

	const tenant = "busy-tenant"

	tests := []struct {
		name string
		// objects is what the tenant holds on disk before the load is asked for.
		objects int
		// asked is the shard name the sweep would load.
		asked string
		// loadFirst loads the tenant's shard first, standing in for a request
		// that arrived while the sweep was waiting for its tick.
		loadFirst   bool
		wantOutcome monitoring.WarmupOutcome
		wantLoaded  bool
	}{
		{
			name:    "a cold shard is loaded and counted as loaded",
			objects: 3, asked: tenant,
			wantOutcome: monitoring.WarmupLoaded, wantLoaded: true,
		},
		{
			name:    "a shard a request took during the tick counts as no load",
			objects: 3, asked: tenant, loadFirst: true,
			wantOutcome: monitoring.WarmupSkippedAlreadyLoaded, wantLoaded: true,
		},
		{
			name:    "a name the index no longer holds counts as no load",
			objects: 3, asked: "vanished-tenant",
			wantOutcome: monitoring.WarmupSkippedShardGone,
		},
		{
			name:    "a shard that never held an object counts as empty",
			objects: 0, asked: tenant,
			wantOutcome: monitoring.WarmupSkippedEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			seedWarmupTenants(t, dirName, map[string]warmupSeed{tenant: {counted: tt.objects}})

			index, _ := newWarmupIndex(t, dirName, -1, nil, tenant)
			defer index.Shutdown(ctx)

			if tt.loadFirst {
				require.NoError(t, coldWarmupShard(t, index, tenant).Load(ctx))
			}

			outcome, err := index.loadLocalShardIfActive(tt.asked)

			require.NoError(t, err)
			require.Equal(t, tt.wantOutcome, outcome)

			stored := index.shards.Load(tenant)
			require.NotNil(t, stored, "the tenant's shard must stay registered")
			lazy, ok := stored.(*LazyLoadShard)
			require.True(t, ok, "lazy mode should store a wrapper, got %T", stored)
			require.Equal(t, tt.wantLoaded, lazy.isLoaded(),
				"only a shard the load reported as loaded may end up loaded")
		})
	}
}
