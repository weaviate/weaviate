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
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// The gauges below are process-global in production but per-instance here
// (NoopRegisterer skips the AlreadyRegistered dedup), so each test gets a clean
// slate as long as it reads them through the helpers in this file.

// statusMetricsHarness bundles a repo and the two shard gauges under test.
type statusMetricsHarness struct {
	repo     *DB
	migrator *Migrator
	base     *monitoring.PrometheusMetrics
	// shardsCount is captured up front: it lives on the Index, which is gone by
	// the time a drop test wants to assert on it.
	shardsCount *prometheus.GaugeVec
}

// status reads the bucket a status label currently holds. Reading through
// WithLabelValues materializes the series at 0, which is exactly the assertion
// "this bucket holds nothing".
func (h *statusMetricsHarness) status(t *testing.T, status storagestate.Status) float64 {
	t.Helper()
	require.NotNil(t, h.shardsCount, "index metrics were never captured")
	return testutil.ToFloat64(h.shardsCount.WithLabelValues(status.String()))
}

func (h *statusMetricsHarness) loaded(t *testing.T) float64 {
	t.Helper()
	return testutil.ToFloat64(h.base.ShardsLoaded)
}

func (h *statusMetricsHarness) unloaded(t *testing.T) float64 {
	t.Helper()
	return testutil.ToFloat64(h.base.ShardsUnloaded)
}

// requireNoLiveShards asserts that nothing is counted anywhere: every status
// bucket empty and both lifecycle gauges at zero. Buckets are checked for
// exactly 0 rather than "not positive" on purpose — a double release drives a
// gauge negative, which is just as wrong as a leak and much easier to miss.
func (h *statusMetricsHarness) requireNoLiveShards(t *testing.T, msg string) {
	t.Helper()
	for _, s := range []storagestate.Status{
		storagestate.StatusReady,
		storagestate.StatusLoading,
		storagestate.StatusReadOnly,
		storagestate.StatusIndexing,
		storagestate.StatusShutdown,
	} {
		require.Equal(t, float64(0), h.status(t, s),
			"%s: status bucket %q should hold no shards", msg, s)
	}
	require.Equal(t, float64(0), h.loaded(t), "%s: shards_loaded should be 0", msg)
	require.Equal(t, float64(0), h.unloaded(t), "%s: shards_unloaded should be 0", msg)
}

// tenantState returns a partitioned single-tenant state, so the migrator's
// tenant-activation paths are exercised rather than the plain sharding ones.
func tenantState(tenant string) *sharding.State {
	s := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenant: {
				Name:           tenant,
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}
	s.SetLocalName("node1")
	return s
}

func newStatusMetricsHarness(t *testing.T, lazyLoad bool) *statusMetricsHarness {
	t.Helper()
	return newStatusMetricsHarnessWithState(t, lazyLoad, singleShardState())
}

func newStatusMetricsHarnessWithState(t *testing.T, lazyLoad bool, shardState *sharding.State) *statusMetricsHarness {
	t.Helper()

	logger, _ := test.NewNullLogger()

	baseMetrics := monitoring.GetMetrics()
	metricsCopy := *baseMetrics
	metricsCopy.Registerer = monitoring.NoopRegisterer
	metrics := &metricsCopy

	metrics.ShardsLoaded.Set(0)
	metrics.ShardsLoading.Set(0)
	metrics.ShardsUnloaded.Set(0)
	metrics.ShardsUnloading.Set(0)

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	// Consulted only for multi-tenant classes, when the migrator picks eager vs
	// lazy shard loading.
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).
		Return(len(shardState.Physical), nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(lazyLoad),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	return &statusMetricsHarness{
		repo:     repo,
		migrator: NewMigrator(repo, logger, "node1"),
		base:     metrics,
	}
}

// addClass creates the collection and captures the per-status gauge off its index.
func (h *statusMetricsHarness) addClass(t *testing.T, className string) *Index {
	t.Helper()
	return h.addClassWith(t, &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	})
}

func (h *statusMetricsHarness) addClassWith(t *testing.T, class *models.Class) *Index {
	t.Helper()

	className := class.Class
	require.NoError(t, h.migrator.AddClass(context.Background(), class))

	idx := h.repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	h.shardsCount = idx.metrics.shardsCount
	require.NotNil(t, h.shardsCount)

	return idx
}

func onlyShardName(t *testing.T, idx *Index) string {
	t.Helper()

	var name string
	require.NoError(t, idx.ForEachShard(func(shardName string, _ ShardLike) error {
		name = shardName
		return nil
	}))
	require.NotEmpty(t, name)

	return name
}

// TestShardStatusMetricReleasedOnDrop pins the reported bug: dropping a shard
// decremented shards_loaded but left its per-status bucket behind, so a
// collection that was created and deleted kept inflating READY until restart.
func TestShardStatusMetricReleasedOnDrop(t *testing.T) {
	ctx := testCtx()
	h := newStatusMetricsHarness(t, false)

	className := "StatusMetricDrop"
	h.addClass(t, className)

	require.Equal(t, float64(1), h.status(t, storagestate.StatusReady),
		"a freshly created shard should be counted as READY")
	require.Equal(t, float64(1), h.loaded(t))

	require.NoError(t, h.migrator.DropClass(ctx, className, false))

	h.requireNoLiveShards(t, "after dropping the collection")
}

// TestShardStatusMetricReleasedRepeatedly is the drift the dashboards showed:
// the leak was one shard per create/drop cycle, so a single round would not have
// caught it drifting.
func TestShardStatusMetricReleasedRepeatedly(t *testing.T) {
	ctx := testCtx()
	h := newStatusMetricsHarness(t, false)

	for i := 0; i < 3; i++ {
		className := "StatusMetricCycle"
		h.addClass(t, className)
		require.Equal(t, float64(1), h.status(t, storagestate.StatusReady),
			"round %d: exactly one shard should be counted while the collection exists", i)

		require.NoError(t, h.migrator.DropClass(ctx, className, false))
		h.requireNoLiveShards(t, "after round "+string(rune('0'+i)))
	}
}

// TestShardMetricsNotDoubleReleasedOnShutdownThenDrop covers the other half:
// shutdown already moved the shard from loaded to unloaded, so the drop that
// follows must release the unloaded bucket. Releasing loaded a second time drove
// shards_loaded negative.
func TestShardMetricsNotDoubleReleasedOnShutdownThenDrop(t *testing.T) {
	ctx := testCtx()
	h := newStatusMetricsHarness(t, false)

	className := "StatusMetricShutdownDrop"
	idx := h.addClass(t, className)
	shardName := onlyShardName(t, idx)

	shard := idx.shards.Load(shardName)
	require.NotNil(t, shard)
	concrete, ok := shard.(*Shard)
	require.True(t, ok, "lazy loading is off, so the map should hold a concrete shard")

	require.NoError(t, concrete.Shutdown(ctx))

	require.Equal(t, float64(0), h.loaded(t), "shutdown moves the shard out of loaded")
	require.Equal(t, float64(1), h.unloaded(t), "shutdown moves the shard into unloaded")
	require.Equal(t, float64(0), h.status(t, storagestate.StatusReady),
		"a shut-down shard should not be counted as READY")
	require.Equal(t, float64(0), h.status(t, storagestate.StatusShutdown),
		"the SHUTDOWN bucket must not strand shards that left the shard map")

	require.NoError(t, h.migrator.DropClass(ctx, className, false))

	h.requireNoLiveShards(t, "after shutdown followed by drop")
}

// TestShardLifecycleMetricReleasedOnUnloadLocalShard covers tenant deactivation:
// UnloadLocalShard evicts the shard from the map, so its unloaded count has to
// go with it. A never-loaded lazy shard is the sharper case — its Shutdown is a
// no-op, so nothing else would ever release the count NewLazyLoadShard took.
func TestShardLifecycleMetricReleasedOnUnloadLocalShard(t *testing.T) {
	ctx := testCtx()

	for _, tc := range []struct {
		name string
		load bool
	}{
		{name: "never loaded", load: false},
		{name: "loaded first", load: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStatusMetricsHarness(t, true)

			className := "StatusMetricUnload"
			idx := h.addClass(t, className)
			shardName := onlyShardName(t, idx)

			require.Equal(t, float64(1), h.unloaded(t),
				"a lazy shard is registered as unloaded on creation")

			if tc.load {
				shard := idx.shards.Load(shardName)
				require.NotNil(t, shard)
				lazy, ok := shard.(*LazyLoadShard)
				require.True(t, ok)
				require.NoError(t, lazy.Load(ctx))

				require.Equal(t, float64(1), h.loaded(t))
				require.Equal(t, float64(0), h.unloaded(t))
			}

			require.NoError(t, idx.UnloadLocalShard(ctx, shardName))

			h.requireNoLiveShards(t, "after unloading the shard")
		})
	}
}

// TestShardLifecycleMetricReleasedOnTenantDeactivation drives the real
// deactivation path. Migrator.UpdateTenants does its own LoadAndDelete rather
// than going through UnloadLocalShard, which is why the release belongs in
// shutdownOrRestoreShard where every eviction site converges: a fix applied per
// caller would have missed this one, and this is the path that actually runs on
// a multi-tenant cluster.
func TestShardLifecycleMetricReleasedOnTenantDeactivation(t *testing.T) {
	ctx := testCtx()

	const tenant = "tenant-0"

	class := &models.Class{
		Class:               "StatusMetricDeactivate",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}
	for _, tc := range []struct {
		name string
		load bool
	}{
		// The never-loaded case is the sharper one: LazyLoadShard.Shutdown is a
		// no-op while !loaded, so nothing but this release ever gives back the
		// count NewLazyLoadShard took.
		{name: "never loaded", load: false},
		{name: "loaded first", load: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStatusMetricsHarnessWithState(t, false, tenantState(tenant))
			idx := h.addClassWith(t, class)

			// A multi-tenant class registers its tenant shards lazily, so the
			// shard starts out counted as unloaded rather than READY.
			require.Equal(t, float64(1), h.unloaded(t),
				"the tenant's shard should be registered as unloaded on creation")

			if tc.load {
				lazy, ok := idx.shards.Load(tenant).(*LazyLoadShard)
				require.True(t, ok)
				require.NoError(t, lazy.Load(ctx))
				require.Equal(t, float64(1), h.loaded(t))
				require.Equal(t, float64(1), h.status(t, storagestate.StatusReady))
			}

			require.NoError(t, h.migrator.UpdateTenants(ctx, class,
				[]*schemaUC.UpdateTenantPayload{{Name: tenant, Status: models.TenantActivityStatusCOLD}}, false))

			require.Nil(t, idx.shards.Load(tenant),
				"deactivation should have evicted the shard from the shard map")
			h.requireNoLiveShards(t, "after deactivating the tenant")
		})
	}
}

// TestShardLifecycleMetricReleasedOnFailedLazyDrop covers the unloaded drop path
// failing part way. The caller has already removed the wrapper from the shard
// map by then, so releasing only on the success tail would strand the count for
// the life of the process — the same reason Shard.drop releases from a defer.
func TestShardLifecycleMetricReleasedOnFailedLazyDrop(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root, which ignores the directory permissions this test relies on")
	}

	const tenant = "tenant-0"
	h := newStatusMetricsHarnessWithState(t, false, tenantState(tenant))

	idx := h.addClassWith(t, &models.Class{
		Class:               "StatusMetricFailedDrop",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	})

	lazy, ok := idx.shards.Load(tenant).(*LazyLoadShard)
	require.True(t, ok, "a multi-tenant class registers its tenant shards lazily")
	require.Equal(t, float64(1), h.unloaded(t))

	// A never-loaded shard has no directory yet, and the drop skips the rename
	// entirely when there is nothing to rename. Create it so the drop has real
	// work to fail at.
	indexPath := idx.path()
	require.NoError(t, os.MkdirAll(shardPath(indexPath, tenant), 0o755))

	// Make the index directory read-only so the drop fails somewhere in the
	// middle — which step exactly does not matter, only that it returns early.
	info, err := os.Stat(indexPath)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(indexPath, 0o555))
	t.Cleanup(func() { _ = os.Chmod(indexPath, info.Mode()) })

	require.Error(t, lazy.drop(false), "the drop should fail with the index directory read-only")

	require.Equal(t, float64(0), h.unloaded(t),
		"a shard whose drop failed is still gone from the shard map and must not stay counted")
}

// TestShardStatusMetricFollowsCountedLabel guards the subtler desync: GetStatus
// recomputes READY/INDEXING and writes it back to the shard without touching the
// gauge, so the shard's own status drifts from the bucket it is counted in.
// Trusting the drifted status on the next transition decremented a bucket the
// shard was never counted in, driving it negative and stranding the real one.
func TestShardStatusMetricFollowsCountedLabel(t *testing.T) {
	ctx := testCtx()
	h := newStatusMetricsHarness(t, false)

	className := "StatusMetricDrift"
	idx := h.addClass(t, className)
	shardName := onlyShardName(t, idx)

	shard := idx.shards.Load(shardName)
	require.NotNil(t, shard)
	concrete, ok := shard.(*Shard)
	require.True(t, ok)

	require.Equal(t, float64(1), h.status(t, storagestate.StatusReady))

	// Reproduce GetStatus's write-back without going through the vector queues,
	// which cannot be driven deterministically from here.
	concrete.statusLock.Lock()
	concrete.status.Status = storagestate.StatusIndexing
	concrete.statusLock.Unlock()

	require.NoError(t, concrete.UpdateStatus(storagestate.StatusReadOnly.String(), "test"))

	require.Equal(t, float64(1), h.status(t, storagestate.StatusReadOnly),
		"the shard should now be counted as READONLY")
	require.Equal(t, float64(0), h.status(t, storagestate.StatusReady),
		"the bucket the shard was actually counted in should have been released")
	require.Equal(t, float64(0), h.status(t, storagestate.StatusIndexing),
		"INDEXING was never incremented, so it must not be decremented")

	require.NoError(t, h.migrator.DropClass(ctx, className, false))
	h.requireNoLiveShards(t, "after dropping a shard that drifted status")
}
