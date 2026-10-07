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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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

func statusGaugeTenant(i int) string { return fmt.Sprintf("tenant-%d", i) }

var allShardStatuses = []storagestate.Status{
	storagestate.StatusReady,
	storagestate.StatusLoading,
	storagestate.StatusReadOnly,
	storagestate.StatusIndexing,
	storagestate.StatusShutdown,
}

type statusGaugeHarness struct {
	repo         *DB
	migrator     *Migrator
	schemaGetter *fakeSchemaGetter
	// shardsCount is captured here because the Index is gone by the time a drop
	// test reads it.
	shardsCount *prometheus.GaugeVec
}

type statusGaugeOpts struct {
	multiTenant bool
	tenants     int
	asyncIndex  bool
}

func newStatusGaugeHarness(t *testing.T, opts statusGaugeOpts) *statusGaugeHarness {
	t.Helper()

	logger, _ := test.NewNullLogger()

	metricsCopy := *monitoring.GetMetrics()
	// One registry for every index, as in production, so the gauge is shared.
	metricsCopy.Registerer = prometheus.NewRegistry()

	shardState := singleShardState()
	if opts.multiTenant {
		shardState = &sharding.State{
			Physical:            map[string]sharding.Physical{},
			PartitioningEnabled: true,
		}
		for i := 0; i < max(opts.tenants, 1); i++ {
			name := statusGaugeTenant(i)
			shardState.Physical[name] = sharding.Physical{
				Name:           name,
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			}
		}
		shardState.SetLocalName("node1")
	}

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		},
	).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(len(shardState.Physical), nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(
		logger, "node1", Config{
			RootPath:                  t.TempDir(),
			QueryMaximumResults:       10000,
			MaxImportGoroutinesFactor: 1,
			EnableLazyLoadShards:      boolPtr(false),
			AsyncIndexingEnabled:      opts.asyncIndex,
		},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, &metricsCopy, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	return &statusGaugeHarness{
		repo:         repo,
		migrator:     NewMigrator(repo, logger, "node1"),
		schemaGetter: schemaGetter,
	}
}

func (h *statusGaugeHarness) addClass(t *testing.T, class *models.Class) *Index {
	t.Helper()

	require.NoError(t, h.migrator.AddClass(context.Background(), class))
	h.schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	idx := h.repo.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)
	require.NotNil(t, idx.metrics.shardsCount)
	h.shardsCount = idx.metrics.shardsCount

	return idx
}

func statusGaugeClass(name string, multiTenant bool) *models.Class {
	return &models.Class{
		Class:               name,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: multiTenant},
	}
}

// requireBuckets asserts every bucket, absent ones at exactly 0 so a double
// release (negative) is caught.
func (h *statusGaugeHarness) requireBuckets(t *testing.T, msg string, want map[storagestate.Status]float64) {
	t.Helper()

	for _, status := range allShardStatuses {
		require.Equal(t, want[status], testutil.ToFloat64(h.shardsCount.WithLabelValues(status.String())),
			"%s: bucket %s", msg, status)
	}
}

func onlyShard(t *testing.T, idx *Index) (string, ShardLike) {
	t.Helper()

	var (
		name  string
		shard ShardLike
	)
	require.NoError(t, idx.ForEachShard(func(n string, s ShardLike) error {
		name, shard = n, s
		return nil
	}))
	require.NotNil(t, shard)

	return name, shard
}

func TestShardStatusGaugeReleasedWhenShardLeavesNode(t *testing.T) {
	ctx := testCtx()

	type env struct {
		h         *statusGaugeHarness
		idx       *Index
		className string
		shardName string
		shard     ShardLike
	}

	tests := []struct {
		name        string
		multiTenant bool
		skipAsRoot  bool
		counted     map[storagestate.Status]float64
		prepare     func(t *testing.T, e env)
		act         func(t *testing.T, e env)
	}{
		{
			name:    "collection dropped",
			counted: map[storagestate.Status]float64{storagestate.StatusReady: 1},
			act: func(t *testing.T, e env) {
				require.NoError(t, e.h.migrator.DropClass(ctx, e.className, false))
			},
		},
		{
			name:    "shut down then collection dropped",
			counted: nil,
			prepare: func(t *testing.T, e env) {
				require.NoError(t, e.shard.Shutdown(ctx))
			},
			act: func(t *testing.T, e env) {
				// Drop may error on a closed store; the gauge must be released regardless.
				_ = e.h.migrator.DropClass(ctx, e.className, false)
			},
		},
		{
			name:    "evicted with UnloadLocalShard",
			counted: map[storagestate.Status]float64{storagestate.StatusReady: 1},
			act: func(t *testing.T, e env) {
				require.NoError(t, e.idx.UnloadLocalShard(ctx, e.shardName))
			},
		},
		{
			name:       "drop fails part way",
			counted:    map[storagestate.Status]float64{storagestate.StatusReady: 1},
			skipAsRoot: true,
			prepare: func(t *testing.T, e env) {
				// Read-only shard dir makes drop return early while still counted READY.
				dir := e.shard.(*Shard).path()
				require.NoError(t, os.Chmod(dir, 0o555))
				t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
			},
			act: func(t *testing.T, e env) {
				shard, ok := e.idx.shards.LoadAndDelete(e.shardName)
				require.True(t, ok)
				require.Error(t, shard.drop(false))
			},
		},
		{
			name:    "status read after drop",
			counted: map[storagestate.Status]float64{storagestate.StatusReady: 1},
			act: func(t *testing.T, e env) {
				shard, ok := e.idx.shards.LoadAndDelete(e.shardName)
				require.True(t, ok)
				require.NoError(t, shard.drop(false))
				// A dropped shard still reports READY; reading it must not recount it.
				require.Equal(t, storagestate.StatusReady, shard.GetStatus())
			},
		},
		{
			name:    "status update fails then collection dropped",
			counted: map[storagestate.Status]float64{storagestate.StatusReady: 1},
			prepare: func(t *testing.T, e env) {
				// The failed update changes the shard's status but not the gauge;
				// the release must still take the shard out of READY.
				require.NoError(t, e.shard.(*Shard).store.Shutdown(ctx))
				require.Error(t, e.shard.UpdateStatus(storagestate.StatusReadOnly.String(), "closed store"))
			},
			act: func(t *testing.T, e env) {
				_ = e.h.migrator.DropClass(ctx, e.className, false)
			},
		},
		{
			name:        "loaded tenant offloaded as frozen",
			multiTenant: true,
			counted:     map[storagestate.Status]float64{storagestate.StatusReady: 1},
			prepare: func(t *testing.T, e env) {
				require.NoError(t, e.shard.(*LazyLoadShard).Load(ctx))
				e.h.migrator.cluster = noopProcessor{}
			},
			act: func(t *testing.T, e env) {
				ec := errorcompounder.New()
				e.h.migrator.frozen(ctx, e.idx, []string{e.shardName}, ec)
				require.NoError(t, ec.ToError())
				require.Nil(t, e.idx.shards.Load(e.shardName), "offload should evict the shard")
			},
		},
		{
			name:    "shutdown panics",
			counted: map[storagestate.Status]float64{storagestate.StatusReady: 1},
			prepare: func(t *testing.T, e env) {
				// nil cycleCallbacks makes teardown panic part way.
				shard := e.shard.(*Shard)
				callbacks := shard.cycleCallbacks
				shard.cycleCallbacks = nil
				t.Cleanup(func() {
					shard.cycleCallbacks = callbacks
					require.NoError(t, shard.store.Shutdown(ctx))
				})
			},
			act: func(t *testing.T, e env) {
				shard, ok := e.idx.shards.LoadAndDelete(e.shardName)
				require.True(t, ok)
				require.Panics(t, func() { _ = shard.Shutdown(ctx) })
			},
		},
		{
			name:        "loaded tenant deactivated",
			multiTenant: true,
			counted:     map[storagestate.Status]float64{storagestate.StatusReady: 1},
			prepare: func(t *testing.T, e env) {
				require.NoError(t, e.shard.(*LazyLoadShard).Load(ctx))
			},
			act: func(t *testing.T, e env) {
				require.NoError(t, e.h.migrator.UpdateTenants(ctx, statusGaugeClass(e.className, true),
					[]*schemaUC.UpdateTenantPayload{{Name: e.shardName, Status: models.TenantActivityStatusCOLD}}, false))
				require.Nil(t, e.idx.shards.Load(e.shardName), "deactivation should evict the shard")
			},
		},
		{
			name:        "never-loaded tenant deactivated",
			multiTenant: true,
			counted:     nil,
			act: func(t *testing.T, e env) {
				require.NoError(t, e.h.migrator.UpdateTenants(ctx, statusGaugeClass(e.className, true),
					[]*schemaUC.UpdateTenantPayload{{Name: e.shardName, Status: models.TenantActivityStatusCOLD}}, false))
				require.Nil(t, e.idx.shards.Load(e.shardName), "deactivation should evict the shard")
			},
		},
		{
			name:        "never-loaded tenant collection dropped",
			multiTenant: true,
			counted:     nil,
			act: func(t *testing.T, e env) {
				require.NoError(t, e.h.migrator.DropClass(ctx, e.className, false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipAsRoot && os.Geteuid() == 0 {
				t.Skip("root ignores the directory permissions this case relies on")
			}

			h := newStatusGaugeHarness(t, statusGaugeOpts{multiTenant: tc.multiTenant})
			className := "StatusGaugeLeaves"
			idx := h.addClass(t, statusGaugeClass(className, tc.multiTenant))
			shardName, shard := onlyShard(t, idx)
			e := env{h: h, idx: idx, className: className, shardName: shardName, shard: shard}

			if tc.prepare != nil {
				tc.prepare(t, e)
			}
			h.requireBuckets(t, "before", tc.counted)

			tc.act(t, e)

			h.requireBuckets(t, "after", nil)
		})
	}
}

func TestShardStatusGaugeCountsEachShardOfACollection(t *testing.T) {
	ctx := testCtx()
	const tenants = 3

	h := newStatusGaugeHarness(t, statusGaugeOpts{multiTenant: true, tenants: tenants})
	className := "StatusGaugeMany"
	idx := h.addClass(t, statusGaugeClass(className, true))

	for i := 0; i < tenants; i++ {
		require.NoError(t, idx.shards.Load(statusGaugeTenant(i)).(*LazyLoadShard).Load(ctx))
	}
	h.requireBuckets(t, "with every tenant loaded",
		map[storagestate.Status]float64{storagestate.StatusReady: tenants})

	require.NoError(t, h.migrator.UpdateTenants(ctx, statusGaugeClass(className, true),
		[]*schemaUC.UpdateTenantPayload{
			{Name: statusGaugeTenant(0), Status: models.TenantActivityStatusCOLD},
			{Name: statusGaugeTenant(1), Status: models.TenantActivityStatusCOLD},
		}, false))
	h.requireBuckets(t, "with one tenant left active",
		map[storagestate.Status]float64{storagestate.StatusReady: 1})

	require.NoError(t, h.migrator.DropClass(ctx, className, false))
	h.requireBuckets(t, "after the collection is dropped", nil)
}

func TestShardStatusGaugeReleasedOnceLastReferenceDrops(t *testing.T) {
	h := newStatusGaugeHarness(t, statusGaugeOpts{})
	idx := h.addClass(t, statusGaugeClass("StatusGaugeInUse", false))
	_, shard := onlyShard(t, idx)
	concrete := shard.(*Shard)

	release, err := concrete.preventShutdown()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.Error(t, concrete.Shutdown(ctx), "shutdown is refused while a reference is held")
	h.requireBuckets(t, "while the shard is still in use",
		map[storagestate.Status]float64{storagestate.StatusReady: 1})

	release()
	h.requireBuckets(t, "after the last reference drops", nil)
}

func TestShardStatusGaugeReleasedAcrossCreateDropCycles(t *testing.T) {
	h := newStatusGaugeHarness(t, statusGaugeOpts{})

	for i := 0; i < 3; i++ {
		h.addClass(t, statusGaugeClass("StatusGaugeCycle", false))
		h.requireBuckets(t, "while the collection exists",
			map[storagestate.Status]float64{storagestate.StatusReady: 1})

		require.NoError(t, h.migrator.DropClass(testCtx(), "StatusGaugeCycle", false))
		h.requireBuckets(t, "after the collection is dropped", nil)
	}
}

func TestShardStatusGaugeReleasedWhenInitFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permissions this test relies on")
	}

	h := newStatusGaugeHarness(t, statusGaugeOpts{multiTenant: true})
	idx := h.addClass(t, statusGaugeClass("StatusGaugeInit", true))
	_, shard := onlyShard(t, idx)

	// Shard dir cannot be created, so init fails before the store exists.
	require.NoError(t, os.Chmod(idx.path(), 0o555))
	t.Cleanup(func() { _ = os.Chmod(idx.path(), 0o755) })

	require.Error(t, shard.(*LazyLoadShard).Load(testCtx()))

	h.requireBuckets(t, "after the failed init", nil)
}

// Paused queue with one object makes GetStatus recompute READY as INDEXING; the gauge must follow.
func TestShardStatusGaugeReleasedWhileStatusIsAccessed(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name string
		act  func(t *testing.T, h *statusGaugeHarness, className string, shard *Shard)
	}{
		{
			name: "collection dropped",
			act: func(t *testing.T, h *statusGaugeHarness, className string, _ *Shard) {
				require.NoError(t, h.migrator.DropClass(ctx, className, false))
			},
		},
		{
			name: "shut down",
			act: func(t *testing.T, _ *statusGaugeHarness, _ string, shard *Shard) {
				require.NoError(t, shard.Shutdown(ctx))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newStatusGaugeHarness(t, statusGaugeOpts{})
			className := "StatusGaugeConcurrent"
			idx := h.addClass(t, statusGaugeClass(className, false))
			_, shard := onlyShard(t, idx)
			concrete := shard.(*Shard)

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						// Updates fail once the store is closed, which is expected here.
						_ = concrete.GetStatus()
						_ = concrete.UpdateStatus(storagestate.StatusReadOnly.String(), "race")
						_ = concrete.UpdateStatus(storagestate.StatusReady.String(), "race")
					}
				}()
			}

			time.Sleep(20 * time.Millisecond)
			tc.act(t, h, className, concrete)
			close(stop)
			wg.Wait()

			h.requireBuckets(t, "after teardown with concurrent status access", nil)
		})
	}
}

type noopProcessor struct{ processor }

func TestShardStatusGaugeFollowsRecomputedStatus(t *testing.T) {
	ctx := testCtx()
	h := newStatusGaugeHarness(t, statusGaugeOpts{asyncIndex: true})
	className := "StatusGaugeRecompute"
	idx := h.addClass(t, statusGaugeClass(className, false))
	_, shard := onlyShard(t, idx)
	concrete := shard.(*Shard)

	var queue *VectorIndexQueue
	require.NoError(t, concrete.ForEachVectorQueue(func(_ string, q *VectorIndexQueue) error {
		queue = q
		return nil
	}))
	require.NotNil(t, queue)
	require.NoError(t, queue.Pause(ctx))

	obj := &models.Object{Class: className, ID: strfmt.UUID(uuid.NewString())}
	require.NoError(t, h.repo.PutObject(ctx, obj, []float32{1, 2, 3, 4}, nil, nil, nil, 0))

	require.Equal(t, storagestate.StatusIndexing, concrete.GetStatus())
	h.requireBuckets(t, "after GetStatus saw a backlog",
		map[storagestate.Status]float64{storagestate.StatusIndexing: 1})

	require.NoError(t, concrete.UpdateStatus(storagestate.StatusReadOnly.String(), "test"))
	h.requireBuckets(t, "after the shard went read-only",
		map[storagestate.Status]float64{storagestate.StatusReadOnly: 1})

	queue.Resume()
	require.NoError(t, h.migrator.DropClass(ctx, className, false))
	h.requireBuckets(t, "after the collection is dropped", nil)
}
