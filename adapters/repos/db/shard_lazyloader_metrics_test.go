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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// shardGauges is a reading of all four shard gauges at one moment, so a test
// asserts the whole set rather than the one gauge it expects to move.
type shardGauges struct {
	loaded    float64
	loading   float64
	unloaded  float64
	unloading float64
}

// shardMetricsHarness is a lazy-loading repo whose shard gauges start at zero,
// so a test reads them as counts of its own shards.
type shardMetricsHarness struct {
	repo         *DB
	migrator     *Migrator
	metrics      *monitoring.PrometheusMetrics
	schemaGetter *fakeSchemaGetter
}

func newShardMetricsHarness(t *testing.T) *shardMetricsHarness {
	return newShardMetricsHarnessWithLazyLoading(t, true)
}

func newShardMetricsHarnessWithLazyLoading(t *testing.T, lazyLoading bool) *shardMetricsHarness {
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
	for _, labels := range monitoring.AllShardLabels() {
		metrics.Shards.WithLabelValues(labels...).Set(0)
	}

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
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		EnableLazyLoadShards:      boolPtr(lazyLoading),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	return &shardMetricsHarness{
		repo:         repo,
		migrator:     NewMigrator(repo, logger, "node1"),
		metrics:      metrics,
		schemaGetter: schemaGetter,
	}
}

// addClass registers className and returns the name of its only shard, left unloaded.
func (h *shardMetricsHarness) addClass(t *testing.T, className string) string {
	t.Helper()
	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	require.NoError(t, h.migrator.AddClass(context.Background(), class))
	h.schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	var shardName string
	h.repo.GetIndex(schema.ClassName(className)).shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})
	require.NotEmpty(t, shardName, "the new class should have registered a shard")
	return shardName
}

func (h *shardMetricsHarness) gauges() shardGauges {
	return shardGauges{
		loaded:    testutil.ToFloat64(h.metrics.ShardsLoaded),
		loading:   testutil.ToFloat64(h.metrics.ShardsLoading),
		unloaded:  testutil.ToFloat64(h.metrics.ShardsUnloaded),
		unloading: testutil.ToFloat64(h.metrics.ShardsUnloading),
	}
}

// gaugesFor reads the same four states out of weaviate_shards for one
// registration, so a test can tell an eagerly-opened shard from a lazy one.
func (h *shardMetricsHarness) gaugesFor(reg monitoring.ShardRegistration) shardGauges {
	read := func(state monitoring.ShardState) float64 {
		return testutil.ToFloat64(h.metrics.Shards.WithLabelValues(string(state), string(reg)))
	}
	return shardGauges{
		loaded:    read(monitoring.ShardStateLoaded),
		loading:   read(monitoring.ShardStateLoading),
		unloaded:  read(monitoring.ShardStateUnloaded),
		unloading: read(monitoring.ShardStateUnloading),
	}
}

// TestShardRegistrationSplitsLoadedFromUnloaded pins the question
// weaviate_shards exists to answer: of the shards a lazy collection holds,
// which are loaded and which are still unloaded — and that an eager collection
// never reports a lazy one.
func TestShardRegistrationSplitsLoadedFromUnloaded(t *testing.T) {
	ctx := context.Background()
	const className = "TestShardRegistrationSplit"

	tests := []struct {
		name        string
		lazyLoading bool
		// registration is the series the collection's shards count against.
		registration monitoring.ShardRegistration
		// atRest is the split before anything touches the shard.
		atRest shardGauges
		// afterAccess is the split once the shard has been opened.
		afterAccess shardGauges
	}{
		{
			name:         "a lazy collection registers its shard unloaded and opens it on access",
			lazyLoading:  true,
			registration: monitoring.ShardRegistrationLazy,
			atRest:       shardGauges{unloaded: 1},
			afterAccess:  shardGauges{loaded: 1},
		},
		{
			name:         "an eager collection opens its shard at creation",
			lazyLoading:  false,
			registration: monitoring.ShardRegistrationEager,
			atRest:       shardGauges{loaded: 1},
			afterAccess:  shardGauges{loaded: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newShardMetricsHarnessWithLazyLoading(t, tt.lazyLoading)
			shardName := h.addClass(t, className)

			other := monitoring.ShardRegistrationEager
			if tt.registration == monitoring.ShardRegistrationEager {
				other = monitoring.ShardRegistrationLazy
			}

			require.Equal(t, tt.atRest, h.gaugesFor(tt.registration))
			require.Equal(t, shardGauges{}, h.gaugesFor(other),
				"the collection must not report shards under the other registration")
			require.Equal(t, tt.atRest, h.gauges(),
				"the legacy gauges must agree with the sum over registrations")

			shard := h.repo.GetIndex(className).shards.Load(shardName)
			if lazyShard, ok := shard.(*LazyLoadShard); ok {
				require.NoError(t, lazyShard.Load(ctx))
			}

			require.Equal(t, tt.afterAccess, h.gaugesFor(tt.registration))
			require.Equal(t, shardGauges{}, h.gaugesFor(other))
			require.Equal(t, tt.afterAccess, h.gauges())
		})
	}
}

// TestShardRemovalStopsCountingIt pins that a shard taken out of the shard map
// leaves the shard gauges. Shutdown alone moves it to unloaded, which is right
// only while it stays in the map: counted after removal, every tenant
// deactivation raises shards_unloaded for a shard the node no longer holds.
func TestShardRemovalStopsCountingIt(t *testing.T) {
	ctx := context.Background()
	const className = "TestShardRemovalCounting"

	tests := []struct {
		name string
		// loadFirst materializes the shard before it is removed.
		loadFirst bool
		remove    func(t *testing.T, h *shardMetricsHarness, shardName string)
		want      shardGauges
	}{
		{
			name: "ShutdownShard on an unloaded shard",
			remove: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.migrator.ShutdownShard(ctx, className, shardName))
			},
		},
		{
			name:      "ShutdownShard on a loaded shard",
			loadFirst: true,
			remove: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.migrator.ShutdownShard(ctx, className, shardName))
			},
		},
		{
			name: "UnloadLocalShard on an unloaded shard",
			remove: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.repo.GetIndex(className).UnloadLocalShard(ctx, shardName))
			},
		},
		{
			name:      "UnloadLocalShard on a loaded shard",
			loadFirst: true,
			remove: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.repo.GetIndex(className).UnloadLocalShard(ctx, shardName))
			},
		},
		{
			name:      "IncomingReinitShard counts the shard it puts back once",
			loadFirst: true,
			remove: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.repo.GetIndex(className).IncomingReinitShard(ctx, shardName))
			},
			want: shardGauges{unloaded: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newShardMetricsHarness(t)
			shardName := h.addClass(t, className)
			require.Equal(t, shardGauges{unloaded: 1}, h.gauges(),
				"a new lazy shard is counted as unloaded")

			if tt.loadFirst {
				shard := h.repo.GetIndex(className).shards.Load(shardName)
				require.NoError(t, shard.(*LazyLoadShard).Load(ctx))
				require.Equal(t, shardGauges{loaded: 1}, h.gauges())
			}

			tt.remove(t, h, shardName)

			require.Equal(t, tt.want, h.gauges())
		})
	}

	t.Run("a shard put back after a failed shutdown stays counted", func(t *testing.T) {
		// GetShard holds the shard, so Shutdown gives up after its retries and
		// shutdownOrRestoreShard puts the live instance back in the map. A shard in
		// the map is one the node still holds, so it keeps its place in the gauges.
		h := newShardMetricsHarness(t)
		shardName := h.addClass(t, className)

		index := h.repo.GetIndex(className)
		_, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		require.Equal(t, shardGauges{loaded: 1}, h.gauges())

		require.Error(t, index.UnloadLocalShard(ctx, shardName),
			"a shard in use cannot be shut down")
		require.Equal(t, shardGauges{loaded: 1}, h.gauges())

		release()
	})

	t.Run("a name the index does not hold moves no gauge", func(t *testing.T) {
		h := newShardMetricsHarness(t)
		h.addClass(t, className)

		require.NoError(t, h.repo.GetIndex(className).UnloadLocalShard(ctx, "no-such-shard"))

		require.Equal(t, shardGauges{unloaded: 1}, h.gauges(),
			"the class's own shard must keep its count")
	})

	t.Run("deactivating and activating in turn does not accumulate", func(t *testing.T) {
		h := newShardMetricsHarness(t)
		shardName := h.addClass(t, className)

		for range 3 {
			require.NoError(t, h.migrator.ShutdownShard(ctx, className, shardName))
			require.Equal(t, shardGauges{}, h.gauges(),
				"a deactivated shard must leave the gauges every time")

			require.NoError(t, h.migrator.LoadShardForMovement(ctx, className, shardName))
			require.Equal(t, shardGauges{loaded: 1}, h.gauges(),
				"an activated shard must be counted once, not once per activation")
		}
	})
}

// TestReactivationAfterDeferredShutdownCountsOnce pins that a reactivation
// stops counting the shut shard it evicts. A shutdown that finds the shard in
// use puts it back in the map and completes later, once the last reference
// drops; a reactivation then evicts that entry and initializes a fresh shard.
// Left counted, the evicted entry keeps shards_unloaded standing for a shard
// the node no longer holds.
func TestReactivationAfterDeferredShutdownCountsOnce(t *testing.T) {
	ctx := context.Background()
	const className = "TestReactivationCounting"

	tests := []struct {
		name       string
		reactivate func(t *testing.T, h *shardMetricsHarness, shardName string)
	}{
		{
			name: "load for replica movement",
			reactivate: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				require.NoError(t, h.migrator.LoadShardForMovement(ctx, className, shardName))
			},
		},
		{
			name: "write request",
			reactivate: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				_, release, err := h.repo.GetIndex(className).getOrInitShard(ctx, shardName)
				require.NoError(t, err)
				release()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newShardMetricsHarness(t)
			shardName := h.addClass(t, className)
			index := h.repo.GetIndex(className)

			// A request holding the shard makes the shutdown give up and put
			// the shard back in the map.
			_, release, err := index.GetShard(ctx, shardName)
			require.NoError(t, err)
			require.Equal(t, shardGauges{loaded: 1}, h.gauges())
			require.Error(t, index.UnloadLocalShard(ctx, shardName))

			// The last release completes the shutdown, leaving a shard in the
			// map that is shut but still counted.
			release()
			require.Equal(t, shardGauges{unloaded: 1}, h.gauges())

			tt.reactivate(t, h, shardName)

			require.Equal(t, shardGauges{loaded: 1}, h.gauges(),
				"the evicted shard must leave the gauges to the one replacing it")
		})
	}
}

// TestDropStopsCountingTheShard pins that a dropped shard leaves the gauge it
// was counted in, and that the drop itself completes. The shard is out of the
// shard map before drop runs, so a count left behind — or taken off the wrong
// gauge — stands for a shard the node no longer holds.
func TestDropStopsCountingTheShard(t *testing.T) {
	ctx := context.Background()
	const className = "TestDropCounting"

	tests := []struct {
		name string
		// prepare leaves the shard in the state it is dropped from.
		prepare   func(t *testing.T, h *shardMetricsHarness, shardName string)
		wantError bool
	}{
		{
			name:    "an unloaded shard",
			prepare: func(t *testing.T, h *shardMetricsHarness, shardName string) {},
		},
		{
			name: "a loaded shard",
			prepare: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				shard := h.repo.GetIndex(className).shards.Load(shardName)
				require.NoError(t, shard.(*LazyLoadShard).Load(ctx))
				require.Equal(t, shardGauges{loaded: 1}, h.gauges())
			},
		},
		{
			name: "a shard shut down while still in the map",
			prepare: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				// A request holding the shard makes the shutdown give up and
				// put the shard back; the last release then completes it, so
				// the drop meets a shard whose store is already closed.
				index := h.repo.GetIndex(className)
				_, release, err := index.GetShard(ctx, shardName)
				require.NoError(t, err)
				require.Error(t, index.UnloadLocalShard(ctx, shardName))
				release()
				require.Equal(t, shardGauges{unloaded: 1}, h.gauges(),
					"a shut shard in the map is counted as unloaded")
			},
		},
		{
			name: "a drop that fails partway",
			prepare: func(t *testing.T, h *shardMetricsHarness, shardName string) {
				index := h.repo.GetIndex(className)
				shard := index.shards.Load(shardName)
				require.NoError(t, shard.(*LazyLoadShard).Load(ctx))
				// Files removed underneath the shard fail the drop before it
				// finishes.
				require.NoError(t, os.RemoveAll(shardPath(index.path(), shardName)))
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newShardMetricsHarness(t)
			shardName := h.addClass(t, className)
			require.Equal(t, shardGauges{unloaded: 1}, h.gauges())
			index := h.repo.GetIndex(className)

			tt.prepare(t, h, shardName)

			err := index.dropShards([]string{shardName})
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoDirExists(t, shardPath(index.path(), shardName),
					"a dropped shard must leave no files behind")
			}

			require.Equal(t, shardGauges{}, h.gauges(),
				"a dropped shard must leave every gauge")
		})
	}
}

// TestDropDuringDeferredShutdownCountsOnce pins that a shutdown starting while
// a drop is under way leaves every gauge at zero. Both move the same shard
// between gauges, so whichever reaches them second has to find the shard
// already claimed by the first.
func TestDropDuringDeferredShutdownCountsOnce(t *testing.T) {
	// The two teardowns differ only in timing, so one round reproduces the
	// skew about twice in five. Ten rounds keep the odds of missing it under
	// a percent.
	const rounds = 10

	h := newShardMetricsHarness(t)
	for i := 0; i < rounds; i++ {
		t.Run(fmt.Sprintf("round_%d", i), func(t *testing.T) {
			dropDuringDeferredShutdown(t, h, fmt.Sprintf("TestDropDuringShutdown%d", i))
			require.Equal(t, shardGauges{}, h.gauges(),
				"a shard dropped while a shutdown runs must leave every gauge")
		})
	}
}

// dropDuringDeferredShutdown drops className's only shard while the shutdown
// that its last reference release triggers is still tearing the shard down.
func dropDuringDeferredShutdown(t *testing.T, h *shardMetricsHarness, className string) {
	t.Helper()
	ctx := context.Background()

	shardName := h.addClass(t, className)
	index := h.repo.GetIndex(schema.ClassName(className))

	// A request holding the shard makes the unload give up and put the shard
	// back, leaving shutdownRequested set so the last release completes it.
	_, release, err := index.GetShard(ctx, shardName)
	require.NoError(t, err)
	require.Equal(t, shardGauges{loaded: 1}, h.gauges())

	// A deadline shorter than the shutdown's own retry window ends the wait on
	// the held reference without changing its outcome.
	unloadCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	require.Error(t, index.UnloadLocalShard(unloadCtx, shardName))

	lazy, ok := index.shards.Load(shardName).(*LazyLoadShard)
	require.True(t, ok, "a shard put back after a failed unload stays in the map")
	shard := lazy.shard
	require.NotNil(t, shard)

	// Files removed underneath the shard end the drop early, so its metric
	// bookkeeping runs while the shutdown is still tearing the shard down.
	require.NoError(t, os.RemoveAll(shardPath(index.path(), shardName)))

	// Both the drop and the shutdown the release triggers queue on
	// shutdownLock while the test holds it. The drop queues first, so it
	// reaches the gauges while the shutdown is still waiting.
	shard.shutdownLock.Lock()

	dropped := make(chan error, 1)
	enterrors.GoWrapper(func() {
		dropped <- index.dropShards([]string{shardName})
	}, index.logger)

	// dropShards takes the shard out of the map before drop runs, so this is
	// the last step observable from here before the drop blocks on the lock.
	require.Eventually(t, func() bool {
		return index.shards.Load(shardName) == nil
	}, 5*time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	// The release runs the shutdown on this goroutine, so it blocks too.
	released := make(chan struct{})
	enterrors.GoWrapper(func() {
		release()
		close(released)
	}, index.logger)
	require.Eventually(t, func() bool {
		return shard.inUseCounter.Load() == 0
	}, 5*time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	shard.shutdownLock.Unlock()

	select {
	case err := <-dropped:
		require.Error(t, err, "the drop should fail on the removed files")
	case <-time.After(time.Minute):
		t.Fatal("drop did not finish")
	}
	<-released
}

// TestLazyLoadShardMetricsLifecycle tests the full lifecycle of shard metrics:
// 1. Creating a shard increments ShardsUnloaded
// 2. Loading a shard transitions from Unloaded -> Loading -> Loaded
// 3. Dropping a shard transitions from Loaded -> Unloading -> Unloaded
func TestLazyLoadShardMetricsLifecycle(t *testing.T) {
	ctx := context.Background()
	className := "TestMetricsLifecycle"

	h := newShardMetricsHarness(t)
	repo, metrics, migrator, schemaGetter := h.repo, h.metrics, h.migrator, h.schemaGetter
	var err error

	t.Run("create shard increments unloaded count", func(t *testing.T) {
		// Add class - this creates a shard in unloaded state
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

		err = migrator.AddClass(context.Background(), class)
		require.NoError(t, err)
		schemaGetter.schema = sch

		// After creating class, shard should be in unloaded state
		// (NewUnloadedshard() was called)
		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(1), unloadedCount, "shard should be counted as unloaded after creation")

		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "no shards should be loaded yet")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})

	t.Run("loading shard transitions metrics correctly", func(t *testing.T) {
		// Get shard name
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		// Get the lazy shard
		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		require.NotNil(t, shard)
		defer release()

		lazyShard, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard should be a LazyLoadShard")

		// Load the shard - this should update metrics:
		// StartLoadingShard: unloaded--, loading++
		// FinishLoadingShard: loading--, loaded++
		err = lazyShard.Load(ctx)
		require.NoError(t, err)

		// After loading, shard should be counted as loaded
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(1), loadedCount, "shard should be counted as loaded after Load()")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "shard should no longer be counted as unloaded after Load()")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be in loading state after load completes")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})

	t.Run("add object to loaded shard", func(t *testing.T) {
		// Add an object so we have data for later tests
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", 1)).String())
		obj := &models.Object{Class: className, ID: id}
		err = repo.PutObject(ctx, obj, []float32{1, 2, 3, 4}, nil, nil, nil, 0)
		require.NoError(t, err)
	})

	t.Run("loading already loaded shard is idempotent", func(t *testing.T) {
		// Get shard
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		defer release()

		lazyShard := shard.(*LazyLoadShard)

		// Load again - should be a no-op since already loaded
		err = lazyShard.Load(ctx)
		require.NoError(t, err)

		// Metrics should remain unchanged
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(1), loadedCount, "loaded count should remain 1")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "unloaded count should remain 0")
	})

	t.Run("shutdown loaded shard transitions metrics correctly", func(t *testing.T) {
		// Get the loaded shard and shut it down
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		release()

		lazyShard := shard.(*LazyLoadShard)

		// Shutdown the shard (unload from memory, but keep on disk)
		// This should call StartUnloadingShard and FinishUnloadingShard
		err = lazyShard.Shutdown(ctx)
		require.NoError(t, err)

		// After shutdown, the shard is unloaded (still exists on disk)
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "shard should no longer be counted as loaded after shutdown")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(1), unloadedCount, "shard should be counted as unloaded after shutdown")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading after shutdown completes")
	})

	t.Run("dropping shard decrements count correctly", func(t *testing.T) {
		// At this point, the shard is unloaded (from previous test)
		// Drop the class (which drops shards)
		// Since the shard was unloaded, LazyLoadShard.drop() should call DeleteUnloadedShard
		err = migrator.DropClass(ctx, className, false)
		require.NoError(t, err)

		// After dropping, all counts should be 0
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "no shards should be loaded")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "deleted shard should not be counted as unloaded")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})
}
