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
	"path"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// failingAllocChecker fails every mapping reservation, so LazyLoadShard.Load
// (and therefore mustLoad) fails for any shard that gets force-loaded.
type failingAllocChecker struct{}

func (failingAllocChecker) CheckAlloc(int64) error { return nil }

func (failingAllocChecker) CheckMappingAndReserve(int64, int) error {
	return fmt.Errorf("memory pressure: injected")
}

func (failingAllocChecker) Refresh(bool) {}

// addPropertyLazyFixture wires a lazy-load-enabled repo whose shards start cold.
//
// schemaClass is the class returned by getClass(); tests grow it to simulate a
// schema property-add. It is a distinct object from the class snapshot captured
// inside each LazyLoadShard, so a shard that reuses its frozen snapshot instead
// of re-reading the schema at load would miss the added property.
type addPropertyLazyFixture struct {
	migrator    *Migrator
	index       *Index
	schemaClass *models.Class
}

// newLazyLoadRepo wires a lazy-load-enabled repo whose shards start cold and
// stay cold, so a test decides when each one loads.
// It registers no Shutdown cleanup — callers own the repo's lifecycle.
func newLazyLoadRepo(t *testing.T, shardState *sharding.State) (*DB, *Migrator, *fakeSchemaGetter) {
	t.Helper()
	repo, migrator, schemaGetter, _ := newLazyLoadRepoWithConfig(t, shardState, true, true)
	return repo, migrator, schemaGetter
}

// newLazyLoadRepoWithConfig wires a repo with the two lazy-shard knobs set as
// given, returning the log hook so a test can read what startup logged.
func newLazyLoadRepoWithConfig(t *testing.T, shardState *sharding.State,
	lazyLoad, warmupDisabled bool,
) (*DB, *Migrator, *fakeSchemaGetter, *test.Hook) {
	t.Helper()
	ctx := testCtx()
	logger, hook := test.NewNullLogger()

	baseMetrics := monitoring.GetMetrics()
	metricsCopy := *baseMetrics
	metricsCopy.Registerer = monitoring.NoopRegisterer
	metrics := &metricsCopy

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(&models.Class{Class: className}, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                    t.TempDir(),
		QueryMaximumResults:         10000,
		MaxImportGoroutinesFactor:   1,
		EnableLazyLoadShards:        boolPtr(lazyLoad),
		LazyLoadShardWarmupDisabled: warmupDisabled,
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil,
	)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(ctx))

	return repo, NewMigrator(repo, logger, "node1"), schemaGetter, hook
}

func newAddPropertyLazyFixture(t *testing.T, className string, shardState *sharding.State) *addPropertyLazyFixture {
	t.Helper()
	ctx := testCtx()
	repo, migrator, schemaGetter := newLazyLoadRepo(t, shardState)
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	require.NoError(t, migrator.AddClass(ctx, newClassWithWarmProp(className)))
	// Serve a distinct class object from getClass() so that only the code path
	// that re-reads the schema at load sees a property added after AddClass.
	schemaClass := newClassWithWarmProp(className)
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{schemaClass}}}

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)

	return &addPropertyLazyFixture{migrator: migrator, index: index, schemaClass: schemaClass}
}

// coldShards returns every shard as an unloaded LazyLoadShard, keyed by name,
// asserting none is loaded yet.
func (f *addPropertyLazyFixture) coldShards(t *testing.T) map[string]*LazyLoadShard {
	t.Helper()
	shards := map[string]*LazyLoadShard{}
	f.index.shards.Range(func(name string, shard ShardLike) error {
		lazyShard, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard should be a LazyLoadShard")
		require.False(t, lazyShard.isLoaded(), "shard %q should start cold", name)
		shards[name] = lazyShard
		return nil
	})
	require.NotEmpty(t, shards)
	return shards
}

func newClassWithWarmProp(className string) *models.Class {
	return &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties:          []*models.Property{textProp("warm", true)},
	}
}

// textProp is a text property; indexed controls whether it carries an inverted
// index and therefore whether loading a shard must create a bucket for it.
func textProp(name string, indexed bool) *models.Property {
	return &models.Property{
		Name:            name,
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWhitespace,
		IndexFilterable: &indexed,
		IndexSearchable: &indexed,
	}
}

// Adding properties to a class whose shards are cold must not force-load those
// shards; each property is materialized the next time a shard loads, and only
// if it carries an inverted index.
func TestAddProperty_ColdShardMaterializesAtLoad(t *testing.T) {
	ctx := testCtx()

	cases := []struct {
		name        string
		props       []*models.Property
		wantBuckets map[string]bool // property name -> bucket expected after load
	}{
		{
			name:        "single indexed property",
			props:       []*models.Property{textProp("indexedA", true)},
			wantBuckets: map[string]bool{"indexedA": true},
		},
		{
			name:        "multiple indexed properties at once",
			props:       []*models.Property{textProp("indexedA", true), textProp("indexedB", true)},
			wantBuckets: map[string]bool{"indexedA": true, "indexedB": true},
		},
		{
			name:        "property without inverted index creates no bucket",
			props:       []*models.Property{textProp("noIndex", false)},
			wantBuckets: map[string]bool{"noIndex": false},
		},
		{
			name:        "mix of indexed and non-indexed properties",
			props:       []*models.Property{textProp("indexedA", true), textProp("noIndex", false)},
			wantBuckets: map[string]bool{"indexedA": true, "noIndex": false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, "AddPropCold", singleShardState())
			cold := f.coldShards(t)

			// Mirror the real apply order: the schema carries the new properties
			// before the store update runs.
			f.schemaClass.Properties = append(f.schemaClass.Properties, tc.props...)
			require.NoError(t, f.migrator.AddProperty(ctx, f.schemaClass.Class, tc.props...))

			for name, shard := range cold {
				require.False(t, shard.isLoaded(), "shard %q must not be force-loaded by add-property", name)
			}

			for _, shard := range cold {
				require.NoError(t, shard.Load(ctx))
				for propName, want := range tc.wantBuckets {
					bucket := shard.Store().Bucket(helpers.BucketFromPropNameLSM(propName))
					if want {
						require.NotNil(t, bucket, "bucket for %q must exist after load", propName)
					} else {
						require.Nil(t, bucket, "bucket for %q must not be created", propName)
					}
				}
			}
		})
	}
}

// Loading a shard reflects schema changes made after it was created, not the
// snapshot captured at creation. Exercised here without any add-property call
// so it pins the load-time refresh on its own.
func TestLazyLoadShard_LoadReflectsSchemaChangedWhileCold(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "LoadRefresh", singleShardState())
	cold := f.coldShards(t)

	prop := textProp("addedWhileCold", true)
	f.schemaClass.Properties = append(f.schemaClass.Properties, prop)

	for _, shard := range cold {
		require.NoError(t, shard.Load(ctx))
		require.NotNil(t, shard.Store().Bucket(helpers.BucketFromPropNameLSM(prop.Name)),
			"load must reflect the property added while the shard was cold")
	}
}

// A loaded shard gets the new property's bucket immediately, while its cold
// siblings are left untouched and materialize the property only when they load.
func TestAddProperty_LoadedAndColdShardsMix(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "AddPropMix", multiShardState())
	shards := f.coldShards(t)
	require.Greater(t, len(shards), 1, "need multiple shards to exercise the mix")

	// Warm exactly one shard.
	var warmName string
	for name, shard := range shards {
		require.NoError(t, shard.Load(ctx))
		warmName = name
		break
	}

	prop := textProp("mixProp", true)
	f.schemaClass.Properties = append(f.schemaClass.Properties, prop)
	require.NoError(t, f.migrator.AddProperty(ctx, f.schemaClass.Class, prop))

	bucketName := helpers.BucketFromPropNameLSM(prop.Name)
	for name, shard := range shards {
		if name == warmName {
			require.NotNil(t, shard.Store().Bucket(bucketName),
				"loaded shard %q must get the bucket during add-property", name)
			continue
		}
		require.False(t, shard.isLoaded(), "cold shard %q must not be force-loaded", name)
		require.NoError(t, shard.Load(ctx))
		require.NotNil(t, shard.Store().Bucket(bucketName),
			"cold shard %q must materialize the bucket at load", name)
	}
}

// Adding a property must not panic (via mustLoad) when a cold shard's load would
// fail — the schema apply that carries the add must survive memory pressure.
func TestAddProperty_ColdShardLoadFailureDoesNotPanic(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "AddPropNoPanic", singleShardState())
	cold := f.coldShards(t)
	// Force any load attempt to fail: a force-load here would panic via mustLoad.
	for _, shard := range cold {
		shard.memMonitor = failingAllocChecker{}
	}

	prop := textProp("underPressure", true)
	f.schemaClass.Properties = append(f.schemaClass.Properties, prop)

	require.NotPanics(t, func() {
		require.NoError(t, f.migrator.AddProperty(ctx, f.schemaClass.Class, prop))
	})
	for name, shard := range cold {
		require.False(t, shard.isLoaded(), "cold shard %q must remain unloaded", name)
	}
}

// preventShutdown must return a callable release even when the load it triggers
// fails, so a caller can defer the release before checking the error.
func TestLazyLoadShard_PreventShutdownAlwaysReturnsRelease(t *testing.T) {
	cases := []struct {
		name      string
		loadFails bool
	}{
		{name: "load succeeds"},
		{name: "load fails", loadFails: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, "PreventShutdownRelease", singleShardState())

			for name, shard := range f.coldShards(t) {
				if tc.loadFails {
					shard.memMonitor = failingAllocChecker{}
				}

				release, err := shard.preventShutdown()
				require.NotNil(t, release, "release for shard %q must never be nil", name)
				if tc.loadFails {
					require.Error(t, err)
					require.False(t, shard.isLoaded(), "shard %q must remain unloaded", name)
				} else {
					require.NoError(t, err)
				}
				release()
			}
		})
	}
}

// A failed shard init must name the shard and the index: the errors raised
// while initializing carry no such context of their own, and the caller may be
// one branch of a multi-shard fan-out.
func TestGetOrInitShard_InitFailureNamesShard(t *testing.T) {
	f := newAddPropertyLazyFixture(t, "InitFailureContext", singleShardState())
	f.index.allocChecker = failingAllocChecker{}

	shard, release, err := f.index.getOrInitShard(testCtx(), "uninitialized-shard")
	require.NotNil(t, release)
	defer release()
	require.Nil(t, shard)

	require.ErrorContains(t, err, `init local shard "uninitialized-shard"`)
	require.ErrorContains(t, err, f.index.ID())
	require.ErrorContains(t, err, "memory pressure")
}

// coldTestObject builds an object for the class newClassWithWarmProp defines.
func coldTestObject(className string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{"warm": "object count fixture"},
		},
	}
}

// writeCountedObjects writes n objects and returns the shard cold with them in a
// segment a cold count can read: flushing while the shard runs writes each new
// segment's sidecar, and the count comes from those sidecars.
func writeCountedObjects(t *testing.T, shard *LazyLoadShard, className string, n int) {
	t.Helper()
	ctx := testCtx()
	require.NoError(t, shard.Load(ctx))
	for range n {
		require.NoError(t, shard.PutObject(ctx, coldTestObject(className)))
	}
	require.NoError(t, shard.Store().FlushMemtables(ctx))
	require.NoError(t, shard.Shutdown(ctx))
	require.False(t, shard.isLoaded(), "shard %q should be cold again", shard.Name())
}

// writeUncountedObjects writes n objects and returns the shard cold with them in
// a segment a cold count cannot read: the shutdown flush writes the segment but
// not the sidecar holding its object count, which the next load derives.
func writeUncountedObjects(t *testing.T, shard *LazyLoadShard, className string, n int) {
	t.Helper()
	ctx := testCtx()
	require.NoError(t, shard.Load(ctx))
	for range n {
		require.NoError(t, shard.PutObject(ctx, coldTestObject(className)))
	}
	require.NoError(t, shard.Shutdown(ctx))
	require.False(t, shard.isLoaded(), "shard %q should be cold again", shard.Name())
}

// A cold shard answers ObjectCountAsync by listing its objects directory and
// reading a sidecar per segment. nodeWideMetricsObserver asks every shard every
// 30 seconds, and a cold shard cannot move that number without loading first,
// so the answer is cached.
func TestLazyLoadShard_ObjectCountAsyncCachesColdCount(t *testing.T) {
	ctx := testCtx()
	const className = "ColdObjectCount"

	cases := []struct {
		name    string
		objects int
	}{
		{name: "empty shard"},
		{name: "single object", objects: 1},
		{name: "many objects", objects: 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, className, singleShardState())

			for name, shard := range f.coldShards(t) {
				writeCountedObjects(t, shard, className, tc.objects)

				first, err := shard.ObjectCountAsync(ctx)
				require.NoError(t, err)
				require.EqualValues(t, tc.objects, first, "cold read must count what shard %q has on disk", name)

				// A second disk read cannot succeed once the objects directory is gone, so
				// any answer at all proves the count came from the cache.
				require.NoError(t, os.RemoveAll(path.Join(shard.pathLSM(), helpers.ObjectsBucketLSM)))

				second, err := shard.ObjectCountAsync(ctx)
				require.NoError(t, err, "shard %q re-read the objects directory", name)
				require.EqualValues(t, tc.objects, second)
			}
		})
	}
}

// breakProplenTracker makes the shard's next load fail after NewShard has
// already opened the objects bucket and written its segment sidecars, by
// putting a directory where initProplenTracker expects a file.
func breakProplenTracker(t *testing.T, shard *LazyLoadShard) {
	t.Helper()
	plPath := path.Join(path.Dir(shard.pathLSM()), "proplengths")
	require.NoError(t, os.RemoveAll(plPath))
	require.NoError(t, os.Mkdir(plPath, os.ModePerm))
}

// A load writes the segment sidecars a cold count reads, so the count cached
// before it is stale — even when the load fails partway and leaves the shard
// cold. A load that fails before reaching NewShard has touched nothing, so the
// cache still holds.
func TestLazyLoadShard_LoadInvalidatesCachedColdCount(t *testing.T) {
	ctx := testCtx()
	const (
		className = "ColdObjectCountInvalidation"
		counted   = 5 // objects a cold read counts, because their segment has its sidecar
		uncounted = 3 // objects whose segment gets its sidecar only at the next load
	)

	cases := []struct {
		name      string
		breakLoad func(t *testing.T, shard *LazyLoadShard)
		wantErr   bool
		// wantCached asserts the answer came from the cache, not from disk.
		wantCached bool
		wantCount  int64
	}{
		{
			name:      "load succeeds",
			breakLoad: func(*testing.T, *LazyLoadShard) {},
			wantCount: counted + uncounted,
		},
		{
			name:      "load fails after touching disk",
			breakLoad: breakProplenTracker,
			wantErr:   true,
			wantCount: counted + uncounted,
		},
		{
			name: "load fails before touching disk",
			breakLoad: func(_ *testing.T, shard *LazyLoadShard) {
				shard.memMonitor = failingAllocChecker{}
			},
			wantErr:    true,
			wantCached: true,
			wantCount:  counted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, className, singleShardState())

			for name, shard := range f.coldShards(t) {
				writeCountedObjects(t, shard, className, counted)
				writeUncountedObjects(t, shard, className, uncounted)

				cold, err := shard.ObjectCountAsync(ctx)
				require.NoError(t, err)
				require.EqualValues(t, counted, cold,
					"shard %q counts the segments whose sidecar is on disk", name)

				tc.breakLoad(t, shard)
				loadErr := shard.Load(ctx)
				if tc.wantErr {
					require.Error(t, loadErr)
				} else {
					require.NoError(t, loadErr)
				}

				// Go cold again, so the next count comes from the cold path rather than
				// from the loaded shard.
				require.NoError(t, shard.Shutdown(ctx))

				if tc.wantCached {
					// A disk read cannot succeed once the objects directory is gone, so
					// any answer at all proves the cache survived the failed load.
					require.NoError(t, os.RemoveAll(path.Join(shard.pathLSM(), helpers.ObjectsBucketLSM)))
				}

				count, err := shard.ObjectCountAsync(ctx)
				require.NoError(t, err)
				require.EqualValues(t, tc.wantCount, count,
					"cold count for shard %q after the load attempt", name)
			}
		})
	}
}

// background_warmup tells an operator whether this collection's cold shards get
// loaded by the startup sweep. An eagerly loaded collection has no such sweep,
// whatever LazyLoadShardWarmupDisabled is set to.
func TestMigratorAddClass_LogsBackgroundWarmup(t *testing.T) {
	ctx := testCtx()

	cases := []struct {
		name           string
		lazyLoad       bool
		warmupDisabled bool
		want           bool
	}{
		{name: "lazy loading with warmup", lazyLoad: true, want: true},
		{name: "lazy loading with warmup disabled", lazyLoad: true, warmupDisabled: true},
		{name: "eager loading with warmup"},
		{name: "eager loading with warmup disabled", warmupDisabled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo, migrator, _, hook := newLazyLoadRepoWithConfig(t, singleShardState(), tc.lazyLoad, tc.warmupDisabled)
			t.Cleanup(func() { repo.Shutdown(context.Background()) })

			require.NoError(t, migrator.AddClass(ctx, newClassWithWarmProp("WarmupLogging")))

			// Warnings on the same action carry no background_warmup field.
			var logged []interface{}
			for _, entry := range hook.AllEntries() {
				warmup, ok := entry.Data["background_warmup"]
				if ok && entry.Data["action"] == "lazy_shard_auto_detection" {
					logged = append(logged, warmup)
				}
			}
			require.Equal(t, []interface{}{tc.want}, logged)
		})
	}
}

// Resuming maintenance cycles after a backup must not force-load cold shards:
// an unloaded shard has no running cycles to resume.
func TestResumeMaintenanceCycles_DoesNotForceLoadColdShards(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "ResumeMaintenance", multiShardState())
	cold := f.coldShards(t)

	require.NoError(t, f.index.resumeMaintenanceCycles(ctx))

	for name, shard := range cold {
		require.False(t, shard.isLoaded(), "cold shard %q must not be force-loaded", name)
	}
}
