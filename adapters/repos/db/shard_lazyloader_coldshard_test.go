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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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

// newLazyLoadRepo wires a lazy-load-enabled repo whose shards start cold.
// It registers no Shutdown cleanup — callers own the repo's lifecycle.
func newLazyLoadRepo(t *testing.T, shardState *sharding.State) (*DB, *Migrator, *fakeSchemaGetter) {
	t.Helper()
	ctx := testCtx()
	logger, _ := test.NewNullLogger()

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
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(true),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader,
	)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(ctx))

	return repo, NewMigrator(repo, logger, "node1"), schemaGetter
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
