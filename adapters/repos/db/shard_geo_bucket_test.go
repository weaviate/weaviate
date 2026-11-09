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
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const geoBucketClass = "GeoBucketClass"

func geoAt(latitude, longitude float32) *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: &latitude, Longitude: &longitude}
}

var (
	berlinCoords  = geoAt(52.520, 13.405)
	munichCoords  = geoAt(48.137, 11.575)
	hamburgCoords = geoAt(53.551, 9.993)
)

func removeRootPath(t *testing.T, idx *Index) {
	t.Helper()
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func geoBucketTestClass() *models.Class {
	return &models.Class{
		Class: geoBucketClass,
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
			{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString()},
			{Name: "home", DataType: schema.DataTypeGeoCoordinates.PropString()},
		},
	}
}

// testGeoBucketShard starts a shard whose class carries two geo properties.
func testGeoBucketShard(t *testing.T, ctx context.Context) (*Shard, *Index) {
	t.Helper()
	shard, index := testShardWithSettings(t, ctx, geoBucketTestClass(),
		enthnsw.UserConfig{Skip: true}, false, false)
	s, ok := shard.(*Shard)
	require.True(t, ok)
	return s, index
}

func geoTestObject(props map[string]interface{}) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      geoBucketClass,
			Properties: props,
		},
	}
}

// geoDocIDsWithin reads the bucket directly, which proves the write path
// rather than the filter resolution.
func geoDocIDsWithin(t *testing.T, s *Shard, propName string, center *models.GeoCoordinates, meters float32) []uint64 {
	t.Helper()
	bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName))
	require.NotNil(t, bucket, "geo bucket of %q missing", propName)
	bm, err := bucket.GeoPointsWithinRange(context.Background(), *center.Latitude, *center.Longitude, meters)
	require.NoError(t, err)
	out := bm.ToArray()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// geoFilterDocIDs resolves a withinGeoRange filter the way a query does.
func geoFilterDocIDs(t *testing.T, s *Shard, propName string, center *models.GeoCoordinates, meters float32) []uint64 {
	t.Helper()
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorWithinGeoRange,
		On:       &filters.Path{Class: schema.ClassName(geoBucketClass), Property: schema.PropertyName(propName)},
		Value: &filters.Value{
			Value: filters.GeoRange{GeoCoordinates: center, Distance: meters},
			Type:  schema.DataTypeGeoCoordinates,
		},
	}}
	allow, err := s.buildAllowList(context.Background(), filter, additional.Properties{})
	require.NoError(t, err)
	out := allow.Slice()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestGeoBucketWritePaths(t *testing.T) {
	ctx := context.Background()

	t.Run("put indexes every geo property and the filter finds it", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords, "name": "a"})
		require.NoError(t, s.PutObject(ctx, obj))
		noGeo := geoTestObject(map[string]interface{}{"name": "b"})
		require.NoError(t, s.PutObject(ctx, noGeo))

		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "home", munichCoords, 1000))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", munichCoords, 1000))
		assert.Equal(t, []uint64{obj.DocID}, geoFilterDocIDs(t, s, "location", berlinCoords, 1000))
		assert.Empty(t, geoFilterDocIDs(t, s, "home", berlinCoords, 1000))
	})

	t.Run("update moves the point and retires the old doc id", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
		require.NoError(t, s.PutObject(ctx, obj))
		oldDocID := obj.DocID

		updated := geoTestObject(map[string]interface{}{"location": hamburgCoords, "home": munichCoords})
		updated.Object.ID = obj.Object.ID
		require.NoError(t, s.PutObject(ctx, updated))
		require.NotEqual(t, oldDocID, updated.DocID, "an update must get a new doc id for this test to mean anything")

		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
		assert.Equal(t, []uint64{updated.DocID}, geoDocIDsWithin(t, s, "location", hamburgCoords, 1000))
		assert.Equal(t, []uint64{updated.DocID}, geoDocIDsWithin(t, s, "home", munichCoords, 1000),
			"the unchanged property follows the new doc id")
	})

	t.Run("update that drops the property removes the point", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
		require.NoError(t, s.PutObject(ctx, obj))

		updated := geoTestObject(map[string]interface{}{"home": munichCoords})
		updated.Object.ID = obj.Object.ID
		require.NoError(t, s.PutObject(ctx, updated))

		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
		assert.Equal(t, []uint64{updated.DocID}, geoDocIDsWithin(t, s, "home", munichCoords, 1000))
	})

	t.Run("coordinates arriving as a map are indexed", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": map[string]any{
			"latitude": float64(*berlinCoords.Latitude), "longitude": float64(*berlinCoords.Longitude),
		}})
		require.NoError(t, s.PutObject(ctx, obj))
		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	})

	t.Run("batch put indexes and batch update retires", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		objs := []*storobj.Object{
			geoTestObject(map[string]interface{}{"location": berlinCoords}),
			geoTestObject(map[string]interface{}{"location": munichCoords}),
			geoTestObject(map[string]interface{}{"name": "no geo"}),
		}
		for _, err := range s.PutObjectBatch(ctx, objs) {
			require.NoError(t, err)
		}
		assert.ElementsMatch(t, []uint64{objs[0].DocID, objs[1].DocID},
			geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))

		moved := geoTestObject(map[string]interface{}{"location": hamburgCoords})
		moved.Object.ID = objs[0].Object.ID
		for _, err := range s.PutObjectBatch(ctx, []*storobj.Object{moved}) {
			require.NoError(t, err)
		}
		assert.ElementsMatch(t, []uint64{moved.DocID, objs[1].DocID},
			geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	})

	t.Run("a point survives a flush and a restart", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeRootPath(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords})
		require.NoError(t, s.PutObject(ctx, obj))
		require.NoError(t, s.store.FlushMemtables(ctx))
		gone := geoTestObject(map[string]interface{}{"location": munichCoords})
		require.NoError(t, s.PutObject(ctx, gone))
		require.NoError(t, s.DeleteObject(ctx, gone.Object.ID, time.Time{}))
		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))

		class := geoBucketTestClass()
		require.NoError(t, s.Shutdown(ctx))
		reloaded, err := NewShard(ctx, nil, s.Name(), idx, class,
			idx.centralJobQueue, idx.scheduler, idx.shardReindexer, false, idx.bitmapBufPool,
			monitoring.ShardRegistrationEager)
		require.NoError(t, err)
		defer reloaded.Shutdown(ctx)

		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, reloaded, "location", berlinCoords, 1_000_000))
	})
}

func TestGeoBucketDeletePaths(t *testing.T) {
	tests := []struct {
		name   string
		delete func(t *testing.T, ctx context.Context, s *Shard, id strfmt.UUID)
	}{
		{
			name: "single-object delete",
			delete: func(t *testing.T, ctx context.Context, s *Shard, id strfmt.UUID) {
				require.NoError(t, s.DeleteObject(ctx, id, time.Time{}))
			},
		},
		{
			name: "batch delete",
			delete: func(t *testing.T, ctx context.Context, s *Shard, id strfmt.UUID) {
				for _, result := range s.DeleteObjectBatch(ctx, []strfmt.UUID{id}, time.Time{}, false) {
					require.NoError(t, result.Err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, idx := testGeoBucketShard(t, ctx)
			defer removeRootPath(t, idx)

			obj := geoTestObject(map[string]interface{}{"location": munichCoords, "home": berlinCoords})
			require.NoError(t, s.PutObject(ctx, obj))
			other := geoTestObject(map[string]interface{}{"location": munichCoords})
			require.NoError(t, s.PutObject(ctx, other))
			require.Equal(t, []uint64{obj.DocID, other.DocID}, geoDocIDsWithin(t, s, "location", munichCoords, 10_000),
				"sanity: both objects must be geo-indexed before the delete")

			test.delete(t, ctx, s, obj.Object.ID)

			assert.Equal(t, []uint64{other.DocID}, geoDocIDsWithin(t, s, "location", munichCoords, 10_000),
				"the deleted object must be gone from the geo bucket, the other kept")
			assert.Empty(t, geoDocIDsWithin(t, s, "home", berlinCoords, 10_000))
			assert.Equal(t, []uint64{other.DocID}, geoFilterDocIDs(t, s, "location", munichCoords, 10_000))
		})
	}
}

// TestGeoBucketLegacyIndexMigration pins that a shard upgraded from the geo
// HNSW rebuilds its bucket from the objects and removes the old directories,
// whatever state the bucket is in.
func TestGeoBucketLegacyIndexMigration(t *testing.T) {
	tests := []struct {
		name       string
		legacyDirs []string
		// wreckBucket damages the bucket before the restart, standing in for a
		// bucket an interrupted earlier migration left behind
		wreckBucket func(t *testing.T, s *Shard)
	}{
		{
			name:       "commit log dir, no bucket",
			legacyDirs: []string{"geo.location.hnsw.commitlog.d"},
			wreckBucket: func(t *testing.T, s *Shard) {
				require.NoError(t, os.RemoveAll(filepath.Join(s.pathLSM(), helpers.BucketGeoFromPropNameLSM("location"))))
			},
		},
		{
			name:       "commit log and queue dirs, partial bucket",
			legacyDirs: []string{"geo.location.hnsw.commitlog.d", "geo.location.queue.d"},
			wreckBucket: func(t *testing.T, s *Shard) {
				dir := filepath.Join(s.pathLSM(), helpers.BucketGeoFromPropNameLSM("location"))
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				for _, e := range entries {
					require.NoError(t, os.RemoveAll(filepath.Join(dir, e.Name())))
				}
			},
		},
		{
			name:        "queue dir only, intact bucket",
			legacyDirs:  []string{"geo.location.queue.d"},
			wreckBucket: func(*testing.T, *Shard) {},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, idx := testGeoBucketShard(t, ctx)
			defer removeRootPath(t, idx)

			objs := []*storobj.Object{
				geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords}),
				geoTestObject(map[string]interface{}{"location": munichCoords}),
				geoTestObject(map[string]interface{}{"name": "no geo"}),
			}
			for _, obj := range objs {
				require.NoError(t, s.PutObject(ctx, obj))
			}
			deleted := geoTestObject(map[string]interface{}{"location": hamburgCoords})
			require.NoError(t, s.PutObject(ctx, deleted))
			require.NoError(t, s.DeleteObject(ctx, deleted.Object.ID, time.Time{}))

			shardPath := s.path()
			require.NoError(t, s.Shutdown(ctx))

			test.wreckBucket(t, s)
			for _, dir := range test.legacyDirs {
				require.NoError(t, os.MkdirAll(filepath.Join(shardPath, dir), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(shardPath, dir, "1000"), []byte("old"), 0o644))
			}

			reloaded, err := NewShard(ctx, nil, s.Name(), idx, geoBucketTestClass(),
				idx.centralJobQueue, idx.scheduler, idx.shardReindexer, false, idx.bitmapBufPool,
				monitoring.ShardRegistrationEager)
			require.NoError(t, err)
			defer reloaded.Shutdown(ctx)

			assert.Equal(t, []uint64{objs[0].DocID, objs[1].DocID},
				geoDocIDsWithin(t, reloaded, "location", berlinCoords, 1_000_000))
			assert.Empty(t, geoDocIDsWithin(t, reloaded, "location", hamburgCoords, 1000),
				"the deleted object must not come back")
			assert.Equal(t, []uint64{objs[0].DocID}, geoDocIDsWithin(t, reloaded, "home", munichCoords, 1000),
				"the other geo property is untouched")

			for _, dir := range test.legacyDirs {
				_, err := os.Stat(filepath.Join(shardPath, dir))
				assert.True(t, os.IsNotExist(err), "legacy dir %q must be removed", dir)
			}
		})
	}
}

func TestGeoBucketDebugReset(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)

	objs := []*storobj.Object{
		geoTestObject(map[string]interface{}{"location": berlinCoords}),
		geoTestObject(map[string]interface{}{"location": munichCoords}),
	}
	for _, obj := range objs {
		require.NoError(t, s.PutObject(ctx, obj))
	}

	// a point lost from the bucket while the object is still there
	bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM("location"))
	require.NoError(t, bucket.GeoPointsRemove(objs[0].DocID))
	require.Equal(t, []uint64{objs[1].DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))

	require.NoError(t, s.DebugResetGeoIndex(ctx, "location"))
	assert.Equal(t, []uint64{objs[0].DocID, objs[1].DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))

	require.Error(t, s.DebugResetGeoIndex(ctx, "name"), "a text property has no geo index")
	require.Error(t, s.DebugResetGeoIndex(ctx, "missing"))

	// the bucket keeps working after the reset
	later := geoTestObject(map[string]interface{}{"location": hamburgCoords})
	require.NoError(t, s.PutObject(ctx, later))
	assert.Equal(t, []uint64{objs[0].DocID, objs[1].DocID, later.DocID},
		geoDocIDsWithin(t, s, "location", berlinCoords, 1_000_000))
}

// TestShardListBackupFilesCoversGeoBucket pins that a backup carries the geo
// bucket: a restore without it answers every geo filter with nothing.
func TestShardListBackupFilesCoversGeoBucket(t *testing.T) {
	ctx := testCtx()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)

	objs := []*storobj.Object{
		geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords}),
		geoTestObject(map[string]interface{}{"location": munichCoords, "home": hamburgCoords}),
	}
	for _, obj := range objs {
		require.NoError(t, s.PutObject(ctx, obj))
	}

	// 0 timeout disables the inactivity auto-resume, so a slow test body
	// cannot resume the shard before the files are listed.
	require.NoError(t, s.HaltForTransfer(ctx, false, 0))
	files, err := s.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err)

	dstRoot := t.TempDir()
	for _, relPath := range files {
		data, err := os.ReadFile(filepath.Join(idx.Config.RootPath, relPath))
		require.NoError(t, err)
		dst := filepath.Join(dstRoot, relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		require.NoError(t, os.WriteFile(dst, data, 0o644))
	}
	relShardPath, err := filepath.Rel(idx.Config.RootPath, s.path())
	require.NoError(t, err)
	restoredLSM := filepath.Join(dstRoot, relShardPath, "lsm")

	for _, propName := range []string{"location", "home"} {
		want := geoDocIDsWithin(t, s, propName, berlinCoords, 1_000_000)
		require.Len(t, want, 2, "the live bucket of %q lost entries, so the comparison below proves nothing", propName)

		restored, err := lsmkv.NewBucketCreator().NewBucket(ctx,
			filepath.Join(restoredLSM, helpers.BucketGeoFromPropNameLSM(propName)), dstRoot,
			idx.logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			lsmkv.WithStrategy(lsmkv.StrategyGeoPoints))
		require.NoError(t, err, "the backup did not carry the geo bucket of %q", propName)
		bm, err := restored.GeoPointsWithinRange(ctx, *berlinCoords.Latitude, *berlinCoords.Longitude, 1_000_000)
		require.NoError(t, err)
		assert.ElementsMatch(t, want, bm.ToArray(), "the backup did not carry the geo points of %q", propName)
		require.NoError(t, restored.Shutdown(ctx))
	}

	require.NoError(t, s.resumeMaintenanceCycles(ctx))
}

func TestGeoCoordinatesOfProp(t *testing.T) {
	lat, lon := float32(1.5), float32(2.5)
	tests := []struct {
		name    string
		props   interface{}
		want    *models.GeoCoordinates
		wantErr bool
	}{
		{name: "no properties", props: nil},
		{name: "property missing", props: map[string]interface{}{"other": 1}},
		{name: "nil value", props: map[string]interface{}{"location": nil}},
		{name: "model type", props: map[string]interface{}{"location": geoAt(lat, lon)}, want: geoAt(lat, lon)},
		{name: "map type", props: map[string]interface{}{"location": map[string]any{"latitude": 1.5, "longitude": 2.5}}, want: geoAt(lat, lon)},
		{name: "map missing longitude", props: map[string]interface{}{"location": map[string]any{"latitude": 1.5}}, wantErr: true},
		{name: "model missing latitude", props: map[string]interface{}{"location": &models.GeoCoordinates{Longitude: &lon}}, wantErr: true},
		{name: "wrong type", props: map[string]interface{}{"location": "berlin"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			obj := &storobj.Object{Object: models.Object{Properties: test.props}}
			got, err := geoCoordinatesOfProp(obj, "location")
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if test.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, *test.want.Latitude, *got.Latitude)
			assert.Equal(t, *test.want.Longitude, *got.Longitude)
		})
	}
}

func TestGeoBucketNotCreatedWithoutFilterableIndex(t *testing.T) {
	ctx := context.Background()
	vFalse := false
	class := geoBucketTestClass()
	class.Properties[1].IndexFilterable = &vFalse

	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	defer removeRootPath(t, idx)
	s := shard.(*Shard)

	assert.False(t, s.hasGeoIndexForProp("location"))
	assert.True(t, s.hasGeoIndexForProp("home"))
	assert.True(t, s.hasGeoIndex())

	obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
	require.NoError(t, s.PutObject(ctx, obj), "a value for the unindexed property is stored, not indexed")
	assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "home", munichCoords, 1000))
}

// TestGeoBucketRemovedWhenFilterableIndexDisabled pins that turning a geo
// property's filterable index off removes its geo bucket, the way the other
// index types lose theirs.
func TestGeoBucketRemovedWhenFilterableIndexDisabled(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)

	obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
	require.NoError(t, s.PutObject(ctx, obj))
	require.True(t, s.hasGeoIndexForProp("location"))

	vFalse := false
	disabled := &models.Property{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString(), IndexFilterable: &vFalse}
	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	s.updatePropertyBuckets(ctx, eg, disabled, &migrationSweepCounts{})
	require.NoError(t, eg.Wait())

	assert.False(t, s.hasGeoIndexForProp("location"))
	_, err := os.Stat(filepath.Join(s.pathLSM(), helpers.BucketGeoFromPropNameLSM("location")))
	assert.True(t, os.IsNotExist(err), "the geo bucket directory must be gone")
	assert.True(t, s.hasGeoIndexForProp("home"), "the other geo property keeps its bucket")
	assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "home", munichCoords, 1000))
}

func TestMainBucketForProperty(t *testing.T) {
	tests := []struct {
		name      string
		prop      *models.Property
		indexType string
		want      string
		wantOK    bool
	}{
		{name: "geo filterable", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString()}, indexType: "filterable", want: helpers.BucketGeoFromPropNameLSM("p"), wantOK: true},
		{name: "geo searchable has no bucket of its own", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString()}, indexType: "searchable", want: helpers.BucketSearchableFromPropNameLSM("p"), wantOK: true},
		{name: "text filterable", prop: &models.Property{Name: "p", DataType: schema.DataTypeText.PropString()}, indexType: "filterable", want: helpers.BucketFromPropNameLSM("p"), wantOK: true},
		{name: "unknown index type", prop: &models.Property{Name: "p", DataType: schema.DataTypeText.PropString()}, indexType: "nope"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := mainBucketForProperty(test.prop, test.indexType)
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.want, got)
		})
	}
}
