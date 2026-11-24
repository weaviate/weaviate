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
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

var (
	berlinCoords  = geoAt(52.520, 13.405)
	munichCoords  = geoAt(48.137, 11.575)
	hamburgCoords = geoAt(53.551, 9.993)
)

func removeGeoTestRoot(t *testing.T, idx *Index) {
	t.Helper()
	require.NoError(t, os.RemoveAll(idx.Config.RootPath))
}

func geoBucketTestClass() *models.Class {
	return &models.Class{
		Class: geoBucketClass,
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
			{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd},
			{Name: "home", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd},
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
		defer removeGeoTestRoot(t, idx)

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
		defer removeGeoTestRoot(t, idx)

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
		defer removeGeoTestRoot(t, idx)

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
		defer removeGeoTestRoot(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": map[string]any{
			"latitude": float64(*berlinCoords.Latitude), "longitude": float64(*berlinCoords.Longitude),
		}})
		require.NoError(t, s.PutObject(ctx, obj))
		assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	})

	t.Run("batch put indexes and batch update retires", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeGeoTestRoot(t, idx)

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
		defer removeGeoTestRoot(t, idx)

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
			defer removeGeoTestRoot(t, idx)

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

func TestGeoBucketDebugReset(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeGeoTestRoot(t, idx)

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
	defer removeGeoTestRoot(t, idx)

	objs := []*storobj.Object{
		geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords}),
		geoTestObject(map[string]interface{}{"location": munichCoords, "home": hamburgCoords}),
	}
	for _, obj := range objs {
		require.NoError(t, s.PutObject(ctx, obj))
	}

	require.NoError(t, s.HaltForTransfer(ctx, false))
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

func TestGeoPointOfProp(t *testing.T) {
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
			got, err := geoPointOfProp(obj, "location")
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
	defer removeGeoTestRoot(t, idx)
	s := shard.(*Shard)

	assert.False(t, s.hasGeoIndexForProp("location"))
	assert.True(t, s.hasGeoIndexForProp("home"))

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
	defer removeGeoTestRoot(t, idx)

	obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
	require.NoError(t, s.PutObject(ctx, obj))
	require.True(t, s.hasGeoIndexForProp("location"))

	vFalse := false
	disabled := &models.Property{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd, IndexFilterable: &vFalse}
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
		{name: "geo filterable", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd}, indexType: "filterable", want: helpers.BucketGeoFromPropNameLSM("p"), wantOK: true},
		{name: "geo searchable has no bucket of its own", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd}, indexType: "searchable", want: helpers.BucketSearchableFromPropNameLSM("p"), wantOK: true},
		{name: "geo filterable, hnsw", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString(), GeoIndexType: models.PropertyGeoIndexTypeHnsw}, indexType: "filterable", want: helpers.BucketFromPropNameLSM("p"), wantOK: true},
		{name: "geo filterable, type unset", prop: &models.Property{Name: "p", DataType: schema.DataTypeGeoCoordinates.PropString()}, indexType: "filterable", want: helpers.BucketFromPropNameLSM("p"), wantOK: true},
		{name: "text filterable with a stray geo index type", prop: &models.Property{Name: "p", DataType: schema.DataTypeText.PropString(), GeoIndexType: models.PropertyGeoIndexTypeBkd}, indexType: "filterable", want: helpers.BucketFromPropNameLSM("p"), wantOK: true},
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

// TestGeoBucketLateWriteAfterDelete pins that a geo write for a doc ID that
// a delete or update has retired cannot bring it back, in the memtable or
// after a flush. The write path does the geo write under the object's lock, so
// an insert's write cannot run after its object's delete; the bucket letting a
// removal stand over a later add is the backstop, exercised here by repeating
// the insert's geo write after the delete.
func TestGeoBucketLateWriteAfterDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("after a delete", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeGeoTestRoot(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords})
		require.NoError(t, s.PutObject(ctx, obj))
		require.NoError(t, s.DeleteObject(ctx, obj.Object.ID, time.Time{}))

		// the straggling geo write of the insert
		require.NoError(t, s.updateGeoBuckets(ctx, obj, objectInsertStatus{docID: obj.DocID}))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
		require.NoError(t, s.store.FlushMemtables(ctx))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	})

	t.Run("after an update", func(t *testing.T) {
		s, idx := testGeoBucketShard(t, ctx)
		defer removeGeoTestRoot(t, idx)

		obj := geoTestObject(map[string]interface{}{"location": berlinCoords})
		require.NoError(t, s.PutObject(ctx, obj))
		updated := geoTestObject(map[string]interface{}{"location": munichCoords})
		updated.Object.ID = obj.Object.ID
		require.NoError(t, s.PutObject(ctx, updated))
		require.NotEqual(t, obj.DocID, updated.DocID)

		require.NoError(t, s.updateGeoBuckets(ctx, obj, objectInsertStatus{docID: obj.DocID}))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
		assert.Equal(t, []uint64{updated.DocID}, geoDocIDsWithin(t, s, "location", munichCoords, 1000))
		require.NoError(t, s.store.FlushMemtables(ctx))
		assert.Empty(t, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	})
}

// TestGeoBucketConcurrentPutDelete hammers a few objects with concurrent puts,
// updates and deletes and checks that the geo bucket ends up holding exactly
// the doc IDs of the objects that survive, which only holds if every geo write
// is ordered with the object write it belongs to.
func TestGeoBucketConcurrentPutDelete(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeGeoTestRoot(t, idx)

	const objects, workers, rounds = 8, 8, 150
	ids := make([]strfmt.UUID, objects)
	for i := range ids {
		ids[i] = strfmt.UUID(uuid.NewString())
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		r := rand.New(rand.NewSource(int64(w)))
		wg.Go(func() {
			for i := 0; i < rounds; i++ {
				id := ids[r.Intn(objects)]
				if r.Intn(3) == 0 {
					require.NoError(t, s.DeleteObject(ctx, id, time.Time{}))
					continue
				}
				obj := geoTestObject(map[string]interface{}{"location": geoAt(52+float32(r.Intn(3)), 13)})
				obj.Object.ID = id
				require.NoError(t, s.PutObject(ctx, obj))
			}
		})
	}
	wg.Wait()

	want := []uint64{}
	for _, id := range ids {
		obj, err := s.ObjectByID(ctx, id, nil, additional.Properties{})
		require.NoError(t, err)
		if obj != nil {
			want = append(want, obj.DocID)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	assert.Equal(t, want, geoDocIDsWithin(t, s, "location", geoAt(53, 13), 500_000),
		"the geo bucket must hold the doc IDs of the live objects and nothing else")
}

// TestGeoBucketNameDoesNotCollide pins that the geo bucket of "location" is
// not the filterable bucket of a property named "location_geo": a schema may
// hold both, and a shared directory would have the geo rebuild remove the
// other property's index.
func TestGeoBucketNameDoesNotCollide(t *testing.T) {
	require.NotEqual(t, helpers.BucketFromPropNameLSM("location_geo"), helpers.BucketGeoFromPropNameLSM("location"))

	ctx := context.Background()
	class := geoBucketTestClass()
	class.Properties = append(class.Properties, &models.Property{
		Name: "location_geo", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationField,
	})
	shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	defer removeGeoTestRoot(t, idx)
	s := shard.(*Shard)

	obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "location_geo": "berlin"})
	require.NoError(t, s.PutObject(ctx, obj))

	textFilter := func() []uint64 {
		filter := &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On:       &filters.Path{Class: schema.ClassName(geoBucketClass), Property: "location_geo"},
			Value:    &filters.Value{Value: "berlin", Type: schema.DataTypeText},
		}}
		allow, err := s.buildAllowList(ctx, filter, additional.Properties{})
		require.NoError(t, err)
		return allow.Slice()
	}
	assert.Equal(t, []uint64{obj.DocID}, textFilter())
	assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))

	// the geo rebuild replaces its own bucket only
	require.NoError(t, s.DebugResetGeoIndex(ctx, "location"))
	assert.Equal(t, []uint64{obj.DocID}, textFilter(), "the text property's index must survive the geo rebuild")
	assert.Equal(t, []uint64{obj.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	textBucket := s.store.Bucket(helpers.BucketFromPropNameLSM("location_geo"))
	require.NotNil(t, textBucket)
	assert.Equal(t, lsmkv.StrategyRoaringSet, textBucket.Strategy())
}

// TestGeoBucketRebuildAfterChurn pins that the rebuild indexes the live
// objects after many versions of them were written and most were deleted.
func TestGeoBucketRebuildAfterChurn(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeGeoTestRoot(t, idx)

	kept := geoTestObject(map[string]interface{}{"location": berlinCoords})
	for i := 0; i < 50; i++ {
		// each put is a new version and a new doc ID
		kept.Object.Properties = map[string]interface{}{"location": geoAt(52.52, 13.405+float32(i)*0.0001)}
		require.NoError(t, s.PutObject(ctx, kept))
		churn := geoTestObject(map[string]interface{}{"location": munichCoords})
		require.NoError(t, s.PutObject(ctx, churn))
		require.NoError(t, s.DeleteObject(ctx, churn.Object.ID, time.Time{}))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))

	require.NoError(t, s.DebugResetGeoIndex(ctx, "location"))
	assert.Equal(t, []uint64{kept.DocID}, geoDocIDsWithin(t, s, "location", berlinCoords, 1000))
	assert.Empty(t, geoDocIDsWithin(t, s, "location", munichCoords, 1000))
}

// TestGeoIndexTypeSelectsIndex pins that geoIndexType alone decides which
// index a geo property gets: a bucket for "bkd", the HNSW graph otherwise, and
// never both. Each serves its own property through every write path while the
// other kind sits beside it in the same class.
func TestGeoIndexTypeSelectsIndex(t *testing.T) {
	tests := []struct {
		name         string
		geoIndexType string
		wantBucket   bool
	}{
		{name: "unset defaults to hnsw", geoIndexType: "", wantBucket: false},
		{name: "hnsw", geoIndexType: models.PropertyGeoIndexTypeHnsw, wantBucket: false},
		{name: "bkd", geoIndexType: models.PropertyGeoIndexTypeBkd, wantBucket: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			// "location" is under test; "home" is always the other kind
			otherType := models.PropertyGeoIndexTypeBkd
			if test.wantBucket {
				otherType = models.PropertyGeoIndexTypeHnsw
			}
			class := geoBucketTestClass()
			class.Properties[1].GeoIndexType = test.geoIndexType
			class.Properties[2].GeoIndexType = otherType

			shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
			defer removeGeoTestRoot(t, idx)
			s := shard.(*Shard)

			for propName, wantBucket := range map[string]bool{"location": test.wantBucket, "home": !test.wantBucket} {
				assert.Equal(t, wantBucket, s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName)) != nil,
					"geo bucket of %q", propName)
				_, hasGraph := s.propertyIndicesSnapshot().ByProp(propName)
				assert.Equal(t, !wantBucket, hasGraph, "geo hnsw of %q", propName)
				_, err := os.Stat(filepath.Join(s.path(), "geo."+propName+".hnsw.commitlog.d"))
				assert.Equal(t, !wantBucket, err == nil, "geo hnsw commit log of %q", propName)
				assert.True(t, s.hasGeoIndexForProp(propName))
			}
			assert.True(t, s.hasGeoIndex(), "the hnsw property keeps the shard on the property-specific write path")

			both := func(coords *models.GeoCoordinates) map[string]interface{} {
				return map[string]interface{}{"location": coords, "home": coords}
			}
			within := func(propName string, center *models.GeoCoordinates) []uint64 {
				return geoFilterDocIDs(t, s, propName, center, 1000)
			}

			put := geoTestObject(both(berlinCoords))
			require.NoError(t, s.PutObject(ctx, put))
			batched := geoTestObject(both(berlinCoords))
			for _, err := range s.PutObjectBatch(ctx, []*storobj.Object{batched}) {
				require.NoError(t, err)
			}
			for _, propName := range []string{"location", "home"} {
				assert.Equal(t, []uint64{put.DocID, batched.DocID}, within(propName, berlinCoords), propName)
				assert.Empty(t, within(propName, munichCoords), propName)
			}

			moved := geoTestObject(both(munichCoords))
			moved.Object.ID = put.Object.ID
			require.NoError(t, s.PutObject(ctx, moved))
			for _, propName := range []string{"location", "home"} {
				assert.Equal(t, []uint64{batched.DocID}, within(propName, berlinCoords), propName)
				assert.Equal(t, []uint64{moved.DocID}, within(propName, munichCoords), propName)
			}

			require.NoError(t, s.DeleteObject(ctx, moved.ID(), time.Now()))
			for _, propName := range []string{"location", "home"} {
				assert.Empty(t, within(propName, munichCoords), propName)
				assert.Equal(t, []uint64{batched.DocID}, within(propName, berlinCoords), propName)
			}
		})
	}
}
