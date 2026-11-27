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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestGeoHNSWRemovedWhenFilterableIndexDisabled pins that turning off the
// filterable index of a geo property with the HNSW geo index removes that
// index and its queue, from the shard and from disk, the way
// TestGeoBucketRemovedWhenFilterableIndexDisabled does for a bucket. The shard
// keeps taking writes, deletes and a shutdown afterwards.
func TestGeoHNSWRemovedWhenFilterableIndexDisabled(t *testing.T) {
	tests := []struct {
		name         string
		geoIndexType string
		async        bool
	}{
		{name: "type unset"},
		{name: "hnsw", geoIndexType: models.PropertyGeoIndexTypeHnsw},
		{name: "hnsw with async indexing", geoIndexType: models.PropertyGeoIndexTypeHnsw, async: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			class := geoBucketTestClass()
			class.Properties[1].GeoIndexType = test.geoIndexType
			class.Properties[2].GeoIndexType = test.geoIndexType
			shard, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, test.async)
			defer removeGeoTestRoot(t, idx)
			s := shard.(*Shard)

			obj := geoTestObject(map[string]interface{}{"location": berlinCoords, "home": munichCoords})
			require.NoError(t, s.PutObject(ctx, obj))
			require.True(t, s.hasGeoIndexForProp("location"))
			for _, dir := range geoHNSWDirs(s.path(), "location")[:1] {
				require.DirExists(t, dir)
			}

			vFalse := false
			disabled := &models.Property{
				Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString(),
				GeoIndexType: test.geoIndexType, IndexFilterable: &vFalse,
			}
			eg := enterrors.NewErrorGroupWrapper(idx.logger)
			s.updatePropertyBuckets(ctx, eg, disabled, &migrationSweepCounts{})
			require.NoError(t, eg.Wait())

			assert.False(t, s.hasGeoIndexForProp("location"), "the geo hnsw is still registered and still written to")
			require.NoError(t, s.ForEachGeoQueue(func(propName string, _ *VectorIndexQueue) error {
				assert.NotEqual(t, "location", propName, "the geo queue is still registered")
				return nil
			}))
			for _, dir := range geoHNSWDirs(s.path(), "location") {
				assert.NoDirExists(t, dir)
			}
			assert.True(t, s.hasGeoIndexForProp("home"), "the other geo property keeps its index")
			for _, dir := range geoHNSWDirs(s.path(), "home")[:1] {
				assert.DirExists(t, dir)
			}

			// repeating the removal, as a retried apply does, is a no-op
			eg = enterrors.NewErrorGroupWrapper(idx.logger)
			s.updatePropertyBuckets(ctx, eg, disabled, &migrationSweepCounts{})
			require.NoError(t, eg.Wait())

			later := geoTestObject(map[string]interface{}{"location": hamburgCoords, "home": hamburgCoords})
			require.NoError(t, s.PutObject(ctx, later))
			for _, err := range s.PutObjectBatch(ctx, []*storobj.Object{
				geoTestObject(map[string]interface{}{"location": hamburgCoords, "home": hamburgCoords}),
			}) {
				require.NoError(t, err)
			}
			require.NoError(t, s.DeleteObject(ctx, obj.ID(), time.Now()))
			for _, dir := range geoHNSWDirs(s.path(), "location") {
				assert.NoDirExists(t, dir, "a later write recreated the dropped index")
			}
			require.NoError(t, s.Shutdown(ctx))
		})
	}
}

// TestGeoHNSWDirsSweptAtStartup pins that a shard which missed the removal (an
// inactive tenant, a failed apply, a version that never removed them) loses the
// leftover geo graph directories when it loads, and nothing else.
func TestGeoHNSWDirsSweptAtStartup(t *testing.T) {
	geo := schema.DataTypeGeoCoordinates.PropString()
	vFalse, vTrue := false, true
	tests := []struct {
		name     string
		prop     *models.Property
		wantKept bool
	}{
		{name: "filterable removed, type unset", prop: &models.Property{Name: "location", DataType: geo, IndexFilterable: &vFalse}},
		{name: "filterable removed, hnsw", prop: &models.Property{Name: "location", DataType: geo, GeoIndexType: models.PropertyGeoIndexTypeHnsw, IndexFilterable: &vFalse}},
		{name: "filterable on", prop: &models.Property{Name: "location", DataType: geo, IndexFilterable: &vTrue}, wantKept: true},
		{name: "filterable defaulted", prop: &models.Property{Name: "location", DataType: geo}, wantKept: true},
		{name: "filterable removed, bkd", prop: &models.Property{Name: "location", DataType: geo, GeoIndexType: models.PropertyGeoIndexTypeBkd, IndexFilterable: &vFalse}, wantKept: true},
		{name: "filterable removed, text property", prop: &models.Property{Name: "location", DataType: schema.DataTypeText.PropString(), IndexFilterable: &vFalse}, wantKept: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexPath, shardName := t.TempDir(), "shard1"
			shardDir := filepath.Join(indexPath, shardName)
			dirs := append(geoHNSWDirs(shardDir, "location"), geoHNSWDirs(shardDir, "home")...)
			for _, dir := range dirs {
				require.NoError(t, os.MkdirAll(dir, 0o777))
			}
			class := &models.Class{Class: "C", Properties: []*models.Property{
				test.prop,
				{Name: "home", DataType: geo},
			}}

			require.NoError(t, newPropertyDeleteIndexHelper().ensureBucketsAreRemovedForNonExistentPropertyIndexes(
				indexPath, shardName, class, func(string, string) bool { return false }))

			for _, dir := range geoHNSWDirs(shardDir, "location") {
				if test.wantKept {
					assert.DirExists(t, dir)
				} else {
					assert.NoDirExists(t, dir)
				}
			}
			for _, dir := range geoHNSWDirs(shardDir, "home") {
				assert.DirExists(t, dir)
			}
		})
	}
}
