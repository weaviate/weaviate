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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// A backup that misses the geo commit log restores an empty geo index. An empty
// geo index answers every withinGeoRange filter with zero results and no error.
func TestShardListBackupFilesCoversGeoIndex(t *testing.T) {
	var (
		berlin  = geoAt(52.520, 13.405)
		munich  = geoAt(48.137, 11.575)
		hamburg = geoAt(53.551, 9.993)
	)

	tests := []struct {
		name    string
		props   []string
		objects []map[string]*models.GeoCoordinates
	}{
		{
			name:    "no geo property",
			objects: []map[string]*models.GeoCoordinates{{}},
		},
		{
			name:    "one object",
			props:   []string{"location"},
			objects: []map[string]*models.GeoCoordinates{{"location": berlin}},
		},
		{
			name:  "several objects",
			props: []string{"location"},
			objects: []map[string]*models.GeoCoordinates{
				{"location": berlin},
				{"location": munich},
				{"location": hamburg},
			},
		},
		{
			name:  "two geo properties",
			props: []string{"location", "office"},
			objects: []map[string]*models.GeoCoordinates{
				{"location": berlin, "office": munich},
				{"location": munich, "office": hamburg},
			},
		},
		{
			name:  "geo property without any object",
			props: []string{"location"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()

			class := &models.Class{Class: "GeoBackup"}
			for _, propName := range tc.props {
				class.Properties = append(class.Properties, &models.Property{
					Name:     propName,
					DataType: schema.DataTypeGeoCoordinates.PropString(),
				})
			}

			shardLike, index := testShardWithSettings(t, ctx, class,
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shardLike.(*Shard)

			indexedPerProp := map[string]int{}
			for _, coordinatesPerProp := range tc.objects {
				props := make(map[string]interface{}, len(coordinatesPerProp))
				for propName, coordinates := range coordinatesPerProp {
					props[propName] = coordinates
					indexedPerProp[propName]++
				}
				require.NoError(t, shard.PutObject(ctx, &storobj.Object{
					MarshallerVersion: 1,
					Object: models.Object{
						ID:         strfmt.UUID(uuid.NewString()),
						Class:      class.Class,
						Properties: props,
					},
				}))
			}

			// 0 timeout disables the inactivity auto-resume, so a slow test body
			// cannot resume the shard before the files are listed.
			require.NoError(t, shard.HaltForTransfer(ctx, "test:geo-backup", false, 0))
			files, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
			require.NoError(t, err)

			restoredShardPath := restoreBackupFiles(t, index.Config.RootPath, shard.path(), files)

			for _, propName := range tc.props {
				want := allWithinRange(t, ctx, liveGeoIndex(t, shard, propName))
				require.Len(t, want, indexedPerProp[propName],
					"the live geo index for %q lost entries, so the comparison below proves nothing", propName)

				restored := openGeoIndexAt(t, ctx, shard, propName, restoredShardPath)
				require.ElementsMatch(t, want, allWithinRange(t, ctx, restored),
					"the backup did not carry the geo index of %q", propName)
			}

			require.NoError(t, shard.resumeMaintenanceCycles(ctx, "test:geo-backup"))
		})
	}
}

func geoAt(latitude, longitude float32) *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: &latitude, Longitude: &longitude}
}

// restoreBackupFiles copies the listed files into a fresh root, the way a
// restore lands them on a new node, and returns the restored shard's directory.
func restoreBackupFiles(t *testing.T, srcRoot, shardPath string, files []string) string {
	t.Helper()

	dstRoot := t.TempDir()
	for _, relPath := range files {
		data, err := os.ReadFile(filepath.Join(srcRoot, relPath))
		require.NoError(t, err)

		dst := filepath.Join(dstRoot, relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		require.NoError(t, os.WriteFile(dst, data, 0o644))
	}

	relShardPath, err := filepath.Rel(srcRoot, shardPath)
	require.NoError(t, err)
	return filepath.Join(dstRoot, relShardPath)
}

func liveGeoIndex(t *testing.T, s *Shard, propName string) *geo.Index {
	t.Helper()

	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	index, ok := s.propertyIndices.ByProp(propName)
	require.True(t, ok, "shard has no geo index for %q", propName)
	return index.GeoIndex
}

// openGeoIndexAt reads a geo index back from the commit log under rootPath. It
// takes its coordinates from the live shard's objects, which a restore gets
// from the LSM buckets it lands alongside the commit log.
func openGeoIndexAt(t *testing.T, ctx context.Context, s *Shard, propName, rootPath string) *geo.Index {
	t.Helper()

	index, err := geo.NewIndex(geo.Config{
		ID:               geoPropID(propName),
		RootPath:         rootPath,
		CoordinatesForID: s.makeCoordinatesForID(propName),
		Logger:           s.index.logger,
		AllocChecker:     memwatch.NewDummyMonitor(),
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })

	index.PostStartup(ctx)
	return index
}

// allWithinRange returns every doc id in the index, through a range wide enough
// to span the planet.
func allWithinRange(t *testing.T, ctx context.Context, index *geo.Index) []uint64 {
	t.Helper()

	const everywhere = 1e8 // metres

	ids, err := index.WithinRange(ctx, filters.GeoRange{
		GeoCoordinates: geoAt(0, 0),
		Distance:       everywhere,
	})
	require.NoError(t, err)
	return ids
}
