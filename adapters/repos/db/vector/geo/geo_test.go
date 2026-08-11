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

package geo

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestGeoJourney(t *testing.T) {
	ctx := context.Background()
	elements := []models.GeoCoordinates{
		{ // coordinates of munich
			Latitude:  ptFloat32(48.13743),
			Longitude: ptFloat32(11.57549),
		},
		{ // coordinates of stuttgart
			Latitude:  ptFloat32(48.78232),
			Longitude: ptFloat32(9.17702),
		},
	}

	getCoordinates := func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
		return &elements[id], nil
	}

	geoIndex, err := NewIndex(Config{
		AllocChecker:       memwatch.NewDummyMonitor(),
		ID:                 "unit-test",
		CoordinatesForID:   getCoordinates,
		DisablePersistence: true,
		RootPath:           t.TempDir(),
	},
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	t.Run("importing all", func(t *testing.T) {
		for id, coordinates := range elements {
			err := geoIndex.Add(ctx, uint64(id), &coordinates)
			require.Nil(t, err)
		}
	})

	t.Run("importing an invalid object", func(t *testing.T) {
		err := geoIndex.Add(ctx, 9000, &models.GeoCoordinates{})
		assert.Equal(t, "invalid arguments: latitude must be set", err.Error())
	})

	km := float32(1000)
	t.Run("searching missing longitude", func(t *testing.T) {
		_, err := geoIndex.WithinRange(context.Background(), filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{
				Latitude: ptFloat32(48.13743),
			},
			Distance: 300 * km,
		})
		assert.Equal(t, "invalid arguments: longitude must be set", err.Error())
	})

	t.Run("searching missing latitude", func(t *testing.T) {
		_, err := geoIndex.WithinRange(context.Background(), filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{
				Longitude: ptFloat32(11.57549),
			},
			Distance: 300 * km,
		})
		assert.Equal(t, "invalid arguments: latitude must be set", err.Error())
	})

	t.Run("searching within 500km of munich", func(t *testing.T) {
		// should return both cities, with munich first and stuttgart second
		results, err := geoIndex.WithinRange(context.Background(), filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{
				Latitude:  ptFloat32(48.13743),
				Longitude: ptFloat32(11.57549),
			},
			Distance: 500 * km,
		})
		require.Nil(t, err)

		expectedResults := []uint64{0, 1}
		assert.Equal(t, expectedResults, results)
	})

	t.Run("searching within 10km of munich", func(t *testing.T) {
		// should return both cities, with munich first and stuttgart second
		results, err := geoIndex.WithinRange(context.Background(), filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{
				Latitude:  ptFloat32(48.13743),
				Longitude: ptFloat32(11.57549),
			},
			Distance: 10 * km,
		})
		require.Nil(t, err)

		expectedResults := []uint64{0}
		assert.Equal(t, expectedResults, results)
	})
}

// A geo index that leaves its cache maximum unset reloads every coordinate
// from disk every few seconds.
func TestGeoVectorCacheSurvivesDeletionCycle(t *testing.T) {
	ctx := context.Background()
	elements := []models.GeoCoordinates{
		{ // coordinates of munich
			Latitude:  ptFloat32(48.13743),
			Longitude: ptFloat32(11.57549),
		},
		{ // coordinates of stuttgart
			Latitude:  ptFloat32(48.78232),
			Longitude: ptFloat32(9.17702),
		},
	}

	var coordinateLoads atomic.Int64
	getCoordinates := func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
		coordinateLoads.Add(1)
		return &elements[id], nil
	}

	geoIndex, err := NewIndex(Config{
		AllocChecker:       memwatch.NewDummyMonitor(),
		ID:                 "unit-test-vector-cache",
		CoordinatesForID:   getCoordinates,
		DisablePersistence: true,
		RootPath:           t.TempDir(),
	},
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, geoIndex.Shutdown(ctx)) })

	for id := range elements {
		require.NoError(t, geoIndex.Add(ctx, uint64(id), &elements[id]))
	}

	searchAroundMunich := func() []uint64 {
		results, err := geoIndex.WithinRange(ctx, filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{
				Latitude:  ptFloat32(48.13743),
				Longitude: ptFloat32(11.57549),
			},
			Distance: 500 * 1000,
		})
		require.NoError(t, err)
		return results
	}

	require.Equal(t, []uint64{0, 1}, searchAroundMunich())
	loadsAfterWarmup := coordinateLoads.Load()

	time.Sleep(2 * cache.DefaultDeletionInterval)

	require.Equal(t, []uint64{0, 1}, searchAroundMunich())
	require.Equal(t, loadsAfterWarmup, coordinateLoads.Load(),
		"searching after a deletion cycle must not reload coordinates")
}

func TestGeoConfig(t *testing.T) {
	cfg := Config{}
	require.Equal(t, 800, cfg.hnswEF())
	cfg = Config{HNSWEF: 0}
	require.Equal(t, 800, cfg.hnswEF())
	cfg = Config{HNSWEF: 1900}
	require.Equal(t, 1900, cfg.hnswEF())
}

func ptFloat32(in float32) *float32 {
	return &in
}
