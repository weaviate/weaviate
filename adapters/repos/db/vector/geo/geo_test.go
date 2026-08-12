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
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	// aliased so the tables below can keep naming their loop variable "test"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
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

func TestCoordinatesFromObjectVectorFromObject(t *testing.T) {
	readErr := errors.New("cannot read object")

	tests := []struct {
		name        string
		coordinates *models.GeoCoordinates
		err         error
		want        []float32
		wantErr     error
		wantErrMsg  string
	}{
		{
			name:        "coordinates become a lat/lon vector",
			coordinates: &models.GeoCoordinates{Latitude: ptFloat32(48.13743), Longitude: ptFloat32(11.57549)},
			want:        []float32{48.13743, 11.57549},
		},
		{
			// the prefill scan takes a nil vector as "skip this object"
			name:        "object without coordinates yields no vector",
			coordinates: nil,
		},
		{
			name:       "read failure is passed on",
			err:        readErr,
			wantErr:    readErr,
			wantErrMsg: "cannot read object",
		},
		{
			name:        "coordinates without latitude",
			coordinates: &models.GeoCoordinates{Longitude: ptFloat32(11.57549)},
			wantErrMsg:  "latitude must be set",
		},
		{
			name:        "coordinates without longitude",
			coordinates: &models.GeoCoordinates{Latitude: ptFloat32(48.13743)},
			wantErrMsg:  "longitude must be set",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var from CoordinatesFromObject = func([]byte) (*models.GeoCoordinates, error) {
				return test.coordinates, test.err
			}

			vec, err := from.VectorFromObject([]byte("ignored"))

			if test.wantErrMsg != "" {
				require.ErrorContains(t, err, test.wantErrMsg)
				if test.wantErr != nil {
					require.ErrorIs(t, err, test.wantErr)
				}
				require.Nil(t, vec)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, vec)
		})
	}
}

// A store without a decoder would let the prefill scan cache each object's own
// vector as if it were a coordinate, so NewIndex must refuse the pair.
func TestNewIndexStoreRequiresCoordinatesFromObject(t *testing.T) {
	newIndex := func(t *testing.T, store *lsmkv.Store, from CoordinatesFromObject) (*Index, error) {
		t.Helper()
		return NewIndex(Config{
			AllocChecker:          memwatch.NewDummyMonitor(),
			ID:                    "unit-test",
			CoordinatesForID:      func(context.Context, uint64) (*models.GeoCoordinates, error) { return nil, nil },
			CoordinatesFromObject: from,
			Store:                 store,
			DisablePersistence:    true,
			RootPath:              t.TempDir(),
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	}
	decode := CoordinatesFromObject(func([]byte) (*models.GeoCoordinates, error) { return nil, nil })

	tests := []struct {
		name    string
		store   func(t testing.TB) *lsmkv.Store
		decode  CoordinatesFromObject
		wantErr string
	}{
		{
			name:    "store without a decoder is rejected",
			store:   testinghelpers.NewDummyStore,
			wantErr: "coordinatesFromObject is required alongside a store",
		},
		{
			name:   "store with a decoder is accepted",
			store:  testinghelpers.NewDummyStore,
			decode: decode,
		},
		{
			name:  "no store needs no decoder",
			store: func(testing.TB) *lsmkv.Store { return nil },
		},
		{
			name:   "decoder without a store is unused, not an error",
			store:  func(testing.TB) *lsmkv.Store { return nil },
			decode: decode,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx, err := newIndex(t, test.store(t), test.decode)

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, idx)
			require.NoError(t, idx.Shutdown(context.Background()))
		})
	}
}

// A persisting geo index builds a commit logger, which dereferences the logger
// while opening its files.
func TestNewIndexWithoutLogger(t *testing.T) {
	idx, err := NewIndex(Config{
		AllocChecker:     memwatch.NewDummyMonitor(),
		ID:               "unit-test-no-logger",
		RootPath:         t.TempDir(),
		CoordinatesForID: func(context.Context, uint64) (*models.GeoCoordinates, error) { return nil, nil },
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	require.NoError(t, idx.Shutdown(context.Background()))
}

// putGeoObject stores an object carrying propName, marshalled the way the write
// path does, under a unique sortable key.
func putGeoObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64, propName string,
	coordinates *models.GeoCoordinates,
) {
	t.Helper()

	obj := storobj.New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", docID)),
		Class:      "Test",
		Properties: map[string]interface{}{propName: coordinates},
	}
	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[8:], docID)
	require.NoError(t, bucket.Put(key, data))
}

func coordinatesFromObjectProp(propName string) CoordinatesFromObject {
	propExtraction := storobj.NewPropExtraction().Add(propName)

	return func(objectBytes []byte) (*models.GeoCoordinates, error) {
		obj, err := storobj.FromBinaryOptionalNetwork(objectBytes, additional.Properties{}, propExtraction)
		if err != nil {
			return nil, err
		}

		props, ok := obj.Properties().(map[string]interface{})
		if !ok {
			return nil, nil
		}
		coordinates, ok := props[propName].(*models.GeoCoordinates)
		if !ok {
			return nil, nil
		}
		return coordinates, nil
	}
}

// TestGeoPrefillReadsCoordinatesFromObjectsBucket restarts a persisted geo index
// and requires its cache to come back warm off a scan of the objects bucket. The
// by-id reader fails on the restarted index, so any coordinate the scan missed
// surfaces as a read error instead of a silent fallback to random seeks.
func TestGeoPrefillReadsCoordinatesFromObjectsBucket(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	// enough objects, flushed to a segment, for QuantileKeys to seed several
	// cursors — a memtable-only bucket yields no seeds and scans single-threaded
	const docs = 2000
	coordinates := make([]*models.GeoCoordinates, docs)
	for i := range coordinates {
		coordinates[i] = &models.GeoCoordinates{
			Latitude:  ptFloat32(48.13743 + float32(i)*0.001),
			Longitude: ptFloat32(11.57549 + float32(i)*0.002),
		}
	}

	store := testinghelpers.NewDummyStore(t)
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	for id, c := range coordinates {
		putGeoObject(t, store.Bucket(helpers.ObjectsBucketLSM), uint64(id), "location", c)
	}
	require.NoError(t, store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

	newGeoIndex := func(t *testing.T, forID CoordinatesForID) *Index {
		t.Helper()
		idx, err := NewIndex(Config{
			AllocChecker:          memwatch.NewDummyMonitor(),
			ID:                    "unit-test-prefill",
			RootPath:              rootPath,
			CoordinatesForID:      forID,
			Store:                 store,
			CoordinatesFromObject: coordinatesFromObjectProp("location"),
			WaitForCachePrefill:   true,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		return idx
	}

	built := newGeoIndex(t, func(_ context.Context, id uint64) (*models.GeoCoordinates, error) {
		return coordinates[id], nil
	})
	for id, c := range coordinates {
		require.NoError(t, built.Add(ctx, uint64(id), c))
	}
	require.NoError(t, built.Flush())
	require.NoError(t, built.Shutdown(ctx))

	var byIDLoads atomic.Int64
	restarted := newGeoIndex(t, func(_ context.Context, id uint64) (*models.GeoCoordinates, error) {
		byIDLoads.Add(1)
		return nil, errors.New("coordinate must come from the prefilled cache")
	})
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown(ctx)) })

	// restoring the graph probes the entrypoint by id to learn the dimensions,
	// which is not what this test is counting
	byIDLoads.Store(0)
	restarted.PostStartup(ctx)

	// asserting per doc rather than through a search: a search only visits the
	// nodes its descent happens to reach, which would leave gaps unnoticed
	cached, ok := restarted.UnderlyingVectorIndex().(interface {
		Get(id uint64) ([]float32, error)
	})
	require.True(t, ok)
	for id, c := range coordinates {
		vec, err := cached.Get(uint64(id))
		require.NoErrorf(t, err, "doc id %d was not prefilled", id)
		require.Equal(t, []float32{*c.Latitude, *c.Longitude}, vec)
	}
	require.Zero(t, byIDLoads.Load(),
		"the objects-bucket scan must cover every coordinate, leaving no by-id lookup")
}

// Every geo prop of a shard logs through that shard's one logger, and the
// underlying index leaves its target vector empty, so class and shard alone
// would not say which property a line came from.
func TestNewIndexTagsLogLines(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	const className, shardName = "Article", "shard-a1b2c3"
	ids := []string{"geo.location", "geo.office"}

	for _, id := range ids {
		idx, err := NewIndex(Config{
			AllocChecker: memwatch.NewDummyMonitor(),
			ID:           id,
			RootPath:     t.TempDir(),
			Logger:       logger,
			ClassName:    className,
			ShardName:    shardName,
			CoordinatesForID: func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
				return &models.GeoCoordinates{Latitude: ptFloat32(1), Longitude: ptFloat32(2)}, nil
			},
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, idx.Shutdown(context.Background())) })
	}

	linesPerIndex := map[string]int{}
	var sawUnderlyingIndexLine bool
	for _, entry := range hook.AllEntries() {
		id, ok := entry.Data["index_id"]
		require.Truef(t, ok, "line %q names no index", entry.Message)
		linesPerIndex[id.(string)]++

		// only the underlying index's own logger carries these; the commit logger
		// and the vector cache have their own
		class, ok := entry.Data["class"]
		if !ok {
			continue
		}
		sawUnderlyingIndexLine = true
		require.Equalf(t, className, class, "line %q", entry.Message)
		require.Equalf(t, shardName, entry.Data["shard"], "line %q", entry.Message)
	}

	require.True(t, sawUnderlyingIndexLine,
		"no line came from the underlying index, so class/shard went unchecked")
	for _, id := range ids {
		require.NotZerof(t, linesPerIndex[id], "%q logged nothing under its own id", id)
	}
	require.Len(t, linesPerIndex, len(ids), "two geo props were not separable in the log")
}

// The prefill is what the fields are for: it is the work that holds up a
// restart, and PostStartup is the only path that logs it.
func TestPostStartupTagsPrefillLines(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	const className, shardName, indexID = "Article", "shard-a1b2c3", "geo.location"

	coordinates := []*models.GeoCoordinates{
		{Latitude: ptFloat32(48.13743), Longitude: ptFloat32(11.57549)},
		{Latitude: ptFloat32(48.78232), Longitude: ptFloat32(9.17702)},
	}

	newGeoIndex := func(t *testing.T, logger logrus.FieldLogger) *Index {
		t.Helper()
		idx, err := NewIndex(Config{
			AllocChecker: memwatch.NewDummyMonitor(),
			ID:           indexID,
			RootPath:     rootPath,
			Logger:       logger,
			ClassName:    className,
			ShardName:    shardName,
			// without the wait the prefill runs on a goroutine of its own, which is
			// not the journey that leaves an operator staring at a stalled startup
			WaitForCachePrefill: true,
			CoordinatesForID: func(_ context.Context, id uint64) (*models.GeoCoordinates, error) {
				return coordinates[id], nil
			},
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		return idx
	}

	// a reopened index has commit log state to restore, which is what leaves its
	// cache unfilled; a fresh one skips the prefill altogether
	built := newGeoIndex(t, nil)
	for id, c := range coordinates {
		require.NoError(t, built.Add(ctx, uint64(id), c))
	}
	require.NoError(t, built.Flush())
	require.NoError(t, built.Shutdown(ctx))

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	restarted := newGeoIndex(t, logger)
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown(ctx)) })

	hook.Reset()
	restarted.PostStartup(ctx)

	var prefillLines int
	for _, entry := range hook.AllEntries() {
		action, _ := entry.Data["action"].(string)
		if !strings.Contains(action, "prefill") {
			continue
		}
		prefillLines++
		require.Equalf(t, indexID, entry.Data["index_id"], "line %q", entry.Message)
		require.Equalf(t, className, entry.Data["class"], "line %q", entry.Message)
		require.Equalf(t, shardName, entry.Data["shard"], "line %q", entry.Message)
	}
	require.NotZero(t, prefillLines, "PostStartup logged no prefill line to check")
}

func ptFloat32(in float32) *float32 {
	return &in
}
