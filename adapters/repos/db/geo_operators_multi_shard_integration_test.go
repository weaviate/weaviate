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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const geoPlaceClass = "GeoPlace"

type geoPlace struct {
	name     string
	rank     int
	lat, lon float32
	located  bool
}

// Test_MultiShard_GeoOperators runs the box filter and the sort by distance
// over a collection of several shards, where each shard answers from its own
// geo bucket and the index merges what they return.
func Test_MultiShard_GeoOperators(t *testing.T) {
	ctx := context.Background()
	repo, logger, mockSchemaReader := setupMultiShardTest(t)
	defer repo.Shutdown(ctx)

	class := &models.Class{
		Class:               geoPlaceClass,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
			{Name: "rank", DataType: schema.DataTypeInt.PropString()},
			{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString()},
		},
	}
	t.Run("prepare", makeTestMultiShardSchema(repo, logger, mockSchemaReader, false, class))

	r := rand.New(rand.NewSource(17))
	places := make([]geoPlace, 240)
	byName := map[string]geoPlace{}
	for i := range places {
		p := geoPlace{name: fmt.Sprintf("place%03d", i), rank: (i * 37) % len(places), located: i%6 != 4}
		if i%2 == 0 {
			p.lat, p.lon = 52.5+float32(r.NormFloat64()), 13.4+float32(r.NormFloat64())
		} else {
			p.lat, p.lon = float32(r.Float64()*170-85), float32(r.Float64()*360-180)
		}
		places[i], byName[p.name] = p, p

		props := map[string]interface{}{"name": p.name, "rank": p.rank}
		if p.located {
			props["location"] = &models.GeoCoordinates{Latitude: &p.lat, Longitude: &p.lon}
		}
		obj := &models.Object{Class: geoPlaceClass, ID: strfmt.UUID(uuid.NewString()), Properties: props}
		vector := []float32{r.Float32(), r.Float32(), r.Float32()}
		require.NoError(t, repo.PutObject(ctx, obj, vector, nil, nil, nil, 0))
	}

	idx := repo.GetIndex(geoPlaceClass)
	require.NotNil(t, idx)
	shards := 0
	require.NoError(t, idx.ForEachShard(func(string, ShardLike) error { shards++; return nil }))
	require.Greater(t, shards, 1, "the test needs several shards to merge")

	const originLat, originLon = float32(48.137), float32(11.575)
	distance := func(p geoPlace) float32 { return geopoints.Distance(originLat, originLon, p.lat, p.lon) }
	distanceSortBy := func(order string) []filters.Sort {
		return []filters.Sort{{
			Path: []string{"location"}, Order: order,
			DistanceFrom: &filters.SortGeoOrigin{Latitude: originLat, Longitude: originLon},
		}}
	}
	onlyName := search.SelectProperties{{Name: "name", IsPrimitive: true}}
	names := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i], _ = res[i].Schema.(map[string]interface{})["name"].(string)
		}
		return out
	}
	// wantByDistance is the names in order, the places without a location as
	// a trailing count: their order among themselves is not defined
	wantByDistance := func(order string, limit int, keep func(geoPlace) bool) (located []string, missing int) {
		var in []geoPlace
		for _, p := range places {
			if keep != nil && !keep(p) {
				continue
			}
			if p.located {
				in = append(in, p)
			} else {
				missing++
			}
		}
		sort.Slice(in, func(i, j int) bool {
			if order == "desc" {
				return distance(in[i]) > distance(in[j])
			}
			return distance(in[i]) < distance(in[j])
		})
		for _, p := range in {
			located = append(located, p.name)
		}
		if len(located) >= limit {
			return located[:limit], 0
		}
		return located, min(missing, limit-len(located))
	}
	assertByDistance := func(t *testing.T, got []string, order string, limit int, keep func(geoPlace) bool) {
		t.Helper()
		located, missing := wantByDistance(order, limit, keep)
		require.Len(t, got, len(located)+missing)
		assert.Equal(t, located, got[:len(located)])
		for _, name := range got[len(located):] {
			assert.False(t, byName[name].located, "%s has a location and must not trail", name)
		}
	}

	inBox := func(south, north, west, east float32) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorWithinGeoBox,
			On:       &filters.Path{Class: geoPlaceClass, Property: "location"},
			Value: &filters.Value{Type: schema.DataTypeGeoCoordinates, Value: filters.GeoBox{
				SouthWest: &models.GeoCoordinates{Latitude: &south, Longitude: &west},
				NorthEast: &models.GeoCoordinates{Latitude: &north, Longitude: &east},
			}},
		}}
	}

	t.Run("box filter", func(t *testing.T) {
		for _, box := range [][4]float32{{50, 55, 10, 16}, {-90, 0, -180, 180}, {-30, 30, 150, -150}, {0, 0.001, 0, 0.001}} {
			q, err := geopoints.NewBoxQuery(box[0], box[1], box[2], box[3])
			require.NoError(t, err)
			var want []string
			for _, p := range places {
				if p.located && q.Contains(p.lat, p.lon) {
					want = append(want, p.name)
				}
			}
			res, err := repo.Search(ctx, dto.GetParams{
				ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: 1000},
				Filters: inBox(box[0], box[1], box[2], box[3]), Properties: onlyName,
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, want, names(res), "box %v", box)
		}
	})

	// A sort must not depend on the request also selecting the property it
	// sorts by: under a filter a shard returns only the selected properties,
	// and the merge of the shards sorts what they return.
	selections := map[string]search.SelectProperties{
		"location selected":      {{Name: "name", IsPrimitive: true}, {Name: "location", IsPrimitive: true}},
		"only the name selected": onlyName,
	}
	for selName, props := range selections {
		t.Run("sort by distance, "+selName, func(t *testing.T) {
			for _, order := range []string{"asc", "desc"} {
				for _, limit := range []int{1, 10, 100, 200, 240, 500} {
					t.Run(fmt.Sprintf("%s limit %d", order, limit), func(t *testing.T) {
						res, err := repo.Search(ctx, dto.GetParams{
							ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: limit},
							Sort: distanceSortBy(order), Properties: props,
						})
						require.NoError(t, err)
						assertByDistance(t, names(res), order, limit, nil)
					})
					t.Run(fmt.Sprintf("%s limit %d in a box", order, limit), func(t *testing.T) {
						res, err := repo.Search(ctx, dto.GetParams{
							ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: limit},
							Sort: distanceSortBy(order), Properties: props, Filters: inBox(0, 90, -180, 180),
						})
						require.NoError(t, err)
						assertByDistance(t, names(res), order, limit, func(p geoPlace) bool { return p.located && p.lat >= 0 })
					})
				}
			}
		})
	}

	t.Run("pagination walks the order", func(t *testing.T) {
		var got []string
		for offset := 0; offset < 60; offset += 20 {
			res, err := repo.Search(ctx, dto.GetParams{
				ClassName: geoPlaceClass, Pagination: &filters.Pagination{Offset: offset, Limit: 20},
				Sort: distanceSortBy("asc"), Properties: onlyName,
			})
			require.NoError(t, err)
			got = append(got, names(res)...)
		}
		assertByDistance(t, got, "asc", 60, nil)
	})

	// The same for a sort by value, which the merge has always read from the
	// objects: with the key missing it kept the shards' results one after the
	// other and cut the limit off the first shard.
	t.Run("sort by an int the request did not select", func(t *testing.T) {
		for _, withFilter := range []bool{false, true} {
			for _, order := range []string{"asc", "desc"} {
				params := dto.GetParams{
					ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: 50},
					Sort: []filters.Sort{{Path: []string{"rank"}, Order: order}}, Properties: onlyName,
				}
				var want []int
				for _, p := range places {
					if !withFilter || p.located {
						want = append(want, p.rank)
					}
				}
				sort.Ints(want)
				if order == "desc" {
					sort.Sort(sort.Reverse(sort.IntSlice(want)))
				}
				if withFilter {
					params.Filters = inBox(-90, 90, -180, 180)
				}

				res, err := repo.Search(ctx, params)
				require.NoError(t, err)
				var ranks []int
				for _, name := range names(res) {
					ranks = append(ranks, byName[name].rank)
				}
				assert.Equal(t, want[:50], ranks, "filter=%v order=%s", withFilter, order)
			}
		}
	})

	// A vector search merges a sort the same way.
	t.Run("vector search sorted by a property the request did not select", func(t *testing.T) {
		query := []models.Vector{[]float32{0.5, 0.5, 0.5}}
		t.Run("by an int", func(t *testing.T) {
			res, err := repo.VectorSearch(ctx, dto.GetParams{
				ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: len(places)},
				Sort: []filters.Sort{{Path: []string{"rank"}, Order: "asc"}}, Properties: onlyName,
			}, []string{""}, query)
			require.NoError(t, err)
			require.Len(t, res, len(places))
			var ranks []int
			for _, name := range names(res) {
				ranks = append(ranks, byName[name].rank)
			}
			assert.True(t, sort.IntsAreSorted(ranks), "ranks out of order: %v", ranks)
		})
		t.Run("by distance", func(t *testing.T) {
			res, err := repo.VectorSearch(ctx, dto.GetParams{
				ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: len(places)},
				Sort: distanceSortBy("asc"), Properties: onlyName,
			}, []string{""}, query)
			require.NoError(t, err)
			assertByDistance(t, names(res), "asc", len(places), nil)
		})
	})

	// Past the request validation, a shard must refuse it too: the planner
	// would give an int to the inverted sorter, which orders by value.
	t.Run("a shard refuses a distance sort on an int", func(t *testing.T) {
		badSort := []filters.Sort{{
			Path: []string{"rank"}, Order: "asc",
			DistanceFrom: &filters.SortGeoOrigin{Latitude: originLat, Longitude: originLon},
		}}
		require.NoError(t, idx.ForEachShard(func(name string, shard ShardLike) error {
			_, err := shard.ObjectList(ctx, 5, badSort, nil, additional.Properties{}, geoPlaceClass)
			assert.ErrorContains(t, err, "needs a geoCoordinates property", name)
			_, _, err = shard.ObjectSearch(ctx, 5, inBox(-90, 90, -180, 180), nil, badSort, nil,
				additional.Properties{}, []string{"name"})
			assert.ErrorContains(t, err, "needs a geoCoordinates property", name)
			return nil
		}))

		_, err := repo.Search(ctx, dto.GetParams{
			ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: 5}, Sort: badSort, Properties: onlyName,
		})
		require.ErrorContains(t, err, "needs a geoCoordinates property")
	})

	t.Run("the sort key is not handed to the caller's selection", func(t *testing.T) {
		props := search.SelectProperties{{Name: "name", IsPrimitive: true}}
		_, err := repo.Search(ctx, dto.GetParams{
			ClassName: geoPlaceClass, Pagination: &filters.Pagination{Limit: 5},
			Sort: distanceSortBy("asc"), Properties: props, Filters: inBox(-90, 90, -180, 180),
		})
		require.NoError(t, err)
		assert.Equal(t, search.SelectProperties{{Name: "name", IsPrimitive: true}}, props)
	})
}
