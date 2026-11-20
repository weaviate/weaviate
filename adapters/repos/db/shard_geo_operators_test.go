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
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func geoBoxFilter(propName string, south, north, west, east float32) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorWithinGeoBox,
		On:       &filters.Path{Class: schema.ClassName(geoBucketClass), Property: schema.PropertyName(propName)},
		Value: &filters.Value{
			Value: filters.GeoBox{SouthWest: geoAt(south, west), NorthEast: geoAt(north, east)},
			Type:  schema.DataTypeGeoCoordinates,
		},
	}}
}

func geoRangeFilter(propName string, center *models.GeoCoordinates, meters float32) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorWithinGeoRange,
		On:       &filters.Path{Class: schema.ClassName(geoBucketClass), Property: schema.PropertyName(propName)},
		Value: &filters.Value{
			Value: filters.GeoRange{GeoCoordinates: center, Distance: meters},
			Type:  schema.DataTypeGeoCoordinates,
		},
	}}
}

func distanceSort(propName string, origin *models.GeoCoordinates, order string) []filters.Sort {
	return []filters.Sort{{
		Path:         []string{propName},
		Order:        order,
		DistanceFrom: &filters.SortGeoOrigin{Latitude: *origin.Latitude, Longitude: *origin.Longitude},
	}}
}

func namesOf(objs []*storobj.Object) []string {
	names := make([]string, len(objs))
	for i, obj := range objs {
		names[i], _ = obj.Properties().(map[string]interface{})["name"].(string)
	}
	return names
}

func TestGeoBoxFilter(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)

	places := map[string]*models.GeoCoordinates{
		"berlin":  berlinCoords,
		"munich":  munichCoords,
		"hamburg": hamburgCoords,
		"suva":    geoAt(-18.14, 178.44),   // Fiji, west of the antimeridian
		"apia":    geoAt(-13.83, -171.77),  // Samoa, east of it
		"taveuni": geoAt(-16.85, 180),      // on it
		"edge":    geoAt(-17, -180),        // on it, by its other name
		"sydney":  geoAt(-33.87, 151.21),   // outside every box below
		"corner":  geoAt(47.0, 11.0),       // on the corner of the alpine box
		"outside": geoAt(46.999, 10.99999), // just past that corner
	}
	for name, coords := range places {
		require.NoError(t, s.PutObject(ctx, geoTestObject(map[string]interface{}{"name": name, "location": coords})))
	}
	require.NoError(t, s.PutObject(ctx, geoTestObject(map[string]interface{}{"name": "nowhere"})))

	search := func(t *testing.T, filter *filters.LocalFilter) []string {
		t.Helper()
		objs, _, err := s.ObjectSearch(ctx, 100, filter, nil, nil, nil, additional.Properties{}, []string{"name"})
		require.NoError(t, err)
		names := namesOf(objs)
		sort.Strings(names)
		return names
	}

	tests := []struct {
		name                     string
		south, north, west, east float32
		want                     []string
	}{
		{name: "germany", south: 47, north: 55, west: 5, east: 15, want: []string{"berlin", "corner", "hamburg", "munich"}},
		{name: "edges are inclusive", south: 47, north: 48.137, west: 11, east: 11.575, want: []string{"corner", "munich"}},
		{name: "across the antimeridian", south: -20, north: -10, west: 175, east: -170, want: []string{"apia", "edge", "suva", "taveuni"}},
		{name: "up to the antimeridian from the west", south: -20, north: -10, west: 175, east: 180, want: []string{"edge", "suva", "taveuni"}},
		{name: "from the antimeridian to the east", south: -20, north: -10, west: -180, east: -170, want: []string{"apia", "edge", "taveuni"}},
		{name: "the whole globe", south: -90, north: 90, west: -180, east: 180, want: []string{
			"apia", "berlin", "corner", "edge", "hamburg", "munich", "outside", "suva", "sydney", "taveuni",
		}},
		{name: "empty ocean", south: 0, north: 1, west: -30, east: -29, want: []string{}},
	}
	for _, flushed := range []bool{false, true} {
		if flushed {
			bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM("location"))
			require.NotNil(t, bucket)
			require.NoError(t, bucket.FlushAndSwitch())
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s flushed=%v", tt.name, flushed), func(t *testing.T) {
				assert.Equal(t, tt.want, search(t, geoBoxFilter("location", tt.south, tt.north, tt.west, tt.east)))
			})
		}
	}

	t.Run("a deleted object leaves the box", func(t *testing.T) {
		objs, _, err := s.ObjectSearch(ctx, 100, geoBoxFilter("location", 52, 53, 13, 14), nil, nil, nil,
			additional.Properties{}, []string{"name"})
		require.NoError(t, err)
		require.Len(t, objs, 1)
		require.NoError(t, s.DeleteObject(ctx, objs[0].ID(), time.Now()))
		assert.Equal(t, []string{"corner", "hamburg", "munich"}, search(t, geoBoxFilter("location", 47, 55, 5, 15)))
	})

	t.Run("an invalid box is an error", func(t *testing.T) {
		_, _, err := s.ObjectSearch(ctx, 100, geoBoxFilter("location", 55, 47, 5, 15), nil, nil, nil,
			additional.Properties{}, []string{"name"})
		require.ErrorContains(t, err, "past north")
		_, _, err = s.ObjectSearch(ctx, 100, geoBoxFilter("location", -100, 47, 5, 15), nil, nil, nil,
			additional.Properties{}, []string{"name"})
		require.ErrorContains(t, err, "within [-90, 90]")
	})

	t.Run("a box with a missing corner is an error", func(t *testing.T) {
		filter := geoBoxFilter("location", 47, 55, 5, 15)
		filter.Root.Value.Value = filters.GeoBox{SouthWest: geoAt(47, 5)}
		_, _, err := s.ObjectSearch(ctx, 100, filter, nil, nil, nil, additional.Properties{}, []string{"name"})
		require.ErrorContains(t, err, "both corners")
	})
}

// A filter value that does not go with a geo property or with its operator
// must come back as an error: the value is whatever the request carried.
func TestGeoFilterValueMismatch(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)
	require.NoError(t, s.PutObject(ctx, geoTestObject(map[string]interface{}{"name": "berlin", "location": berlinCoords})))

	tests := []struct {
		name     string
		operator filters.Operator
		value    interface{}
		wantErr  string
	}{
		{name: "text on a geo property", operator: filters.OperatorEqual, value: "berlin", wantErr: "geo range and geo box filters"},
		{
			name: "range value with the box operator", operator: filters.OperatorWithinGeoBox,
			value: filters.GeoRange{GeoCoordinates: berlinCoords, Distance: 10}, wantErr: "needs operator WithinGeoRange",
		},
		{
			name: "box value with the range operator", operator: filters.OperatorWithinGeoRange,
			value: filters.GeoBox{SouthWest: geoAt(0, 0), NorthEast: geoAt(1, 1)}, wantErr: "needs operator WithinGeoBox",
		},
		{
			name: "range value with equal", operator: filters.OperatorEqual,
			value: filters.GeoRange{GeoCoordinates: berlinCoords, Distance: 10}, wantErr: "needs operator WithinGeoRange",
		},
		{name: "nil value", operator: filters.OperatorWithinGeoRange, value: nil, wantErr: "geo range and geo box filters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := &filters.LocalFilter{Root: &filters.Clause{
				Operator: tt.operator,
				On:       &filters.Path{Class: schema.ClassName(geoBucketClass), Property: "location"},
				Value:    &filters.Value{Value: tt.value, Type: schema.DataTypeGeoCoordinates},
			}}
			require.NotPanics(t, func() {
				_, _, err := s.ObjectSearch(ctx, 100, filter, nil, nil, nil, additional.Properties{}, []string{"name"})
				require.ErrorContains(t, err, tt.wantErr)
			})
		})
	}
}

// geoSortFixture is a shard of random places, some without coordinates, and
// what a sort by distance over them must return.
type geoSortFixture struct {
	s      *Shard
	coords map[string]*models.GeoCoordinates // by name; nil for a place without a location
}

func newGeoSortFixture(t *testing.T, ctx context.Context, s *Shard, n int, seed int64) *geoSortFixture {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	f := &geoSortFixture{s: s, coords: map[string]*models.GeoCoordinates{}}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("place%04d", i)
		props := map[string]interface{}{"name": name}
		if i%7 != 3 {
			var coords *models.GeoCoordinates
			if i%2 == 0 {
				// a cluster around Berlin, the rest anywhere
				coords = geoAt(52.5+float32(r.NormFloat64()), 13.4+float32(r.NormFloat64()))
			} else {
				coords = geoAt(float32(r.Float64()*170-85), float32(r.Float64()*360-180))
			}
			props["location"] = coords
			f.coords[name] = coords
		} else {
			f.coords[name] = nil
		}
		require.NoError(t, s.PutObject(ctx, geoTestObject(props)))
	}
	return f
}

// distances are those of the result in order, NaN for a place without a
// location; names alone cannot be compared since equal distances may swap.
func (f *geoSortFixture) distances(objs []*storobj.Object, origin *models.GeoCoordinates) []float64 {
	out := make([]float64, len(objs))
	for i, name := range namesOf(objs) {
		out[i] = f.distance(name, origin)
	}
	return out
}

func (f *geoSortFixture) distance(name string, origin *models.GeoCoordinates) float64 {
	coords := f.coords[name]
	if coords == nil {
		return math.NaN()
	}
	return float64(geopoints.Distance(*origin.Latitude, *origin.Longitude, *coords.Latitude, *coords.Longitude))
}

// want is the sorted distances of the places keep admits, places without a
// location last in either order, cut at limit.
func (f *geoSortFixture) want(origin *models.GeoCoordinates, order string, limit int, keep func(name string) bool) []float64 {
	var located []float64
	missing := 0
	for name := range f.coords {
		if keep != nil && !keep(name) {
			continue
		}
		if d := f.distance(name, origin); math.IsNaN(d) {
			missing++
		} else {
			located = append(located, d)
		}
	}
	sort.Float64s(located)
	if order == "desc" {
		for i, j := 0, len(located)-1; i < j; i, j = i+1, j-1 {
			located[i], located[j] = located[j], located[i]
		}
	}
	for i := 0; i < missing; i++ {
		located = append(located, math.NaN())
	}
	return located[:min(limit, len(located))]
}

func assertSameDistances(t *testing.T, want, got []float64, msgAndArgs ...interface{}) {
	t.Helper()
	require.Len(t, got, len(want), msgAndArgs...)
	for i := range want {
		if math.IsNaN(want[i]) {
			require.True(t, math.IsNaN(got[i]), "position %d: want a place without a location, got distance %v", i, got[i])
			continue
		}
		require.Equal(t, want[i], got[i], "position %d", i)
	}
}

func TestGeoSortByDistance(t *testing.T) {
	ctx := context.Background()
	s, idx := testGeoBucketShard(t, ctx)
	defer removeRootPath(t, idx)

	const n = 700
	f := newGeoSortFixture(t, ctx, s, n, 42)
	located := 0
	for _, coords := range f.coords {
		if coords != nil {
			located++
		}
	}

	origins := map[string]*models.GeoCoordinates{
		"berlin":      berlinCoords,
		"south pole":  geoAt(-90, 0),
		"antimeridan": geoAt(0, 180),
	}
	// the first limits are answered from the geo bucket, the last ones need
	// the places without a location, which only the objects have
	limits := []int{1, 10, 150, located, located + 1, n, n + 50}

	run := func(t *testing.T) {
		for originName, origin := range origins {
			for _, order := range []string{"asc", "desc"} {
				for _, limit := range limits {
					t.Run(fmt.Sprintf("%s %s limit %d", originName, order, limit), func(t *testing.T) {
						objs, err := s.ObjectList(ctx, limit, distanceSort("location", origin, order), nil,
							additional.Properties{}, schema.ClassName(geoBucketClass))
						require.NoError(t, err)
						assertSameDistances(t, f.want(origin, order, limit, nil), f.distances(objs, origin))
					})
				}
			}
		}

		t.Run("under a filter", func(t *testing.T) {
			filterTests := []struct {
				name   string
				filter *filters.LocalFilter
				keep   func(coords *models.GeoCoordinates) bool
			}{
				{
					name:   "a broad box",
					filter: geoBoxFilter("location", -90, 90, -180, 180),
					keep:   func(c *models.GeoCoordinates) bool { return c != nil },
				},
				{
					name:   "the northern hemisphere",
					filter: geoBoxFilter("location", 0, 90, -180, 180),
					keep:   func(c *models.GeoCoordinates) bool { return c != nil && *c.Latitude >= 0 },
				},
				{
					name:   "a selective range",
					filter: geoRangeFilter("location", berlinCoords, 50_000),
					keep: func(c *models.GeoCoordinates) bool {
						return c != nil && geopoints.Distance(*berlinCoords.Latitude, *berlinCoords.Longitude,
							*c.Latitude, *c.Longitude) <= 50_000
					},
				},
				{
					name:   "nothing",
					filter: geoBoxFilter("location", 0, 0.0001, 0, 0.0001),
					keep:   func(c *models.GeoCoordinates) bool { return false },
				},
			}
			for _, ft := range filterTests {
				keep := func(name string) bool { return ft.keep(f.coords[name]) }
				for _, order := range []string{"asc", "desc"} {
					for _, limit := range []int{1, 10, 150, n} {
						t.Run(fmt.Sprintf("%s %s limit %d", ft.name, order, limit), func(t *testing.T) {
							objs, _, err := s.ObjectSearch(ctx, limit, ft.filter, nil,
								distanceSort("location", munichCoords, order), nil, additional.Properties{}, []string{"name"})
							require.NoError(t, err)
							assertSameDistances(t, f.want(munichCoords, order, limit, keep), f.distances(objs, munichCoords))
						})
					}
				}
			}
		})
	}

	t.Run("memtable", run)

	bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM("location"))
	require.NotNil(t, bucket)
	require.NoError(t, bucket.FlushAndSwitch())
	t.Run("segment", run)

	t.Run("deleted and moved places", func(t *testing.T) {
		nearest := func() *storobj.Object {
			objs, err := s.ObjectList(ctx, 1, distanceSort("location", berlinCoords, "asc"), nil,
				additional.Properties{}, schema.ClassName(geoBucketClass))
			require.NoError(t, err)
			require.Len(t, objs, 1)
			return objs[0]
		}

		first := nearest()
		firstName := namesOf([]*storobj.Object{first})[0]
		require.NoError(t, s.DeleteObject(ctx, first.ID(), time.Now()))
		delete(f.coords, firstName)
		second := nearest()
		require.NotEqual(t, first.ID(), second.ID(), "a deleted place must not be the nearest")
		assertSameDistances(t, f.want(berlinCoords, "asc", 1, nil), f.distances([]*storobj.Object{second}, berlinCoords))

		// the farthest place moves onto the origin and must come first
		farthest, err := s.ObjectList(ctx, 1, distanceSort("location", berlinCoords, "desc"), nil,
			additional.Properties{}, schema.ClassName(geoBucketClass))
		require.NoError(t, err)
		require.Len(t, farthest, 1)
		movedName := namesOf(farthest)[0]
		moved := geoTestObject(map[string]interface{}{"name": movedName, "location": berlinCoords})
		moved.Object.ID = farthest[0].ID()
		require.NoError(t, s.PutObject(ctx, moved))
		f.coords[movedName] = berlinCoords
		assert.Equal(t, movedName, namesOf([]*storobj.Object{nearest()})[0])

		// and the whole order still holds across the memtable and the segment
		objs, err := s.ObjectList(ctx, 200, distanceSort("location", berlinCoords, "asc"), nil,
			additional.Properties{}, schema.ClassName(geoBucketClass))
		require.NoError(t, err)
		assertSameDistances(t, f.want(berlinCoords, "asc", 200, nil), f.distances(objs, berlinCoords))
	})

	t.Run("a second geo property sorts by its own coordinates", func(t *testing.T) {
		near := geoTestObject(map[string]interface{}{"name": "home-near", "home": geoAt(48.2, 11.6), "location": geoAt(-40, 100)})
		far := geoTestObject(map[string]interface{}{"name": "home-far", "home": geoAt(-40, 100), "location": geoAt(48.2, 11.6)})
		require.NoError(t, s.PutObject(ctx, far))
		require.NoError(t, s.PutObject(ctx, near))

		objs, err := s.ObjectList(ctx, 2, distanceSort("home", munichCoords, "asc"), nil,
			additional.Properties{}, schema.ClassName(geoBucketClass))
		require.NoError(t, err)
		assert.Equal(t, []string{"home-near", "home-far"}, namesOf(objs))
	})

	t.Run("an explicit null location sorts as a missing one", func(t *testing.T) {
		require.NoError(t, s.PutObject(ctx, geoTestObject(map[string]interface{}{"name": "null-island", "location": nil})))
		f.coords["null-island"] = nil
		for _, sortBy := range [][]filters.Sort{
			distanceSort("location", berlinCoords, "asc"),
			distanceSort("location", berlinCoords, "desc"),
			{{Path: []string{"location"}, Order: "asc"}},
		} {
			require.NotPanics(t, func() {
				objs, err := s.ObjectList(ctx, 2000, sortBy, nil, additional.Properties{}, schema.ClassName(geoBucketClass))
				require.NoError(t, err)
				assert.Contains(t, namesOf(objs), "null-island")
				if sortBy[0].DistanceFrom != nil {
					last := namesOf(objs)[len(objs)-1]
					assert.Nil(t, f.coordsOrHome(last), "%s has a location and must not come last", last)
				}
			})
		}
	})

	t.Run("a distance sort needs a geo property", func(t *testing.T) {
		_, err := s.ObjectList(ctx, 10, distanceSort("name", berlinCoords, "asc"), nil,
			additional.Properties{}, schema.ClassName(geoBucketClass))
		require.ErrorContains(t, err, "needs a geoCoordinates property")
	})

	t.Run("without an origin a geo property still sorts by its coordinates", func(t *testing.T) {
		objs, err := s.ObjectList(ctx, 5, []filters.Sort{{Path: []string{"location"}, Order: "desc"}}, nil,
			additional.Properties{}, schema.ClassName(geoBucketClass))
		require.NoError(t, err)
		require.Len(t, objs, 5)
		var lons []float64
		for _, name := range namesOf(objs) {
			lons = append(lons, float64(*f.coordsOrHome(name).Longitude))
		}
		assert.True(t, sort.IsSorted(sort.Reverse(sort.Float64Slice(lons))), "longitudes descending: %v", lons)
	})
}

// coordsOrHome covers the two places the fixture did not create.
func (f *geoSortFixture) coordsOrHome(name string) *models.GeoCoordinates {
	switch name {
	case "home-near":
		return geoAt(-40, 100)
	case "home-far":
		return geoAt(48.2, 11.6)
	}
	return f.coords[name]
}
