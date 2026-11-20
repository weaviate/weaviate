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

package sorter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func distanceSort(prop, order string, lat, lon float32) []filters.Sort {
	return []filters.Sort{{
		Path: []string{prop}, Order: order,
		DistanceFrom: &filters.SortGeoOrigin{Latitude: lat, Longitude: lon},
	}}
}

func cityNames(objs []*storobj.Object) []string {
	names := make([]string, len(objs))
	for i, obj := range objs {
		names[i] = obj.Properties().(map[string]interface{})["name"].(string)
	}
	return names
}

// TestObjectsSorterByDistance covers the merge of several shards' results,
// which sorts the objects themselves.
func TestObjectsSorterByDistance(t *testing.T) {
	const (
		berlinLat, berlinLon = 52.52, 13.405
		nycLat, nycLon       = 40.7, -74
	)
	tests := []struct {
		name string
		sort []filters.Sort
		// the cities without a location follow in either order
		wantLocated []string
	}{
		{
			name:        "nearest to Berlin first",
			sort:        distanceSort("location", "asc", berlinLat, berlinLon),
			wantLocated: []string{"Berlin", "Wroclaw", "Amsterdam", "New York"},
		},
		{
			name:        "farthest from Berlin first",
			sort:        distanceSort("location", "desc", berlinLat, berlinLon),
			wantLocated: []string{"New York", "Amsterdam", "Wroclaw", "Berlin"},
		},
		{
			name:        "nearest to New York first",
			sort:        distanceSort("location", "asc", nycLat, nycLon),
			wantLocated: []string{"New York", "Amsterdam", "Berlin", "Wroclaw"},
		},
		{
			name: "a second criterion is still applied",
			sort: append(distanceSort("location", "asc", berlinLat, berlinLon),
				filters.Sort{Path: []string{"name"}, Order: "desc"}),
			wantLocated: []string{"Berlin", "Wroclaw", "Amsterdam", "New York"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorter := NewObjectsSorter(sorterCitySchema().GetClass)
			objs, dists, err := sorter.Sort(sorterCitySchemaObjects(), sorterCitySchemaDistances(), 0, tt.sort)
			require.NoError(t, err)

			names := cityNames(objs)
			require.Len(t, names, 6)
			assert.Equal(t, tt.wantLocated, names[:4])
			assert.ElementsMatch(t, []string{"Nil", "Nil2"}, names[4:], "cities without a location come last")
			// the payload travels with its object
			byName := map[string]float32{"Wroclaw": 0.1, "Berlin": 0.2, "New York": 0.3, "Amsterdam": 0.4, "Nil": 0, "Nil2": 0}
			for i, name := range names {
				assert.Equal(t, byName[name], dists[i], name)
			}
		})
	}

	t.Run("the second criterion orders the cities without a location", func(t *testing.T) {
		sort := append(distanceSort("location", "asc", berlinLat, berlinLon),
			filters.Sort{Path: []string{"name"}, Order: "desc"})
		objs, _, err := NewObjectsSorter(sorterCitySchema().GetClass).Sort(sorterCitySchemaObjects(), nil, 0, sort)
		require.NoError(t, err)
		assert.Equal(t, []string{"Nil2", "Nil"}, cityNames(objs)[4:])
	})

	t.Run("a limit cuts the nearest", func(t *testing.T) {
		objs, _, err := NewObjectsSorter(sorterCitySchema().GetClass).
			Sort(sorterCitySchemaObjects(), nil, 2, distanceSort("location", "asc", berlinLat, berlinLon))
		require.NoError(t, err)
		assert.Equal(t, []string{"Berlin", "Wroclaw"}, cityNames(objs))
	})

	t.Run("a location decoded from JSON is a map", func(t *testing.T) {
		asMap := func(name string, lat, lon float64) *storobj.Object {
			return &storobj.Object{Object: models.Object{Class: "City", Properties: map[string]interface{}{
				"name": name, "location": map[string]interface{}{"latitude": lat, "longitude": lon},
			}}}
		}
		objs := []*storobj.Object{asMap("far", -33.87, 151.21), asMap("near", 52.4, 13.1), cityNil, asMap("mid", 48.1, 11.6)}
		got, _, err := NewObjectsSorter(sorterCitySchema().GetClass).
			Sort(objs, nil, 0, distanceSort("location", "asc", berlinLat, berlinLon))
		require.NoError(t, err)
		assert.Equal(t, []string{"near", "mid", "far", "Nil"}, cityNames(got))
	})

	// Coordinates are not range-checked on write, and the geo bucket measures
	// between canonical points; the sort of the objects must agree with it.
	t.Run("coordinates out of range are the points they name", func(t *testing.T) {
		at := func(name string, lat, lon float32) *storobj.Object {
			return &storobj.Object{Object: models.Object{Class: "City", Properties: map[string]interface{}{
				"name": name, "location": &models.GeoCoordinates{Latitude: &lat, Longitude: &lon},
			}}}
		}
		objs := func() []*storobj.Object {
			return []*storobj.Object{
				at("a degree east", 0, -79),
				at("many turns around", 0, 1e10),     // (0, -80): 27777777 turns and 280 degrees
				at("over the pole", 100, 100),        // (80, -80)
				at("a turn and a bit", 0, 360-80.01), // (0, -80.01)
			}
		}
		want := []string{"many turns around", "a turn and a bit", "a degree east", "over the pole"}
		for name, origin := range map[string][2]float32{"canonical": {0, -80}, "a turn around": {0, 280}, "many turns": {0, 1e10}} {
			got, _, err := NewObjectsSorter(sorterCitySchema().GetClass).
				Sort(objs(), nil, 0, distanceSort("location", "asc", origin[0], origin[1]))
			require.NoError(t, err)
			assert.Equal(t, want, cityNames(got), "origin %s", name)
		}
	})

	t.Run("a distance sort on another type is an error", func(t *testing.T) {
		for _, prop := range []string{"name", "population", "phoneNumber", "doesNotExist"} {
			_, _, err := NewObjectsSorter(sorterCitySchema().GetClass).
				Sort(sorterCitySchemaObjects(), nil, 0, distanceSort(prop, "asc", berlinLat, berlinLon))
			require.ErrorContains(t, err, "needs a geoCoordinates property", prop)
		}
	})

	t.Run("without an origin the coordinates are compared as before", func(t *testing.T) {
		objs, _, err := NewObjectsSorter(sorterCitySchema().GetClass).
			Sort(sorterCitySchemaObjects(), nil, 0, sort1("location", "asc"))
		require.NoError(t, err)
		// by longitude, cities without a location first
		assert.Equal(t, []string{"New York", "Amsterdam", "Berlin", "Wroclaw"}, cityNames(objs)[2:])
	})
}

func TestDistanceComparator(t *testing.T) {
	near, far := 10.0, 20.0
	var typedNil *float64
	tests := []struct {
		name      string
		a, b      interface{}
		asc, desc int
	}{
		{name: "nearer before farther", a: &near, b: &far, asc: -1, desc: 1},
		{name: "farther after nearer", a: &far, b: &near, asc: 1, desc: -1},
		{name: "equal", a: &near, b: &near, asc: 0, desc: 0},
		{name: "missing after a distance", a: nil, b: &near, asc: 1, desc: 1},
		{name: "a distance before missing", a: &far, b: nil, asc: -1, desc: -1},
		{name: "a typed nil is missing", a: typedNil, b: &near, asc: 1, desc: 1},
		{name: "both missing", a: nil, b: typedNil, asc: 0, desc: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.asc, newDistanceComparator("asc").compare(tt.a, tt.b))
			assert.Equal(t, tt.desc, newDistanceComparator("desc").compare(tt.a, tt.b))
		})
	}
}

// A property stored as an explicit null must sort as a missing one. The
// structured types used to panic on it: the JSON null decodes to a nil
// pointer.
func TestComparableValueExtractorNullStructuredValues(t *testing.T) {
	sch := getMyFavoriteClassSchemaForTests()
	extractor := newComparableValueExtractor(newDataTypesHelper(sch.GetClass(testClassName)))

	object := createMyFavoriteClassObject()
	props := object.Object.Properties.(map[string]interface{})
	props["geoProp"] = nil
	props["phoneProp"] = nil
	objData, err := object.MarshalBinary()
	require.NoError(t, err)

	for _, propName := range []string{"geoProp", "phoneProp"} {
		t.Run(propName, func(t *testing.T) {
			require.NotPanics(t, func() {
				assert.Nil(t, extractor.extractFromBytes(objData, propName))
			})
		})
	}
	t.Run("distance", func(t *testing.T) {
		origin := &filters.SortGeoOrigin{Latitude: 1, Longitude: 2}
		assert.Nil(t, extractor.extractDistanceFromBytes(objData, "geoProp", origin))
		assert.Nil(t, extractor.extractDistanceFromBytes(objData, "doesNotExist", origin))
		assert.Nil(t, extractor.extractDistanceFromBytes([]byte("not an object"), "geoProp", origin))
	})
}
