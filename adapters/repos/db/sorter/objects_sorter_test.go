//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestObjectsSorter(t *testing.T) {
	tests := []struct {
		name      string
		sort      []filters.Sort
		limit     int
		wantObjs  []*storobj.Object
		wantDists []float32
	}{
		{
			name:      "sort by string asc",
			sort:      sort1("name", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by string desc",
			sort:      sort1("name", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by text asc",
			sort:      sort1("country", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityBerlin, cityWroclaw, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.2, 0.1, 0.4, 0.3},
		},
		{
			name:      "sort by text desc",
			sort:      sort1("country", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNewYork, cityAmsterdam, cityWroclaw, cityBerlin, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.4, 0.1, 0.2, 0.0, 0.0},
		},
		{
			name:      "sort by int asc",
			sort:      sort1("population", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityAmsterdam, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.4, 0.2, 0.3},
		},
		{
			name:      "sort by int desc",
			sort:      sort1("population", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityAmsterdam, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.4, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by number asc",
			sort:      sort1("cityArea", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityWroclaw, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.1, 0.2, 0.3},
		},
		{
			name:      "sort by number desc",
			sort:      sort1("cityArea", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityWroclaw, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.1, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by date asc",
			sort:      sort1("cityRights", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityWroclaw, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.1, 0.2, 0.3},
		},
		{
			name:      "sort by date desc",
			sort:      sort1("cityRights", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityWroclaw, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.1, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by string array asc",
			sort:      sort1("timezones", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.4, 0.3},
		},
		{
			name:      "sort by string array desc",
			sort:      sort1("timezones", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.2, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by text array asc",
			sort:      sort1("timezonesUTC", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.4, 0.3},
		},
		{
			name:      "sort by text array desc",
			sort:      sort1("timezonesUTC", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.2, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by bool asc",
			sort:      sort1("isCapital", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.1, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by bool desc",
			sort:      sort1("isCapital", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityBerlin, cityAmsterdam, cityWroclaw, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.2, 0.4, 0.1, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by bool array asc",
			sort:      sort1("isCapitalArray", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.4, 0.3},
		},
		{
			name:      "sort by bool array desc",
			sort:      sort1("isCapitalArray", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNewYork, cityAmsterdam, cityBerlin, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.4, 0.2, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by number array asc",
			sort:      sort1("favoriteNumbers", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.3, 0.1, 0.2, 0.4},
		},
		{
			name:      "sort by number array desc",
			sort:      sort1("favoriteNumbers", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityWroclaw, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.4, 0.2, 0.1, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by int array asc",
			sort:      sort1("favoriteInts", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.3, 0.1, 0.2, 0.4},
		},
		{
			name:      "sort by int array desc",
			sort:      sort1("favoriteInts", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityWroclaw, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.4, 0.2, 0.1, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by date array asc",
			sort:      sort1("favoriteDates", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityWroclaw, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.1, 0.2, 0.3},
		},
		{
			name:      "sort by date array desc",
			sort:      sort1("favoriteDates", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityWroclaw, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.1, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by phoneNumber asc",
			sort:      sort1("phoneNumber", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityAmsterdam, cityNewYork, cityBerlin},
			wantDists: []float32{0.0, 0.0, 0.1, 0.4, 0.3, 0.2},
		},
		{
			name:      "sort by phoneNumber desc",
			sort:      sort1("phoneNumber", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityBerlin, cityNewYork, cityAmsterdam, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.2, 0.3, 0.4, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by location asc",
			sort:      sort1("location", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityAmsterdam, cityBerlin, cityWroclaw},
			wantDists: []float32{0.0, 0.0, 0.3, 0.4, 0.2, 0.1},
		},
		{
			name:      "sort by location desc",
			sort:      sort1("location", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.1, 0.2, 0.4, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by special id property asc",
			sort:      sort1("id", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special id property desc",
			sort:      sort1("id", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by special _id property asc",
			sort:      sort1("_id", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special _id property desc",
			sort:      sort1("_id", "desc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by special _creationTimeUnix property asc",
			sort:      sort1("_creationTimeUnix", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special _creationTimeUnix property desc",
			sort:      sort1("_creationTimeUnix", "desc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by special _lastUpdateTimeUnix property asc",
			sort:      sort1("_lastUpdateTimeUnix", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special _lastUpdateTimeUnix property desc",
			sort:      sort1("_lastUpdateTimeUnix", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by isCapital asc & name asc",
			sort:      sort2("isCapital", "asc", "name", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNil, cityNil2, cityNewYork, cityWroclaw, cityAmsterdam, cityBerlin},
			wantDists: []float32{0.0, 0.0, 0.3, 0.1, 0.4, 0.2},
		},
		{
			name:      "sort by isCapital desc & name asc",
			sort:      sort2("isCapital", "desc", "name", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityWroclaw, cityNil, cityNil2},
			wantDists: []float32{0.4, 0.2, 0.3, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by timezones desc & name desc",
			sort:      sort2("timezones", "desc", "name", "desc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.2, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by timezones desc & name asc",
			sort:      sort2("timezones", "desc", "name", "asc"),
			limit:     3,
			wantObjs:  []*storobj.Object{cityNewYork, cityAmsterdam, cityBerlin, cityWroclaw, cityNil, cityNil2},
			wantDists: []float32{0.3, 0.4, 0.2, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by timezonesUTC asc & timezones desc & isCapital asc & population asc",
			sort:      sort4("timezonesUTC", "asc", "timezones", "desc", "isCapital", "asc", "population", "asc"),
			limit:     4,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityAmsterdam, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.4, 0.2, 0.3},
		},
		{
			name:      "sort by timezonesUTC asc & timezones desc & isCapital desc & population asc",
			sort:      sort4("timezonesUTC", "asc", "timezones", "desc", "isCapital", "desc", "population", "asc"),
			limit:     5,
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityBerlin, cityWroclaw, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.2, 0.1, 0.3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("with distance", func(t *testing.T) {
				sorter := NewObjectsSorter(sorterCitySchema())
				gotObjs, gotDists, err := sorter.Sort(sorterCitySchemaObjects(), sorterCitySchemaDistances(), 0, tt.sort)

				require.Nil(t, err)

				if !reflect.DeepEqual(gotObjs, tt.wantObjs) {
					t.Fatalf("objects got = %v, want %v",
						extractCityNames(gotObjs), extractCityNames(tt.wantObjs))
				}
				if !reflect.DeepEqual(gotDists, tt.wantDists) {
					t.Fatalf("distances got = %v, want %v",
						gotDists, tt.wantDists)
				}
			})

			t.Run("without distance", func(t *testing.T) {
				sorter := NewObjectsSorter(sorterCitySchema())
				gotObjs, gotDists, err := sorter.Sort(sorterCitySchemaObjects(), nil, 0, tt.sort)

				require.Nil(t, err)

				if !reflect.DeepEqual(gotObjs, tt.wantObjs) {
					t.Fatalf("objects got = %v, want %v",
						extractCityNames(gotObjs), extractCityNames(tt.wantObjs))
				}
				if gotDists != nil {
					t.Fatalf("distances got = %v, want nil",
						gotDists)
				}
			})

			t.Run("with limit", func(t *testing.T) {
				sorter := NewObjectsSorter(sorterCitySchema())
				gotObjs, gotDists, err := sorter.Sort(sorterCitySchemaObjects(), sorterCitySchemaDistances(), tt.limit, tt.sort)

				require.Nil(t, err)

				if !reflect.DeepEqual(gotObjs, tt.wantObjs[:tt.limit]) {
					t.Fatalf("objects got = %v, want %v",
						extractCityNames(gotObjs), extractCityNames(tt.wantObjs))
				}
				if !reflect.DeepEqual(gotDists, tt.wantDists[:tt.limit]) {
					t.Fatalf("distances got = %v, want %v",
						gotDists, tt.wantDists)
				}
			})
		})
	}
}

func createSort(property, order string) filters.Sort {
	return filters.Sort{Path: []string{property}, Order: order}
}

func sort1(property, order string) []filters.Sort {
	return []filters.Sort{createSort(property, order)}
}

func sort2(property1, order1, property2, order2 string) []filters.Sort {
	return []filters.Sort{
		createSort(property1, order1),
		createSort(property2, order2),
	}
}

func sort4(property1, order1, property2, order2, property3, order3, property4, order4 string) []filters.Sort {
	return []filters.Sort{
		createSort(property1, order1),
		createSort(property2, order2),
		createSort(property3, order3),
		createSort(property4, order4),
	}
}

func extractCityNames(in []*storobj.Object) []string {
	out := make([]string, len(in))
	for i := range in {
		if asMap, ok := in[i].Properties().(map[string]interface{}); ok {
			for k, v := range asMap {
				if k == "name" {
					out[i] = v.(string)
				}
			}
		}
	}
	return out
}
