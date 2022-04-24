//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

func Test_objectsSorter(t *testing.T) {
	type args struct {
		property string
		order    string
	}
	tests := []struct {
		name      string
		args      args
		wantObjs  []*storobj.Object
		wantDists []float32
	}{
		{
			name:      "sort by string asc",
			args:      args{"name", "asc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by string desc",
			args:      args{"name", "desc"},
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by text asc",
			args:      args{"country", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityBerlin, cityWroclaw, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.2, 0.1, 0.4, 0.3},
		},
		{
			name:      "sort by text desc",
			args:      args{"country", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityAmsterdam, cityWroclaw, cityBerlin, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.4, 0.1, 0.2, 0.0, 0.0},
		},
		{
			name:      "sort by int asc",
			args:      args{"population", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityAmsterdam, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.4, 0.2, 0.3},
		},
		{
			name:      "sort by int desc",
			args:      args{"population", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityAmsterdam, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.4, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by number asc",
			args:      args{"cityArea", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityWroclaw, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.1, 0.2, 0.3},
		},
		{
			name:      "sort by number desc",
			args:      args{"cityArea", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityWroclaw, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.1, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by date asc",
			args:      args{"cityRights", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityAmsterdam, cityWroclaw, cityBerlin, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.4, 0.1, 0.2, 0.3},
		},
		{
			name:      "sort by date desc",
			args:      args{"cityRights", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityBerlin, cityWroclaw, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.2, 0.1, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by string array asc",
			args:      args{"timezones", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.4, 0.3},
		},
		{
			name:      "sort by string array desc",
			args:      args{"timezones", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.2, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by text array asc",
			args:      args{"timezonesUTC", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.4, 0.3},
		},
		{
			name:      "sort by text array desc",
			args:      args{"timezonesUTC", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.2, 0.4, 0.0, 0.0},
		},
		{
			name:      "sort by bool asc",
			args:      args{"isCapital", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityBerlin, cityAmsterdam, cityWroclaw, cityNewYork},
			wantDists: []float32{0.0, 0.0, 0.2, 0.4, 0.1, 0.3},
		},
		{
			name:      "sort by bool desc",
			args:      args{"isCapital", "desc"},
			wantObjs:  []*storobj.Object{cityNewYork, cityWroclaw, cityAmsterdam, cityBerlin, cityNil2, cityNil},
			wantDists: []float32{0.3, 0.1, 0.4, 0.2, 0.0, 0.0},
		},
		{
			name:      "sort by bool array asc",
			args:      args{"isCapitalArray", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityBerlin, cityNewYork, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.1, 0.2, 0.3, 0.4},
		},
		{
			name:      "sort by bool array desc",
			args:      args{"isCapitalArray", "desc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityNewYork, cityBerlin, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.4, 0.3, 0.2, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by number array asc",
			args:      args{"favoriteNumbers", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.3, 0.1, 0.2, 0.4},
		},
		{
			name:      "sort by number array desc",
			args:      args{"favoriteNumbers", "desc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityWroclaw, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.4, 0.2, 0.1, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by int array asc",
			args:      args{"favoriteInts", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityWroclaw, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.0, 0.0, 0.3, 0.1, 0.2, 0.4},
		},
		{
			name:      "sort by int array desc",
			args:      args{"favoriteInts", "desc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityWroclaw, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.4, 0.2, 0.1, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by date array asc",
			args:      args{"favoriteDates", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityNewYork, cityAmsterdam, cityBerlin},
			wantDists: []float32{0.0, 0.0, 0.1, 0.3, 0.4, 0.2},
		},
		{
			name:      "sort by date array desc",
			args:      args{"favoriteDates", "desc"},
			wantObjs:  []*storobj.Object{cityBerlin, cityAmsterdam, cityNewYork, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.2, 0.4, 0.3, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by phoneNumber asc",
			args:      args{"phoneNumber", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityWroclaw, cityAmsterdam, cityNewYork, cityBerlin},
			wantDists: []float32{0.0, 0.0, 0.1, 0.4, 0.3, 0.2},
		},
		{
			name:      "sort by phoneNumber desc",
			args:      args{"phoneNumber", "desc"},
			wantObjs:  []*storobj.Object{cityBerlin, cityNewYork, cityAmsterdam, cityWroclaw, cityNil2, cityNil},
			wantDists: []float32{0.2, 0.3, 0.4, 0.1, 0.0, 0.0},
		},
		{
			name:      "sort by location asc",
			args:      args{"location", "asc"},
			wantObjs:  []*storobj.Object{cityNil2, cityNil, cityNewYork, cityAmsterdam, cityBerlin, cityWroclaw},
			wantDists: []float32{0.0, 0.0, 0.3, 0.4, 0.2, 0.1},
		},
		{
			name:      "sort by location desc",
			args:      args{"location", "desc"},
			wantObjs:  []*storobj.Object{cityWroclaw, cityBerlin, cityAmsterdam, cityNewYork, cityNil2, cityNil},
			wantDists: []float32{0.1, 0.2, 0.4, 0.3, 0.0, 0.0},
		},
		{
			name:      "sort by special id property asc",
			args:      args{"id", "asc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special id property desc",
			args:      args{"id", "desc"},
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
		{
			name:      "sort by special _id property asc",
			args:      args{"_id", "asc"},
			wantObjs:  []*storobj.Object{cityAmsterdam, cityBerlin, cityNewYork, cityNil, cityNil2, cityWroclaw},
			wantDists: []float32{0.4, 0.2, 0.3, 0.0, 0.0, 0.1},
		},
		{
			name:      "sort by special _id property desc",
			args:      args{"_id", "desc"},
			wantObjs:  []*storobj.Object{cityWroclaw, cityNil2, cityNil, cityNewYork, cityBerlin, cityAmsterdam},
			wantDists: []float32{0.1, 0.0, 0.0, 0.3, 0.2, 0.4},
		},
	}
	// test with distances
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newObjectsSorter(
				getCitySchema(),
				sorterCitySchemaObjects(),
				sorterCitySchemaDistances(),
			)
			gotObjs, gotDists := s.sort([]string{tt.args.property}, tt.args.order)
			if !reflect.DeepEqual(gotObjs, tt.wantObjs) {
				t.Errorf("sorterImpl.sort() objects got = %v, want %v",
					getCityNames(gotObjs), getCityNames(tt.wantObjs))
			}
			if !reflect.DeepEqual(gotDists, tt.wantDists) {
				t.Errorf("sorterImpl.sort() distances got = %v, want %v",
					gotDists, tt.wantDists)
			}
		})
	}
	// test with nil distances
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newObjectsSorter(
				getCitySchema(),
				sorterCitySchemaObjects(),
				nil,
			)
			gotObjs, gotDists := s.sort([]string{tt.args.property}, tt.args.order)
			if !reflect.DeepEqual(gotObjs, tt.wantObjs) {
				t.Errorf("sorterImpl.sort() objects got = %v, want %v",
					getCityNames(gotObjs), getCityNames(tt.wantObjs))
			}
			if gotDists != nil {
				t.Errorf("sorterImpl.sort() distances got = %v, want nil",
					gotDists)
			}
		})
	}
}

func getCitySchema() schema.Schema {
	schemaGetter := &fakeSorterSchemaGetter{schema: sorterCitySchema()}
	return schemaGetter.schema
}

func getCityNames(in []*storobj.Object) []string {
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

type fakeSorterSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSorterSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSorterSchemaGetter) ShardingState(class string) *sharding.State {
	return f.shardState
}

func sorterCitySchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "City",
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "country",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:     "population",
							DataType: []string{string(schema.DataTypeInt)},
						},
						{
							Name:     "cityArea",
							DataType: []string{string(schema.DataTypeNumber)},
						},
						{
							Name:     "cityRights",
							DataType: []string{string(schema.DataTypeDate)},
						},
						{
							Name:     "timezones",
							DataType: []string{string(schema.DataTypeStringArray)},
						},
						{
							Name:     "timezonesUTC",
							DataType: []string{string(schema.DataTypeTextArray)},
						},
						{
							Name:     "isCapital",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
						{
							Name:     "isCapitalArray",
							DataType: []string{string(schema.DataTypeBooleanArray)},
						},
						{
							Name:     "favoriteNumbers",
							DataType: []string{string(schema.DataTypeNumberArray)},
						},
						{
							Name:     "favoriteInts",
							DataType: []string{string(schema.DataTypeIntArray)},
						},
						{
							Name:     "favoriteDates",
							DataType: []string{string(schema.DataTypeDateArray)},
						},
						{
							Name:     "phoneNumber",
							DataType: []string{string(schema.DataTypePhoneNumber)},
						},
						{
							Name:     "location",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
					},
				},
			},
		},
	}
}

func sorterCitySchemaDistances() []float32 {
	return []float32{0.1, 0.0, 0.2, 0.3, 0.4, 0.0}
}

func sorterCitySchemaObjects() []*storobj.Object {
	return []*storobj.Object{cityWroclaw, cityNil2, cityBerlin, cityNewYork, cityAmsterdam, cityNil}
}

var (
	cityWroclaw = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("f10018a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name":            "Wroclaw",
				"country":         "Poland",
				"population":      float64(641928),
				"cityArea":        float64(292.23),
				"cityRights":      "1214-01-01T00:00:00+02:00",
				"timezones":       []string{"CET", "CEST"},
				"timezonesUTC":    []string{"UTC+1", "UTC+2"},
				"isCapital":       false,
				"isCapitalArray":  []bool{false, false},
				"favoriteNumbers": []float64{0, 0, 0},
				"favoriteInts":    []float64{0, 0, 0},
				"favoriteDates":   []string{"1214-01-01T00:00:00+02:00", "1214-01-01T00:00:00+02:00"},
				"phoneNumber": &models.PhoneNumber{
					CountryCode: 0,
					National:    400500600,
				},
				"location": &models.GeoCoordinates{
					Latitude:  pointerFloat32(51.11),
					Longitude: pointerFloat32(17.022222),
				},
			},
		},
	}
	cityBerlin = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("b06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name":            "Berlin",
				"country":         "Germany",
				"population":      float64(3664088),
				"cityArea":        float64(891.95),
				"cityRights":      "1400-01-01T00:00:00+02:00",
				"timezones":       []string{"CET", "CEST"},
				"timezonesUTC":    []string{"UTC+1", "UTC+2"},
				"isCapital":       true,
				"isCapitalArray":  []bool{false, false, true},
				"favoriteNumbers": []float64{0, 10, 1},
				"favoriteInts":    []float64{0, 10, 1},
				"favoriteDates":   []string{"1400-01-01T00:00:00+02:00"},
				"phoneNumber": &models.PhoneNumber{
					CountryCode: 33,
					National:    400500610,
				},
				"location": &models.GeoCoordinates{
					Latitude:  pointerFloat32(52.518611),
					Longitude: pointerFloat32(13.408333),
				},
			},
		},
	}
	cityNewYork = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("e06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name":            "New York",
				"country":         "USA",
				"population":      float64(8336817),
				"cityArea":        float64(1223.59),
				"cityRights":      "1653-01-01T00:00:00+02:00",
				"timezones":       []string{"EST", "EDT"},
				"timezonesUTC":    []string{"UTC-5", "UTC-4"},
				"isCapital":       false,
				"isCapitalArray":  []bool{true, true, true},
				"favoriteNumbers": []float64{-100000.23, -8.909},
				"favoriteInts":    []float64{-100000, -8},
				"favoriteDates":   []string{"1400-01-01T00:00:00+02:00", "1653-01-01T00:00:00+02:00"},
				"phoneNumber": &models.PhoneNumber{
					CountryCode: 33,
					National:    400500609,
				},
				"location": &models.GeoCoordinates{
					Latitude:  pointerFloat32(40.716667),
					Longitude: pointerFloat32(-74),
				},
			},
		},
	}
	cityAmsterdam = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("a06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name":            "Amsterdam",
				"country":         "The Netherlands",
				"population":      float64(905234),
				"cityArea":        float64(219.32),
				"cityRights":      "1100-01-01T00:00:00+02:00",
				"timezones":       []string{"CET", "CEST"},
				"timezonesUTC":    []string{"UTC+1", "UTC+2"},
				"isCapital":       true,
				"isCapitalArray":  []bool{true},
				"favoriteNumbers": []float64{1, 2, 3, 4, 5, 6, 8.8, 9.9},
				"favoriteInts":    []float64{1, 2, 3, 4, 5, 6, 8, 9},
				"favoriteDates":   []string{"1100-01-01T00:00:00+02:00"},
				"phoneNumber": &models.PhoneNumber{
					CountryCode: 33,
					National:    400500602,
				},
				"location": &models.GeoCoordinates{
					Latitude:  pointerFloat32(52.366667),
					Longitude: pointerFloat32(4.9),
				},
			},
		},
	}
	cityNil = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("f00018a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name": "Nil",
			},
		},
	}
	cityNil2 = &storobj.Object{
		Object: models.Object{
			Class: "City",
			ID:    strfmt.UUID("f00028a7-ad67-4774-a9ac-86a04df51cb6"),
			Properties: map[string]interface{}{
				"name": "Nil2",
			},
		},
	}
)

func pointerFloat32(in float32) *float32 {
	return &in
}
