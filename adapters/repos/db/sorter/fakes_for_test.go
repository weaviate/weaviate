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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

const testClassName = "MyFavoriteClass"

func getMyFavoriteClassSchemaForTests() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: testClassName,
					Properties: []*models.Property{
						{
							Name:     "textProp",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:     "textPropArray",
							DataType: []string{string(schema.DataTypeTextArray)},
						},
						{
							Name:     "intProp",
							DataType: []string{string(schema.DataTypeInt)},
						},
						{
							Name:     "numberProp",
							DataType: []string{string(schema.DataTypeNumber)},
						},
						{
							Name:     "intPropArray",
							DataType: []string{string(schema.DataTypeIntArray)},
						},
						{
							Name:     "numberPropArray",
							DataType: []string{string(schema.DataTypeNumberArray)},
						},
						{
							Name:     "boolProp",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
						{
							Name:     "boolPropArray",
							DataType: []string{string(schema.DataTypeBooleanArray)},
						},
						{
							Name:     "dateProp",
							DataType: []string{string(schema.DataTypeDate)},
						},
						{
							Name:     "datePropArray",
							DataType: []string{string(schema.DataTypeDateArray)},
						},
						{
							Name:     "phoneProp",
							DataType: []string{string(schema.DataTypePhoneNumber)},
						},
						{
							Name:     "geoProp",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
						{
							Name:         "emptyStringProp",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "emptyBoolProp",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
						{
							Name:     "emptyNumberProp",
							DataType: []string{string(schema.DataTypeNumber)},
						},
						{
							Name:     "emptyIntProp",
							DataType: []string{string(schema.DataTypeInt)},
						},
						{
							Name:     "crefProp",
							DataType: []string{string(schema.DataTypeCRef)},
						},
					},
				},
			},
		},
	}
}

func createMyFavoriteClassObject() *storobj.Object {
	return storobj.FromObject(
		&models.Object{
			Class:              testClassName,
			CreationTimeUnix:   900000000001,
			LastUpdateTimeUnix: 900000000002,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{
				"textProp":        "text",
				"textPropArray":   []string{"text", "text"},
				"intProp":         float64(100),
				"numberProp":      float64(17),
				"intPropArray":    []float64{10, 20, 30},
				"numberPropArray": []float64{1, 2, 3},
				"boolProp":        true,
				"boolPropArray":   []bool{true, false, true},
				"dateProp":        "1980-01-01T00:00:00+02:00",
				"datePropArray":   []string{"1980-01-01T00:00:00+02:00"},
				"phoneProp": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1000000",
					Valid:                  true,
					InternationalFormatted: "+49 171 1000000",
					National:               1000000,
					NationalFormatted:      "0171 1000000",
				},
				"geoProp": &models.GeoCoordinates{
					Longitude: ptrFloat32(1),
					Latitude:  ptrFloat32(2),
				},
			},
		},
		[]float32{1, 2, 0.7},
	)
}

func sorterCitySchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "City",
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							Name:         "timezones",
							DataType:     schema.DataTypeTextArray.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
			Class:              "City",
			ID:                 strfmt.UUID("f10018a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000006,
			LastUpdateTimeUnix: 9100000006,
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
					Latitude:  ptrFloat32(51.11),
					Longitude: ptrFloat32(17.022222),
				},
			},
		},
	}
	cityBerlin = &storobj.Object{
		Object: models.Object{
			Class:              "City",
			ID:                 strfmt.UUID("b06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000002,
			LastUpdateTimeUnix: 9100000002,
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
					Latitude:  ptrFloat32(52.518611),
					Longitude: ptrFloat32(13.408333),
				},
			},
		},
	}
	cityNewYork = &storobj.Object{
		Object: models.Object{
			Class:              "City",
			ID:                 strfmt.UUID("e06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000003,
			LastUpdateTimeUnix: 9100000003,
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
					Latitude:  ptrFloat32(40.716667),
					Longitude: ptrFloat32(-74),
				},
			},
		},
	}
	cityAmsterdam = &storobj.Object{
		Object: models.Object{
			Class:              "City",
			ID:                 strfmt.UUID("a06bb8a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000001,
			LastUpdateTimeUnix: 9100000001,
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
					Latitude:  ptrFloat32(52.366667),
					Longitude: ptrFloat32(4.9),
				},
			},
		},
	}
	cityNil = &storobj.Object{
		Object: models.Object{
			Class:              "City",
			ID:                 strfmt.UUID("f00018a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000004,
			LastUpdateTimeUnix: 9100000004,
			Properties: map[string]interface{}{
				"name": "Nil",
			},
		},
	}
	cityNil2 = &storobj.Object{
		Object: models.Object{
			Class:              "City",
			ID:                 strfmt.UUID("f00028a7-ad67-4774-a9ac-86a04df51cb6"),
			CreationTimeUnix:   9000000005,
			LastUpdateTimeUnix: 9100000005,
			Properties: map[string]interface{}{
				"name": "Nil2",
			},
		},
	}
)

func ptrString(s string) *string {
	return &s
}

func ptrStringArray(sa ...string) *[]string {
	return &sa
}

func ptrFloat32(f float32) *float32 {
	return &f
}

func ptrFloat64(f float64) *float64 {
	return &f
}

func ptrFloat64Array(fa ...float64) *[]float64 {
	return &fa
}

func ptrBool(b bool) *bool {
	return &b
}

func ptrBoolArray(ba ...bool) *[]bool {
	return &ba
}

func ptrTime(s string) *time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return &t
}

func ptrTimeArray(sa ...string) *[]time.Time {
	res := make([]time.Time, len(sa))
	for i := range sa {
		t, _ := time.Parse(time.RFC3339, sa[i])
		res[i] = t
	}
	return &res
}
