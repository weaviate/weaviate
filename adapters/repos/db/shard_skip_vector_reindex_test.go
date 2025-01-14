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

package db

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestGeoPropsEqual(t *testing.T) {
	type testCase struct {
		prevProps     map[string]interface{}
		nextProps     map[string]interface{}
		expectedEqual bool
	}

	ptrFloat32 := func(f float32) *float32 {
		return &f
	}

	testCases := []testCase{
		{
			prevProps:     map[string]interface{}{},
			nextProps:     map[string]interface{}{},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"notGeo": "abc",
			},
			nextProps: map[string]interface{}{
				"notGeo": "def",
			},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"geo": nil,
			},
			nextProps: map[string]interface{}{
				"geo": nil,
			},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo": nil,
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps:     map[string]interface{}{},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": nil,
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(-1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(-2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(-1.23),
					Longitude: ptrFloat32(-2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"notGeo": "string",
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"notGeo": "otherString",
			},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(4.56),
					Longitude: ptrFloat32(5.67),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(4.56),
					Longitude: ptrFloat32(5.67),
				},
			},
			expectedEqual: true,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(4.56),
					Longitude: ptrFloat32(5.67),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(4.56),
					Longitude: ptrFloat32(5.67),
				},
			},
			nextProps: map[string]interface{}{
				"geo": &models.GeoCoordinates{
					Latitude:  ptrFloat32(1.23),
					Longitude: ptrFloat32(2.34),
				},
				"geo2": &models.GeoCoordinates{
					Latitude:  ptrFloat32(4.56),
					Longitude: ptrFloat32(-5.67),
				},
			},
			expectedEqual: false,
		},
		{
			prevProps: map[string]interface{}{
				"geoLike": map[string]interface{}{
					"Latitude":  ptrFloat32(1.23),
					"Longitude": ptrFloat32(2.34),
				},
			},
			nextProps: map[string]interface{}{
				"geoLike": map[string]interface{}{
					"Latitude":  ptrFloat32(1.23),
					"Longitude": ptrFloat32(2.34),
				},
			},
			expectedEqual: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			eq := geoPropsEqual(tc.prevProps, tc.nextProps)

			if tc.expectedEqual {
				assert.True(t, eq)
			} else {
				assert.False(t, eq)
			}
		})
	}
}

func TestPropsEqual(t *testing.T) {
	type testCase struct {
		prevProps     map[string]interface{}
		nextProps     map[string]interface{}
		expectedEqual bool
	}

	_uuid := func(i int) uuid.UUID {
		b := [16]byte{}
		binary.BigEndian.PutUint64(b[:8], 0)
		binary.BigEndian.PutUint64(b[8:], 1234567890+uint64(i))
		return uuid.UUID(b)
	}
	_uuidAsText := func(i int) string {
		u, _ := _uuid(i).MarshalText()
		return string(u)
	}
	_date := func(i int) time.Time {
		return time.Unix(int64(1704063600+i), 0)
	}
	_dateAsText := func(i int) string {
		d, _ := _date(i).MarshalText()
		return string(d)
	}
	_text := func(i int) string {
		return fmt.Sprintf("text%d", i)
	}
	ptrFloat32 := func(f float32) *float32 {
		return &f
	}

	createPrevProps := func(i int) map[string]interface{} {
		f := float64(i)
		return map[string]interface{}{
			"int":      f,
			"number":   f + 0.5,
			"text":     _text(i),
			"boolean":  i%2 == 0,
			"uuid":     _uuidAsText(i),
			"date":     _dateAsText(i),
			"ints":     []float64{f + 1, f + 2, f + 3},
			"numbers":  []float64{f + 1.5, f + 2.5, f + 3.5},
			"texts":    []string{_text(i + 1), _text(i + 2), _text(i + 3)},
			"booleans": []bool{i%2 != 0, i%2 == 0},
			"uuids":    []string{_uuidAsText(i + 1), _uuidAsText(i + 2), _uuidAsText(i + 3)},
			"dates":    []string{_dateAsText(i + 1), _dateAsText(i + 2), _dateAsText(i + 3)},
			"phone": &models.PhoneNumber{
				DefaultCountry: "pl",
				Input:          fmt.Sprintf("%d", 100_000_000+i),
			},
			"geo": &models.GeoCoordinates{
				Latitude:  ptrFloat32(45.67),
				Longitude: ptrFloat32(-12.34),
			},
			"object": map[string]interface{}{
				"n_int":     f + 10,
				"n_number":  f + 10.5,
				"n_text":    _text(i + 10),
				"n_boolean": i%2 == 0,
				"n_uuid":    _uuidAsText(i + 10),
				"n_date":    _dateAsText(i + 10),
				"n_object": map[string]interface{}{
					"nn_int": f + 20,
				},
			},
			"objects": []interface{}{
				map[string]interface{}{
					"n_ints":     []float64{f + 11, f + 12, f + 13},
					"n_numbers":  []float64{f + 11.5, f + 12.5, f + 13.5},
					"n_texts":    []string{_text(i + 11), _text(i + 12), _text(i + 13)},
					"n_booleans": []bool{i%2 != 0, i%2 == 0},
					"n_uuids":    []string{_uuidAsText(i + 11), _uuidAsText(i + 12), _uuidAsText(i + 13)},
					"n_dates":    []string{_dateAsText(i + 11), _dateAsText(i + 12), _dateAsText(i + 13)},
					"n_objects": []interface{}{
						map[string]interface{}{
							"nn_ints": []float64{f + 21, f + 22, f + 23},
						},
					},
				},
			},
		}
	}
	createNextProps := func(i int) map[string]interface{} {
		props := createPrevProps(i)
		props["uuid"] = _uuid(i)
		props["date"] = _date(i)
		props["uuids"] = []uuid.UUID{_uuid(i + 1), _uuid(i + 2), _uuid(i + 3)}
		props["dates"] = []time.Time{_date(i + 1), _date(i + 2), _date(i + 3)}

		obj := props["object"].(map[string]interface{})
		obj["n_uuid"] = _uuid(i + 10)
		obj["n_date"] = _date(i + 10)

		objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
		objs0["n_uuids"] = []uuid.UUID{_uuid(i + 11), _uuid(i + 12), _uuid(i + 13)}
		objs0["n_dates"] = []time.Time{_date(i + 11), _date(i + 12), _date(i + 13)}

		return props
	}

	prevProps := createPrevProps(1)
	nextProps := createNextProps(1)
	testCases := []testCase{
		{
			prevProps:     nil,
			nextProps:     nil,
			expectedEqual: true,
		},
		{
			prevProps:     prevProps,
			nextProps:     nil,
			expectedEqual: false,
		},
		{
			prevProps:     nil,
			nextProps:     nextProps,
			expectedEqual: false,
		},
		{
			prevProps:     prevProps,
			nextProps:     nextProps,
			expectedEqual: true,
		},
		{
			prevProps:     prevProps,
			nextProps:     createNextProps(2),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["int"] = float64(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["number"] = float64(1000.5)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["text"] = _text(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["boolean"] = true
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["uuid"] = _uuid(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["date"] = _date(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["ints"] = []float64{1000, 1001}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["numbers"] = []float64{1000.5, 1001.5}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["texts"] = []string{_text(1000), _text(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["booleans"] = []bool{false, true}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["uuids"] = []uuid.UUID{_uuid(1000), _uuid(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["dates"] = []time.Time{_date(1000), _date(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["phone"] = &models.PhoneNumber{
					DefaultCountry: "pl",
					Input:          fmt.Sprintf("%d", 123_456_789),
				}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				props["geo"] = &models.GeoCoordinates{
					Latitude:  ptrFloat32(45.67),
					Longitude: ptrFloat32(12.34),
				}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_int"] = float64(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_number"] = float64(1000.5)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_text"] = _text(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_boolean"] = true
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_uuid"] = _uuid(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				obj["n_date"] = _date(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				obj := props["object"].(map[string]interface{})
				nobj := obj["n_object"].(map[string]interface{})
				nobj["nn_int"] = float64(1000)
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_ints"] = []float64{1000, 1001}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_numbers"] = []float64{1000.5, 1001.5}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_texts"] = []string{_text(1000), _text(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_booleans"] = []bool{false, true}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_uuids"] = []uuid.UUID{_uuid(1000), _uuid(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				objs0["n_dates"] = []time.Time{_date(1000), _date(1001)}
				return props
			}(),
			expectedEqual: false,
		},
		{
			prevProps: prevProps,
			nextProps: func() map[string]interface{} {
				props := createNextProps(1)
				objs0 := props["objects"].([]interface{})[0].(map[string]interface{})
				nobjs0 := objs0["n_objects"].([]interface{})[0].(map[string]interface{})
				nobjs0["nn_ints"] = []float64{1000, 1001}
				return props
			}(),
			expectedEqual: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			eq := propsEqual(tc.prevProps, tc.nextProps)

			if tc.expectedEqual {
				assert.True(t, eq)
			} else {
				assert.False(t, eq)
			}
		})
	}
}

func TestTargetVectorsEqual(t *testing.T) {
	vec1 := []float32{1, 2, 3}
	vec2 := []float32{2, 3, 4}
	vec3 := []float32{3, 4, 5}
	vec4 := []float32{4, 5, 6}

	type testCase struct {
		prevVecs      map[string][]float32
		nextVecs      map[string][]float32
		expectedEqual bool
	}

	testCases := []testCase{
		{
			prevVecs:      nil,
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{},
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{},
			nextVecs:      map[string][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{"vec": vec1},
			nextVecs:      nil,
			expectedEqual: false,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec": vec1},
			nextVecs:      map[string][]float32{},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{},
			nextVecs:      map[string][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec": nil},
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][]float32{"vec": nil},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{"vec": nil},
			nextVecs:      map[string][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{},
			nextVecs:      map[string][]float32{"vec": nil},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{"vec": vec1},
			nextVecs:      map[string][]float32{"vec": nil},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec": nil},
			nextVecs:      map[string][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec4": vec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec": vec1},
			nextVecs:      map[string][]float32{"vec": vec2},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": vec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": vec4},
			nextVecs:      map[string][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			expectedEqual: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			eq := targetVectorsEqual(tc.prevVecs, tc.nextVecs)

			if tc.expectedEqual {
				assert.True(t, eq)
			} else {
				assert.False(t, eq)
			}
		})
	}
}

func TestTargetMultiVectorsEqual(t *testing.T) {
	vec1 := [][]float32{{1, 2, 3}}
	vec2 := [][]float32{{2, 3, 4}}
	vec3 := [][]float32{{3, 4, 5}}
	vec4 := [][]float32{{4, 5, 6}}
	complexVec1 := [][]float32{{1, 2, 3}, {1, 2, 3}}
	complexVec2 := [][]float32{{2, 3, 4}, {2, 3, 4}}
	complexVec3 := [][]float32{{3, 4, 5}, {33, 44, 55}, {333, 444, 555}}
	complexVec4 := [][]float32{{4, 5, 6}, {44, 5, 6}, {444, 5, 6}, {444, 5555, 6}, {7, 8, 9}}

	type testCase struct {
		prevVecs      map[string][][]float32
		nextVecs      map[string][][]float32
		expectedEqual bool
	}

	testCases := []testCase{
		{
			prevVecs:      nil,
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{},
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{},
			nextVecs:      map[string][][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{"vec": vec1},
			nextVecs:      nil,
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": complexVec4},
			nextVecs:      nil,
			expectedEqual: false,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": vec1},
			nextVecs:      map[string][][]float32{},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": complexVec4},
			nextVecs:      map[string][][]float32{},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{},
			nextVecs:      map[string][][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": nil},
			nextVecs:      nil,
			expectedEqual: true,
		},
		{
			prevVecs:      nil,
			nextVecs:      map[string][][]float32{"vec": nil},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{"vec": nil},
			nextVecs:      map[string][][]float32{},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{},
			nextVecs:      map[string][][]float32{"vec": nil},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{"vec": vec1},
			nextVecs:      map[string][][]float32{"vec": nil},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": nil},
			nextVecs:      map[string][][]float32{"vec": vec1},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec4": vec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": vec1},
			nextVecs:      map[string][][]float32{"vec": vec2},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			nextVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": vec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": vec4},
			nextVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": vec1},
			nextVecs:      map[string][][]float32{"vec": complexVec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": complexVec1},
			nextVecs:      map[string][][]float32{"vec": complexVec4},
			expectedEqual: false,
		},
		{
			prevVecs:      map[string][][]float32{"vec": complexVec4},
			nextVecs:      map[string][][]float32{"vec": complexVec4},
			expectedEqual: true,
		},
		{
			prevVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": complexVec1, "vec5": complexVec2, "vec6": complexVec3, "vec7": complexVec4},
			nextVecs:      map[string][][]float32{"vec1": vec1, "vec2": vec2, "vec3": vec3, "vec4": complexVec1, "vec5": complexVec2, "vec6": complexVec3, "vec7": complexVec4},
			expectedEqual: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			eq := targetMultiVectorsEqual(tc.prevVecs, tc.nextVecs)

			if tc.expectedEqual {
				assert.True(t, eq)
			} else {
				assert.False(t, eq)
			}
		})
	}
}
