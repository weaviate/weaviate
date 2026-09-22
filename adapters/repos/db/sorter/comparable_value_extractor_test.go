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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestComparableValueExtractor(t *testing.T) {
	schema := getMyFavoriteClassSchemaForTests()
	class := schema.GetClass(testClassName)
	helper := newDataTypesHelper(class)
	extractor := newComparableValueExtractor(helper)
	object := createMyFavoriteClassObject()

	params := []struct {
		propName string
		expected interface{}
	}{
		{
			"id",
			ptrString("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		},
		{
			"_creationTimeUnix",
			ptrFloat64(900000000001),
		},
		{
			"_lastUpdateTimeUnix",
			ptrFloat64(900000000002),
		},
		{
			"textProp",
			ptrString("text"),
		},
		{
			"textPropArray",
			ptrStringArray("text", "text"),
		},
		{
			"intProp",
			ptrFloat64(100),
		},
		{
			"numberProp",
			ptrFloat64(17),
		},
		{
			"intPropArray",
			ptrFloat64Array(10, 20, 30),
		},
		{
			"numberPropArray",
			ptrFloat64Array(1, 2, 3),
		},
		{
			"boolProp",
			ptrBool(true),
		},
		{
			"boolPropArray",
			ptrBoolArray(true, false, true),
		},
		{
			"dateProp",
			ptrTime("1980-01-01T00:00:00+02:00"),
		},
		{
			"datePropArray",
			ptrTimeArray("1980-01-01T00:00:00+02:00"),
		},
		{
			"phoneProp",
			ptrFloat64Array(49, 1000000),
		},
		{
			"geoProp",
			ptrFloat64Array(1, 2),
		},
		{
			"emptyStringProp",
			nil,
		},
		{
			"emptyBoolProp",
			nil,
		},
		{
			"emptyNumberProp",
			nil,
		},
		{
			"emptyIntProp",
			nil,
		},
		{
			"crefProp",
			nil,
		},
		{
			"nonExistentProp",
			nil,
		},
	}

	t.Run("extract comparable values from binary", func(t *testing.T) {
		objData, err := object.MarshalBinary()
		require.Nil(t, err)

		for _, p := range params {
			t.Run(fmt.Sprintf("data %s", p.propName), func(t *testing.T) {
				assert.Equal(t, p.expected, extractor.extractFromBytes(objData, p.propName))
			})
		}
	})

	t.Run("extract comparable values from object", func(t *testing.T) {
		for _, p := range params {
			t.Run(fmt.Sprintf("data %s", p.propName), func(t *testing.T) {
				assert.Equal(t, p.expected, extractor.extractFromObject(object, p.propName))
			})
		}
	})

	// A raw JSON-decoded property map carries arrays as []interface{} and
	// nested objects as map[string]interface{}; extraction must yield the same
	// comparables as the typed shapes (this used to panic on every array case).
	// Merge/patch-built property maps and empty arrays surface these shapes
	// even though parseObject's enrichment types most stored values.
	t.Run("extract comparable values from JSON-decoded object", func(t *testing.T) {
		decoded := createMyFavoriteClassObjectJSONDecoded()
		for _, p := range params {
			t.Run(fmt.Sprintf("data %s", p.propName), func(t *testing.T) {
				assert.Equal(t, p.expected, extractor.extractFromObject(decoded, p.propName))
			})
		}
	})

	// The shapes a real storage read produces: MarshalBinary/FromBinary runs
	// enrichSchemaTypes, which types non-empty arrays and converts phone/geo
	// maps to model pointers.
	t.Run("extract comparable values from storage round-trip", func(t *testing.T) {
		roundTripped := roundTripThroughStorage(t, object)
		for _, p := range params {
			t.Run(fmt.Sprintf("data %s", p.propName), func(t *testing.T) {
				assert.Equal(t, p.expected, extractor.extractFromObject(roundTripped, p.propName))
			})
		}
	})

	// The request validator replaces date properties with typed values
	// (time.Time / []time.Time), so a freshly written in-memory object carries
	// those shapes; they must extract, not read as missing.
	t.Run("extract typed date shapes of a validated object", func(t *testing.T) {
		date, err := time.Parse(time.RFC3339, "1980-01-01T00:00:00+02:00")
		require.Nil(t, err)
		validated := storobj.FromObject(
			&models.Object{
				Class: testClassName,
				ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{
					"dateProp":      date,
					"datePropArray": []time.Time{date},
				},
			},
			[]float32{1, 2, 0.7},
			nil,
			nil,
		)
		assert.Equal(t, ptrTime("1980-01-01T00:00:00+02:00"), extractor.extractFromObject(validated, "dateProp"))
		assert.Equal(t, ptrTimeArray("1980-01-01T00:00:00+02:00"), extractor.extractFromObject(validated, "datePropArray"))
	})

	// Enrichment cannot type an empty array, so it stays []interface{} on a
	// storage read; it must extract as an empty typed slice, both directly and
	// after a round-trip (this used to panic).
	t.Run("empty arrays extract as empty typed slices", func(t *testing.T) {
		empty := storobj.FromObject(
			&models.Object{
				Class: testClassName,
				ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{
					"textPropArray":   []interface{}{},
					"numberPropArray": []interface{}{},
					"boolPropArray":   []interface{}{},
					"datePropArray":   []interface{}{},
				},
			},
			[]float32{1, 2, 0.7},
			nil,
			nil,
		)
		for _, obj := range map[string]*storobj.Object{
			"direct":     empty,
			"round-trip": roundTripThroughStorage(t, empty),
		} {
			assert.Equal(t, &[]string{}, extractor.extractFromObject(obj, "textPropArray"))
			assert.Equal(t, &[]float64{}, extractor.extractFromObject(obj, "numberPropArray"))
			assert.Equal(t, &[]bool{}, extractor.extractFromObject(obj, "boolPropArray"))
			assert.Equal(t, &[]time.Time{}, extractor.extractFromObject(obj, "datePropArray"))
		}
	})
}

// roundTripThroughStorage reproduces a storage read: parseObject runs
// enrichSchemaTypes over the JSON-decoded properties.
func roundTripThroughStorage(t *testing.T, obj *storobj.Object) *storobj.Object {
	t.Helper()
	data, err := obj.MarshalBinary()
	require.Nil(t, err)
	parsed, err := storobj.FromBinaryDisk(data, obj.Class().String())
	require.Nil(t, err)
	return parsed
}

// TestComparableValueExtractorWrongShapes pins that a property whose value
// does not match its schema type extracts as nil — the same as a missing
// property — instead of panicking the query.
func TestComparableValueExtractorWrongShapes(t *testing.T) {
	schema := getMyFavoriteClassSchemaForTests()
	class := schema.GetClass(testClassName)
	helper := newDataTypesHelper(class)
	extractor := newComparableValueExtractor(helper)

	tests := []struct {
		name     string
		propName string
		value    interface{}
	}{
		{"nil text", "textProp", nil},
		{"mixed text array", "textPropArray", []interface{}{"text", float64(1)}},
		{"string as number", "intProp", "not a number"},
		{"string element in number array", "numberPropArray", []interface{}{"not a number"}},
		{"string as bool", "boolProp", "not a bool"},
		{"number as bool array", "boolPropArray", float64(7)},
		{"nil date", "dateProp", nil},
		{"malformed date string", "dateProp", "not-a-date"},
		{"nil element in date array", "datePropArray", []interface{}{nil}},
		{"malformed element in date array", "datePropArray", []interface{}{"not-a-date"}},
		{"nil phone pointer", "phoneProp", (*models.PhoneNumber)(nil)},
		{"empty phone map", "phoneProp", map[string]interface{}{}},
		{"wrong-typed phone map", "phoneProp", map[string]interface{}{"countryCode": "49", "national": "1000000"}},
		{"string as geo", "geoProp", "not a geo"},
		{"empty geo map", "geoProp", map[string]interface{}{}},
		{"wrong-typed geo map", "geoProp", map[string]interface{}{"longitude": "1", "latitude": "2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := storobj.FromObject(
				&models.Object{
					Class:      testClassName,
					ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
					Properties: map[string]interface{}{tt.propName: tt.value},
				},
				[]float32{1, 2, 0.7},
				nil,
				nil,
			)
			assert.Nil(t, extractor.extractFromObject(object, tt.propName))
		})
	}
}
