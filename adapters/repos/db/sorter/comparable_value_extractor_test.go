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

	// An object unmarshaled from storage carries arrays as []interface{} and
	// nested objects as map[string]interface{}; extraction must yield the same
	// comparables as the typed shapes (this used to panic on every array case).
	t.Run("extract comparable values from JSON-decoded object", func(t *testing.T) {
		decoded := createMyFavoriteClassObjectJSONDecoded()
		for _, p := range params {
			t.Run(fmt.Sprintf("data %s", p.propName), func(t *testing.T) {
				assert.Equal(t, p.expected, extractor.extractFromObject(decoded, p.propName))
			})
		}
	})
}

// TestComparableValueExtractorWrongShapes pins that a property whose value
// does not match its schema type extracts as nil — the same as a missing
// property — instead of panicking the query.
func TestComparableValueExtractorWrongShapes(t *testing.T) {
	schema := getMyFavoriteClassSchemaForTests()
	class := schema.GetClass(testClassName)
	helper := newDataTypesHelper(class)
	extractor := newComparableValueExtractor(helper)

	object := storobj.FromObject(
		&models.Object{
			Class:              testClassName,
			CreationTimeUnix:   900000000001,
			LastUpdateTimeUnix: 900000000002,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{
				"textProp":        nil,
				"textPropArray":   []interface{}{"text", float64(1)},
				"intProp":         "not a number",
				"numberPropArray": []interface{}{"not a number"},
				"boolProp":        "not a bool",
				"boolPropArray":   float64(7),
				"dateProp":        nil,
				"datePropArray":   []interface{}{nil},
				"phoneProp":       (*models.PhoneNumber)(nil),
				"geoProp":         "not a geo",
			},
		},
		[]float32{1, 2, 0.7},
		nil,
		nil,
	)

	for _, propName := range []string{
		"textProp", "textPropArray", "intProp", "numberPropArray",
		"boolProp", "boolPropArray", "dateProp", "datePropArray",
		"phoneProp", "geoProp",
	} {
		t.Run(propName, func(t *testing.T) {
			assert.Nil(t, extractor.extractFromObject(object, propName))
		})
	}
}
