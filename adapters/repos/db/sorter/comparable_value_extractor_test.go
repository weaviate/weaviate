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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}
