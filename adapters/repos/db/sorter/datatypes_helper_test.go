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
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func TestDataTypesHelper(t *testing.T) {
	sch := getMyFavoriteClassSchemaForTests()
	class := sch.GetClass(testClassName)
	helper := newDataTypesHelper(class)

	t.Run("get data types as strings", func(t *testing.T) {
		params := []struct {
			propName string
			expected []string
		}{
			{"stringProp", []string{string(schema.DataTypeString)}},
			{"textProp", []string{string(schema.DataTypeText)}},
			{"stringPropArray", []string{string(schema.DataTypeStringArray)}},
			{"textPropArray", []string{string(schema.DataTypeTextArray)}},
			{"intProp", []string{string(schema.DataTypeInt)}},
			{"numberProp", []string{string(schema.DataTypeNumber)}},
			{"intPropArray", []string{string(schema.DataTypeIntArray)}},
			{"numberPropArray", []string{string(schema.DataTypeNumberArray)}},
			{"boolProp", []string{string(schema.DataTypeBoolean)}},
			{"boolPropArray", []string{string(schema.DataTypeBooleanArray)}},
			{"dateProp", []string{string(schema.DataTypeDate)}},
			{"datePropArray", []string{string(schema.DataTypeDateArray)}},
			{"phoneProp", []string{string(schema.DataTypePhoneNumber)}},
			{"geoProp", []string{string(schema.DataTypeGeoCoordinates)}},
			{"crefProp", []string{string(schema.DataTypeCRef)}},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, helper.getStrings(p.propName))
			})
		}
	})

	t.Run("get data types as type", func(t *testing.T) {
		params := []struct {
			propName string
			expected schema.DataType
		}{
			{"stringProp", schema.DataTypeString},
			{"textProp", schema.DataTypeText},
			{"stringPropArray", schema.DataTypeStringArray},
			{"textPropArray", schema.DataTypeTextArray},
			{"intProp", schema.DataTypeInt},
			{"numberProp", schema.DataTypeNumber},
			{"intPropArray", schema.DataTypeIntArray},
			{"numberPropArray", schema.DataTypeNumberArray},
			{"boolProp", schema.DataTypeBoolean},
			{"boolPropArray", schema.DataTypeBooleanArray},
			{"dateProp", schema.DataTypeDate},
			{"datePropArray", schema.DataTypeDateArray},
			{"phoneProp", schema.DataTypePhoneNumber},
			{"geoProp", schema.DataTypeGeoCoordinates},
			{"crefProp", schema.DataTypeCRef},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, helper.getType(p.propName))
			})
		}
	})
}
