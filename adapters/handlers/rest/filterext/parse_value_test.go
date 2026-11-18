//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|  \__,_|\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filterext

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// Regression test for weaviate/weaviate#13567: an explicitly-set-but-empty
// value<Type>Array must be rejected with an accurate message ("value array
// '<field>' is empty"), not the misleading "no value<Type> field set" that
// suggests the field was never provided at all.
func Test_Parse_EmptyValueArrayAccurateError(t *testing.T) {
	t.Parallel()

	emptyArrays := map[string]*models.WhereFilter{
		"valueInt":     {Operator: "ContainsAny", ValueIntArray: []int64{}},
		"valueNumber":  {Operator: "ContainsAny", ValueNumberArray: []float64{}},
		"valueText":    {Operator: "ContainsAny", ValueTextArray: []string{}},
		"valueDate":    {Operator: "ContainsAny", ValueDateArray: []string{}},
		"valueBoolean": {Operator: "ContainsAny", ValueBooleanArray: []bool{}},
		"valueString":  {Operator: "ContainsAny", ValueStringArray: []string{}},
	}

	for field, input := range emptyArrays {
		input.Path = []string{"nums"}
		t.Run("empty "+field, func(t *testing.T) {
			_, err := Parse(input, "Todo", namespacing.Disabled, nil)
			assert.EqualError(t, err, "invalid where filter: got operator 'ContainsAny', "+
				"but value array '"+field+"' is empty")
		})
	}

	t.Run("empty array with ContainsAll", func(t *testing.T) {
		_, err := Parse(&models.WhereFilter{
			Operator:      "ContainsAll",
			Path:          []string{"nums"},
			ValueIntArray: []int64{},
		}, "Todo", namespacing.Disabled, nil)
		assert.EqualError(t, err, "invalid where filter: got operator 'ContainsAll', "+
			"but value array 'valueInt' is empty")
	})

	t.Run("absent value fields keep the generic error", func(t *testing.T) {
		// No value field set at all (nil arrays): the original generic
		// message is still accurate and must not change.
		_, err := Parse(&models.WhereFilter{
			Operator: "ContainsAny",
			Path:     []string{"nums"},
		}, "Todo", namespacing.Disabled, nil)
		assert.EqualError(t, err, "invalid where filter: got operator 'ContainsAny', "+
			"but no value<Type> field set")
	})

	t.Run("non-empty arrays still parse", func(t *testing.T) {
		filter, err := Parse(&models.WhereFilter{
			Operator:      "ContainsAny",
			Path:          []string{"nums"},
			ValueIntArray: []int64{1, 2},
		}, "Todo", namespacing.Disabled, nil)
		assert.NoError(t, err)
		assert.Equal(t, &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.ContainsAny,
			On: &filters.Path{
				Class:    schema.AssertValidClassName("Todo"),
				Property: schema.AssertValidPropertyName("nums"),
			},
			Value: &filters.Value{
				Value: []int{1, 2},
				Type:  schema.DataTypeInt,
			},
		}}, filter)
	})
}
