/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package common_filters

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
)

// Basic test on filter
func TestExtractFilterToplevelField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()
	/*localfilter is a struct containing a clause struct
		type Clause struct {
		Operator Operator
		On       *Path
		Value    *Value
		Operands []Clause
	}*/
	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("intField"),
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ SomeAction(where: { path: ["intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterNestedField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("hasAction"),
			Child: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ SomeAction(where: { path: ["SomeAction", "HasAction", "SomeAction", "intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractOperand(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorAnd,
		Operands: []Clause{Clause{
			Operator: OperatorEqual,
			On: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
			Value: &Value{
				Value: 42,
				Type:  schema.DataTypeInt,
			},
		},
			Clause{
				Operator: OperatorEqual,
				On: &Path{
					Class:    schema.AssertValidClassName("SomeAction"),
					Property: schema.AssertValidPropertyName("hasAction"),
					Child: &Path{
						Class:    schema.AssertValidClassName("SomeAction"),
						Property: schema.AssertValidPropertyName("intField"),
					},
				},
				Value: &Value{
					Value: 4242,
					Type:  schema.DataTypeInt,
				},
			},
		}}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := `{ SomeAction(where: { operator: And, operands: [
      { operator: Equal, valueInt: 42,   path: ["SomeAction", "intField"]},
      { operator: Equal, valueInt: 4242, path: ["SomeAction", "HasAction", "SomeAction", "intField"] }
    ]}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractCompareOpFailsIfOperandPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ SomeAction(where: { operator: Equal, operands: []}) }`
	resolver.AssertFailToResolve(t, query)
}

func TestExtractOperandFailsIfPathPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ SomeAction(where: { path:["should", "not", "be", "present"], operator: And  })}`
	resolver.AssertFailToResolve(t, query)
}
