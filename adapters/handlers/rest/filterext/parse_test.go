//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package filterext

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func Test_ExtractFlatFilters(t *testing.T) {
	t.Parallel()

	type test struct {
		name           string
		input          *models.WhereFilter
		expectedFilter *filters.LocalFilter
		expectedErr    error
	}

	tests := []test{
		test{
			name: "valid int filter",
			input: &models.WhereFilter{
				Operator: "Equal",
				ValueInt: ptInt(42),
				Path:     []string{"intField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("intField"),
				},
				Value: &filters.Value{
					Value: 42,
					Type:  schema.DataTypeInt,
				},
			}},
		},
		test{
			name: "valid string filter",
			input: &models.WhereFilter{
				Operator:    "Equal",
				ValueString: ptString("foo bar"),
				Path:        []string{"stringField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("stringField"),
				},
				Value: &filters.Value{
					Value: "foo bar",
					Type:  schema.DataTypeString,
				},
			}},
		},
		test{
			name: "valid date filter",
			input: &models.WhereFilter{
				Operator:  "Equal",
				ValueDate: ptString("foo bar"),
				Path:      []string{"dateField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("dateField"),
				},
				Value: &filters.Value{
					Value: "foo bar",
					Type:  schema.DataTypeDate,
				},
			}},
		},
		test{
			name: "valid text filter",
			input: &models.WhereFilter{
				Operator:  "Equal",
				ValueText: ptString("foo bar"),
				Path:      []string{"textField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("textField"),
				},
				Value: &filters.Value{
					Value: "foo bar",
					Type:  schema.DataTypeText,
				},
			}},
		},
		test{
			name: "valid number filter",
			input: &models.WhereFilter{
				Operator:    "Equal",
				ValueNumber: ptFloat(20.20),
				Path:        []string{"numberField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("numberField"),
				},
				Value: &filters.Value{
					Value: 20.20,
					Type:  schema.DataTypeNumber,
				},
			}},
		},
		test{
			name: "valid bool filter",
			input: &models.WhereFilter{
				Operator:     "Equal",
				ValueBoolean: ptBool(true),
				Path:         []string{"booleanField"},
			},
			expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Todo"),
					Property: schema.AssertValidPropertyName("booleanField"),
				},
				Value: &filters.Value{
					Value: true,
					Type:  schema.DataTypeBoolean,
				},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter, err := Parse(test.input)
			assert.Equal(t, test.expectedErr, err)
			assert.Equal(t, test.expectedFilter, filter)
		})
	}
}

func ptInt(in int) *int64 {
	a := int64(in)
	return &a
}

func ptFloat(in float64) *float64 {
	return &in
}

func ptString(in string) *string {
	return &in
}

func ptBool(in bool) *bool {
	return &in
}
