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
