package filterext

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ExtractFlatFilters(t *testing.T) {
	t.Parallel()

	input := &models.WhereFilter{
		Operator: "Equal",
		ValueInt: ptInt(42),
		Path:     []string{"intField"},
	}

	expected := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("Todo"),
			Property: schema.AssertValidPropertyName("intField"),
		},
		Value: &filters.Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	actual, err := Parse(input)
	require.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func ptInt(in int) *int64 {
	a := int64(in)
	return &a
}
