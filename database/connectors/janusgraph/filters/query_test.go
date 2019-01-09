package filters

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_EmptyFilters(t *testing.T) {
	result, err := New(nil).String()

	require.Nil(t, err, "no error should have occurred")
	assert.Equal(t, "", result, "should return an empty string")
}

func Test_SingleIntFilter(t *testing.T) {

	buildFilter := func(propName string, value interface{}, operator cf.Operator) *cf.LocalFilter {
		return &cf.LocalFilter{
			Root: &cf.Clause{
				Operator: operator,
				On: &cf.Path{
					Class:    schema.ClassName("City"),
					Property: schema.PropertyName(propName),
				},
				Value: &cf.Value{
					Value: value,
					Type:  schema.DataTypeInt,
				},
			},
		}
	}

	type testCase struct {
		name           string
		operator       cf.Operator
		expectedResult string
	}

	tests := []testCase{
		{"'City.population == 10000'", cf.OperatorEqual, `.has("population", eq(10000))`},
		{"'City.population != 10000'", cf.OperatorNotEqual, `.has("population", neq(10000))`},
		{"'City.population < 10000'", cf.OperatorLessThan, `.has("population", lt(10000))`},
		{"'City.population <= 10000'", cf.OperatorLessThanEqual, `.has("population", lte(10000))`},
		{"'City.population > 10000'", cf.OperatorGreaterThan, `.has("population", gt(10000))`},
		{"'City.population >= 10000'", cf.OperatorGreaterThanEqual, `.has("population", gte(10000))`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := buildFilter("population", int64(10000), test.operator)

			result, err := New(filter).String()

			require.Nil(t, err, "no error should have occurred")
			assert.Equal(t, test.expectedResult, result, "should form the right query")
		})
	}
}
