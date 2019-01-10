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

func Test_SingleProperties(t *testing.T) {
	type testCase struct {
		name           string
		operator       cf.Operator
		expectedResult string
	}

	t.Run("with propertyType Int", func(t *testing.T) {
		t.Run("with various operators and valid values", func(t *testing.T) {
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
					filter := buildFilter("population", int64(10000), test.operator, schema.DataTypeInt)

					result, err := New(filter).String()

					require.Nil(t, err, "no error should have occurred")
					assert.Equal(t, test.expectedResult, result, "should form the right query")
				})
			}
		})

		t.Run("an invalid value", func(t *testing.T) {
			filter := buildFilter("population", "200", cf.OperatorEqual, schema.DataTypeInt)

			_, err := New(filter).String()

			require.NotNil(t, err, "it should error due to the wrong type")
		})
	})

	t.Run("with propertyType Number (float)", func(t *testing.T) {
		t.Run("with various operators and valid values", func(t *testing.T) {
			tests := []testCase{
				{"'City.energyConsumption == 953.280000'", cf.OperatorEqual, `.has("energyConsumption", eq(953.280000))`},
				{"'City.energyConsumption != 953.280000'", cf.OperatorNotEqual, `.has("energyConsumption", neq(953.280000))`},
				{"'City.energyConsumption < 953.280000'", cf.OperatorLessThan, `.has("energyConsumption", lt(953.280000))`},
				{"'City.energyConsumption <= 953.280000'", cf.OperatorLessThanEqual, `.has("energyConsumption", lte(953.280000))`},
				{"'City.energyConsumption > 953.280000'", cf.OperatorGreaterThan, `.has("energyConsumption", gt(953.280000))`},
				{"'City.energyConsumption >= 953.280000'", cf.OperatorGreaterThanEqual, `.has("energyConsumption", gte(953.280000))`},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					filter := buildFilter("energyConsumption", float64(953.28), test.operator, schema.DataTypeNumber)

					result, err := New(filter).String()

					require.Nil(t, err, "no error should have occurred")
					assert.Equal(t, test.expectedResult, result, "should form the right query")
				})
			}
		})

		t.Run("an invalid value", func(t *testing.T) {
			filter := buildFilter("population", "200", cf.OperatorEqual, schema.DataTypeNumber)

			_, err := New(filter).String()

			require.NotNil(t, err, "it should error due to the wrong type")
		})
	})
}

func Test_InvalidOperator(t *testing.T) {
	filter := buildFilter("population", "200", cf.Operator(27), schema.DataTypeInt)

	_, err := New(filter).String()

	require.NotNil(t, err, "it should error due to the wrong type")
}

func buildFilter(propName string, value interface{}, operator cf.Operator, schemaType schema.DataType) *cf.LocalFilter {
	return &cf.LocalFilter{
		Root: &cf.Clause{
			Operator: operator,
			On: &cf.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName(propName),
			},
			Value: &cf.Value{
				Value: value,
				Type:  schemaType,
			},
		},
	}
}
