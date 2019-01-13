package getmeta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name            string
	query           string
	expectedProps   []MetaProperty
	resolverReturn  interface{}
	expectedResults []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func Test_Resolve(t *testing.T) {
	t.Parallel()

	tests := testCases{
		testCase{
			name:  "single prop: average",
			query: "{ GetMeta { Things { Car { horsepower { average } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []StatisticalAnalysis{Average},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"average": 275.7773,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "average"},
				expectedValue: 275.7773,
			}},
		},

		testCase{
			name:  "two props: highest, lowest, remaining int props",
			query: "{ GetMeta { Things { Car { horsepower { highest, lowest, count, sum } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []StatisticalAnalysis{Highest, Lowest, Count, Sum},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"highest": 610.0,
					"lowest":  89.0,
					"count":   23,
					"sum":     6343.0,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "highest"},
				expectedValue: 610.0,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "lowest"},
				expectedValue: 89.0,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "count"},
				expectedValue: 23,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "sum"},
				expectedValue: 6343.0,
			}},
		},

		testCase{
			name: "all props on a bool field",
			query: `{ GetMeta { Things { Car { stillInProduction { 
					count, totalTrue, totalFalse, percentageTrue, percentageFalse 
				} } } } }`,
			expectedProps: []MetaProperty{
				{
					Name:                "stillInProduction",
					StatisticalAnalyses: []StatisticalAnalysis{Count, TotalTrue, TotalFalse, PercentageTrue, PercentageFalse},
				},
			},
			resolverReturn: map[string]interface{}{
				"stillInProduction": map[string]interface{}{
					"totalTrue":       20,
					"totalFalse":      30,
					"percentageTrue":  0.4,
					"percentageFalse": 0.6,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "totalTrue"},
				expectedValue: 20,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "totalFalse"},
				expectedValue: 30,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "percentageTrue"},
				expectedValue: 0.4,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "percentageFalse"},
				expectedValue: 0.6,
			}},
		},

		testCase{
			name:  "single prop: string",
			query: "{ GetMeta { Things { Car { modelName { topOccurrences { value, occurs } } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "modelName",
					StatisticalAnalyses: []StatisticalAnalysis{TopOccurrencesValue, TopOccurrencesOccurs},
				},
			},
			resolverReturn: map[string]interface{}{
				"modelName": map[string]interface{}{
					"topOccurrences": []map[string]interface{}{
						{"value": "CheapNSlow", "occurs": 3},
						{"value": "FastNPricy", "occurs": 2},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"GetMeta", "Things", "Car", "modelName", "topOccurrences"},
				expectedValue: []interface{}{
					map[string]interface{}{"value": "CheapNSlow", "occurs": 3},
					map[string]interface{}{"value": "FastNPricy", "occurs": 2},
				},
			}},
		},

		testCase{
			name:  "single prop: date",
			query: "{ GetMeta { Things { Car { startOfProduction { topOccurrences { value, occurs } } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "startOfProduction",
					StatisticalAnalyses: []StatisticalAnalysis{TopOccurrencesValue, TopOccurrencesOccurs},
				},
			},
			resolverReturn: map[string]interface{}{
				"startOfProduction": map[string]interface{}{
					"topOccurrences": []map[string]interface{}{
						{"value": "some-timestamp", "occurs": 3},
						{"value": "another-timestamp", "occurs": 2},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"GetMeta", "Things", "Car", "startOfProduction", "topOccurrences"},
				expectedValue: []interface{}{
					map[string]interface{}{"value": "some-timestamp", "occurs": 3},
					map[string]interface{}{"value": "another-timestamp", "occurs": 2},
				},
			}},
		},
	}

	tests.AssertExtraction(t, kind.THING_KIND, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver()

			expectedParams := &Params{
				Kind:       k,
				ClassName:  schema.ClassName(className),
				Properties: testCase.expectedProps,
			}

			resolver.On("LocalGetMeta", expectedParams).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}
