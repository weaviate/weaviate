package getmeta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
)

type testCase struct {
	name          string
	query         string
	expectedProps []MetaProperty
}

type testCases []testCase

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
		},

		testCase{
			name:  "single prop: highest",
			query: "{ GetMeta { Things { Car { horsepower { highest } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []StatisticalAnalysis{Highest},
				},
			},
		},

		testCase{
			name:  "single prop: lowest",
			query: "{ GetMeta { Things { Car { horsepower { lowest } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []StatisticalAnalysis{Lowest},
				},
			},
		},

		testCase{
			name:  "multiple prop: count, sum",
			query: "{ GetMeta { Things { Car { horsepower { count, sum } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []StatisticalAnalysis{Count, Sum},
				},
			},
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
		},

		testCase{
			name:  "single prop: string",
			query: "{ GetMeta { Things { Car { modelName { topOccurrences { value } } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "modelName",
					StatisticalAnalyses: []StatisticalAnalysis{TopOccurrencesValues},
				},
			},
		},

		testCase{
			name:  "single prop: date",
			query: "{ GetMeta { Things { Car { startOfProduction { topOccurrences { value } } } } } }",
			expectedProps: []MetaProperty{
				{
					Name:                "startOfProduction",
					StatisticalAnalyses: []StatisticalAnalysis{TopOccurrencesValues},
				},
			},
		},
	}

	tests.AssertExtraction(t, kind.THING_KIND, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			resolver := newMockResolver()

			expectedParams := &Params{
				Kind:       k,
				ClassName:  schema.ClassName(className),
				Properties: testCase.expectedProps,
			}

			resolver.On("LocalGetMeta", expectedParams).
				Return(map[string]interface{}{}, nil).Once()

			resolver.AssertResolve(t, testCase.query)
		})
	}
}
