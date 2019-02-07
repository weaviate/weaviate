package fetch

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
)

func Test_QueryBuilder(t *testing.T) {
	tests := testCases{
		{
			name: "with a single class name, single property name, correct type",
			inputParams: fetch.Params{
				Kind: kind.THING_KIND,
				PossibleClassNames: contextionary.SearchResults{
					Results: []contextionary.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
					},
				},
				Properties: []fetch.Property{
					{
						PossibleNames: contextionary.SearchResults{
							Results: []contextionary.SearchResult{
								{
									Name:      "population",
									Certainty: 1.0,
								},
							},
						},
						Match: fetch.PropertyMatch{
							Operator: common_filters.OperatorEqual,
							Value: &common_filters.Value{
								Value: "Amsterdam",
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", eq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}
