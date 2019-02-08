package fetch

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
)

func Test_QueryBuilder_StringAllOperators(t *testing.T) {
	tests := testCases{
		{
			name: "string Equal",
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
				Properties: singleProp("name", schema.DataTypeString, cf.OperatorEqual, "Amsterdam"),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_1", eq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "string NotEqual",
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
				Properties: singleProp("name", schema.DataTypeString, cf.OperatorNotEqual, "Amsterdam"),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_1", neq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}

func Test_QueryBuilder_IntAllOperators(t *testing.T) {
	tests := testCases{
		{
			name: "with a single class name, single property name, int type, operator Equal",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorEqual, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", eq(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single class name, single property name, int type, operator NotEqual",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorNotEqual, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", neq(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single class name, single property name, int type, operator LessThan",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorLessThan, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", lt(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single class name, single property name, int type, operator GreaterThan",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorGreaterThan, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", gt(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single class name, single property name, int type, operator LessThanEqual",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorLessThanEqual, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", lte(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single class name, single property name, int type, operator GreaterThanEqual",
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
				Properties: singleProp("population", schema.DataTypeInt, cf.OperatorGreaterThanEqual, 2000),
			},
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", gte(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}
