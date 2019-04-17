/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package fetch

import (
	"errors"
	"testing"

	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
)

func Test_QueryBuilder(t *testing.T) {
	tests := testCases{
		{
			name: "with a single class name, single property name, string type",
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
			name: "with the field type not matching the property type",
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
									Name:      "name", // actually is a string prop, should be included
									Certainty: 1.0,
								},
								{
									Name:      "population", // is an int prop, so should not be included
									Certainty: 1.0,
								},
							},
						},
						Match: fetch.PropertyMatch{
							Operator: cf.OperatorEqual,
							Value: &cf.Value{
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
						has("classId", "class_18").has("prop_1", eq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with multiple class/property combinations, correct type",
			inputParams: fetch.Params{
				Kind: kind.THING_KIND,
				PossibleClassNames: contextionary.SearchResults{
					Results: []contextionary.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
						{
							Name:      "Town",
							Certainty: 1.0,
						},
					},
				},
				Properties: []fetch.Property{
					{
						PossibleNames: contextionary.SearchResults{
							Results: []contextionary.SearchResult{
								{
									Name:      "name",
									Certainty: 1.0,
								},
								{
									Name:      "title",
									Certainty: 1.0,
								},
							},
						},
						Match: fetch.PropertyMatch{
							Operator: cf.OperatorEqual,
							Value: &cf.Value{
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
						has("classId", "class_18").has("prop_1", eq("Amsterdam")),
						has("classId", "class_19").has("prop_11", eq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name: "with a single property with no valid combination of class and props",
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
									Name:      "potato",
									Certainty: 1.0,
								},
							},
						},
						Match: fetch.PropertyMatch{
							Operator: cf.OperatorEqual,
							Value: &cf.Value{
								Value: "Amsterdam",
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},
			expectedQuery: "",
			expectedErr: errors.New("could not find a viable combination of class names, " +
				"properties and search filters, try using different classNames or properties, " +
				"lowering the certainty on either of them or change the specified filtering " +
				"requirements (operator or value<Type>)"),
		},
	}

	tests.AssertQuery(t)
}

func singleProp(propName string, dataType schema.DataType, operator cf.Operator,
	searchValue interface{}) []fetch.Property {
	return []fetch.Property{
		{
			PossibleNames: contextionary.SearchResults{
				Results: []contextionary.SearchResult{
					{
						Name:      propName,
						Certainty: 1.0,
					},
				},
			},
			Match: fetch.PropertyMatch{
				Operator: operator,
				Value: &cf.Value{
					Value: searchValue,
					Type:  dataType,
				},
			},
		},
	}
}
