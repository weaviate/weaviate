/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package fetch

import (
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func Test_QueryBuilder(t *testing.T) {
	tests := testCases{
		{
			name: "with a single class name, single property name, string type",
			inputParams: kinds.FetchParams{
				Kind: kind.Thing,
				PossibleClassNames: kinds.SearchResults{
					Results: []kinds.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
					},
				},
				Properties: singleProp("name", schema.DataTypeString, filters.OperatorEqual, "Amsterdam"),
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
			inputParams: kinds.FetchParams{
				Kind: kind.Thing,
				PossibleClassNames: kinds.SearchResults{
					Results: []kinds.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
					},
				},
				Properties: singleProp("population", schema.DataTypeInt, filters.OperatorEqual, 2000),
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
			inputParams: kinds.FetchParams{
				Kind: kind.Thing,
				PossibleClassNames: kinds.SearchResults{
					Results: []kinds.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
					},
				},
				Properties: []kinds.FetchProperty{
					{
						PossibleNames: kinds.SearchResults{
							Results: []kinds.SearchResult{
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
						Match: kinds.FetchPropertyMatch{
							Operator: filters.OperatorEqual,
							Value: &filters.Value{
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
			inputParams: kinds.FetchParams{
				Kind: kind.Thing,
				PossibleClassNames: kinds.SearchResults{
					Results: []kinds.SearchResult{
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
				Properties: []kinds.FetchProperty{
					{
						PossibleNames: kinds.SearchResults{
							Results: []kinds.SearchResult{
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
						Match: kinds.FetchPropertyMatch{
							Operator: filters.OperatorEqual,
							Value: &filters.Value{
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
			inputParams: kinds.FetchParams{
				Kind: kind.Thing,
				PossibleClassNames: kinds.SearchResults{
					Results: []kinds.SearchResult{
						{
							Name:      "City",
							Certainty: 1.0,
						},
					},
				},
				Properties: []kinds.FetchProperty{
					{
						PossibleNames: kinds.SearchResults{
							Results: []kinds.SearchResult{
								{
									Name:      "potato",
									Certainty: 1.0,
								},
							},
						},
						Match: kinds.FetchPropertyMatch{
							Operator: filters.OperatorEqual,
							Value: &filters.Value{
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

func singleProp(propName string, dataType schema.DataType, operator filters.Operator,
	searchValue interface{}) []kinds.FetchProperty {
	return []kinds.FetchProperty{
		{
			PossibleNames: kinds.SearchResults{
				Results: []kinds.SearchResult{
					{
						Name:      propName,
						Certainty: 1.0,
					},
				},
			},
			Match: kinds.FetchPropertyMatch{
				Operator: operator,
				Value: &filters.Value{
					Value: searchValue,
					Type:  dataType,
				},
			},
		},
	}
}
