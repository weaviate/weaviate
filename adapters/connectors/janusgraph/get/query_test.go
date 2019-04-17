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
 */

package get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common"
	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/get"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

func Test_QueryBuilder(t *testing.T) {
	tests := testCases{
		{
			name: "with a Thing.City with a single primitive prop 'name'",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
				},
				Kind: kind.THING_KIND,
				Pagination: &common.Pagination{
					Limit: 33,
				},
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
				.limit(33).path().by(valueMap())
			`,
		},
		{
			name: "without an explicit limit specified",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
				},
				Kind: kind.THING_KIND,
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
				.limit(20).path().by(valueMap())
			`,
		},
		{
			name: "with a Thing.City with a single primitive prop 'name' and a where filter",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
				},
				Kind: kind.THING_KIND,
				Pagination: &common.Pagination{
					Limit: 33,
				},
				Filters: &cf.LocalFilter{
					Root: &cf.Clause{
						Value: &cf.Value{
							Value: "Amsterdam",
							Type:  schema.DataTypeString,
						},
						On: &cf.Path{
							Class:    schema.ClassName("City"),
							Property: schema.PropertyName("name"),
						},
						Operator: cf.OperatorEqual,
					},
				},
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
			  .union(has("prop_1", eq("Amsterdam")))
				.limit(33).path().by(valueMap())
			`,
		},
		{
			name: "with a Thing.City with a ref prop one level deep",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
					get.SelectProperty{
						IsPrimitive: false,
						Name:        "inCountry",
						Refs: []get.SelectClass{
							get.SelectClass{
								ClassName: "Country",
								RefProperties: []get.SelectProperty{
									get.SelectProperty{
										IsPrimitive: true,
										Name:        "name",
									},
								},
							},
						},
					},
				},
				Kind: kind.THING_KIND,
				Pagination: &common.Pagination{
					Limit: 33,
				},
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
				.union(
				  optional(
						outE("prop_3").inV().hasLabel("class_19")
					)
				)
				.limit(33).path().by(valueMap())
			`,
		},
		{
			name: "with a Thing.City with a network ref prop one level deep",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
					get.SelectProperty{
						IsPrimitive: false,
						Name:        "inCountry",
						Refs: []get.SelectClass{
							get.SelectClass{
								ClassName: "WeaviateB__Country",
								RefProperties: []get.SelectProperty{
									get.SelectProperty{
										IsPrimitive: true,
										Name:        "name",
									},
								},
							},
						},
					},
				},
				Kind: kind.THING_KIND,
				Pagination: &common.Pagination{
					Limit: 33,
				},
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
				.union(
				  optional(
						outE("prop_3").inV()
					)
				)
				.limit(33).path().by(valueMap())
			`,
		},
		{
			name: "with a Thing.City with a ref prop three levels deep",
			inputParams: get.Params{
				ClassName: "City",
				Properties: []get.SelectProperty{
					get.SelectProperty{
						IsPrimitive: true,
						Name:        "name",
					},
					get.SelectProperty{
						IsPrimitive: false,
						Name:        "inCountry",
						Refs: []get.SelectClass{
							get.SelectClass{
								ClassName: "Country",
								RefProperties: []get.SelectProperty{
									get.SelectProperty{
										IsPrimitive: false,
										Name:        "inContinent",
										Refs: []get.SelectClass{
											get.SelectClass{
												ClassName: "Continent",
												RefProperties: []get.SelectProperty{
													get.SelectProperty{
														IsPrimitive: false,
														Name:        "onPlanet",
														Refs: []get.SelectClass{
															get.SelectClass{
																ClassName: "Planet",
																RefProperties: []get.SelectProperty{
																	get.SelectProperty{
																		IsPrimitive: true,
																		Name:        "name",
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Kind: kind.THING_KIND,
				Pagination: &common.Pagination{
					Limit: 33,
				},
			},
			expectedQuery: `
			g.V().has("kind", "thing").hasLabel("class_18")
				.union(
				  optional(outE("prop_3").inV().hasLabel("class_19"))
					.optional( outE("prop_13").inV().hasLabel("class_20"))
					.optional( outE("prop_23").inV().hasLabel("class_21"))
				)
				.limit(33).path().by(valueMap())
			`,
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
