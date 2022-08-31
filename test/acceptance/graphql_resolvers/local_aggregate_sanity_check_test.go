//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/assert"
)

func getResult(result *graphqlhelper.GraphQLResult, className, resultData, property string) interface{} {
	meta := result.Get("Aggregate", className).AsSlice()[0].(map[string]interface{})[property]
	return meta.(map[string]interface{})[resultData]
}

// run by setup_test.go
func runningAggregateArrayClassSanityCheck(t *testing.T) {
	t.Run("running Aggregate against ArrayClass", func(t *testing.T) {
		query := `
			{
				Aggregate {
					ArrayClass
					%s
					{
						booleans{
							count
						}
						meta{
							count
						}
						datesAsStrings{
							count
						}
						dates{
							count
						}
						numbers{
							count
						}
						strings{
							count
						}
						texts{
							count
						}
						ints{
							count
						}
					}
				}
			}
		`

		tests := []struct {
			name    string
			filters string
		}{
			{
				name: "without filters",
			},
			{
				name:    "with where filter",
				filters: `( where:{operator: Like path:["id"] valueString:"*"} )`,
			},
			{
				name:    "with nearObject filter",
				filters: `( nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.1} )`,
			},
			{
				name: "with where and nearObject filter",
				filters: `(
					where:{operator: Like path:["id"] valueString:"*"}
					nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.1}
				)`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.filters))
				assert.Equal(t, json.Number("4"), getResult(result, "ArrayClass", "count", "meta"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "booleans"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "datesAsStrings"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "dates"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "numbers"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "strings"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "texts"))
				assert.Equal(t, json.Number("6"), getResult(result, "ArrayClass", "count", "ints"))
			})
		}
	})

	t.Run("running Aggregate against empty class", func(t *testing.T) {
		query := `
			{
				Aggregate {
					ClassWithoutProperties
					%s
					{
						meta{
							count
						}
					}
				}
			}
		`

		tests := []struct {
			name    string
			filters string
		}{
			{
				name: "without filters",
			},
			{
				name:    "with where filter",
				filters: `( where:{operator: Like path:["id"] valueString:"*"} )`,
			},
			{
				name:    "with nearObject filter",
				filters: `( nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.1} )`,
			},
			{
				name: "with where and nearObject filter",
				filters: `(
					where:{operator: Like path:["id"] valueString:"*"}
					nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.1}
				)`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.filters))
				assert.Equal(t, json.Number("2"), getResult(result, "ClassWithoutProperties", "count", "meta"))
			})
		}
	})

	t.Run("running Aggregate against City", func(t *testing.T) {
		query := `
			{
			  Aggregate {
				City
				%s
				{
				  meta {
					count
				  }
				  cityArea {
					count
				  }
				  isCapital {
					count
				  }
				  population {
					count
				  }
				  cityRights {
					count
				  }
				  history {
					count
				  }
				}
			  }
			}
		`

		tests := []struct {
			name    string
			filters string
		}{
			{
				name: "without filters",
			},
			{
				name:    "with where filter",
				filters: `( where:{operator: Like path:["id"] valueString:"*"} )`,
			},
			{
				name:    "with nearObject filter",
				filters: `( nearObject:{id: "660db307-a163-41d2-8182-560782cd018f" certainty: 0.1} )`,
			},
			{
				name: "with where and nearObject filter",
				filters: `(
					where:{operator: Like path:["id"] valueString:"*"}
					nearObject:{id: "660db307-a163-41d2-8182-560782cd018f" certainty: 0.1}
				)`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.filters))
				// One city object (null island) has no entries for most properties (except population, isCapital and location)
				assert.Equal(t, json.Number("5"), getResult(result, "City", "count", "meta"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "cityArea"))
				assert.Equal(t, json.Number("5"), getResult(result, "City", "count", "isCapital"))
				assert.Equal(t, json.Number("5"), getResult(result, "City", "count", "population"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "cityRights"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "history"))
			})
		}
	})

	t.Run("running Aggregate with empty filter", func(t *testing.T) {
		types := `
					minimum
					maximum
					median
					mean
					mode
					sum
					count
					type`
		query := `
			{
			  Aggregate {
				ArrayClass(where: {operator: Equal, path: ["id"], valueString: "IDoNotExist"}) {
				  numbers {
					%s
				  }
				  ints {
					%s
				  }
				}
			  }
			}
		`
		tests := []struct {
			name       string
			TypeString string
		}{
			{
				name:       "numbers",
				TypeString: "number[]",
			},
			{
				name:       "ints",
				TypeString: `int[]`,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, types, types))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "minimum", tt.name))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "maximum", tt.name))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "median", tt.name))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "mean", tt.name))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "mode", tt.name))
				assert.Equal(t, nil, getResult(result, "ArrayClass", "sum", tt.name))
				assert.Equal(t, json.Number("0"), getResult(result, "ArrayClass", "count", tt.name))
				assert.Equal(t, tt.TypeString, getResult(result, "ArrayClass", "type", tt.name))
			})
		}
	})
}
