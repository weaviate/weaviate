//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func aggregationWithHybridSearch(t *testing.T) {
	t.Run("without search vector", func(t *testing.T) {
		query := `
		{
			Aggregate {
				Company
				(
					objectLimit: 30
      				hybrid: {
        				alpha: 0.5
        				query: "Apple"
      				}
				)
				{
					name {
						topOccurrences {
							value
						}
					}
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "Company").AsSlice()
		require.Len(t, result, 1)
		topOccur := result[0].(map[string]interface{})["name"].(map[string]interface{})["topOccurrences"].([]interface{})
		require.Len(t, topOccur, 5)
		assert.Contains(t, topOccur, map[string]interface{}{"value": "Apple"})
		assert.Contains(t, topOccur, map[string]interface{}{"value": "Apple Inc."})
		assert.Contains(t, topOccur, map[string]interface{}{"value": "Apple Incorporated"})
		assert.Contains(t, topOccur, map[string]interface{}{"value": "Google"})
		assert.Contains(t, topOccur, map[string]interface{}{"value": "Google Inc."})
	})

	t.Run("with grouping, sparse search only", func(t *testing.T) {
		query := `
		{
			Aggregate {
				Company
    			(
					objectLimit: 30
				  	groupBy: "name"
				  	hybrid: {
						alpha: 0
        				query: "Google"
					}
    			)
				{
					name {
        				topOccurrences {
          					value
        				}
      				}
				}
			}
		}`

		type object = map[string]interface{}

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "Company").AsSlice()
		require.Len(t, result, 3)
		assert.Contains(t, result, object{
			"name": object{
				"topOccurrences": []interface{}{
					object{"value": "Google"},
				},
			},
		})
		assert.Contains(t, result, object{
			"name": object{
				"topOccurrences": []interface{}{
					object{"value": "Google Inc."},
				},
			},
		})
		assert.Contains(t, result, object{
			"name": object{
				"topOccurrences": []interface{}{
					object{"value": "Google Incorporated"},
				},
			},
		})
	})

	t.Run("with grouping, nearText", func(t *testing.T) {
		query := `
		{
			Aggregate {
				Company
    			(
					objectLimit: 30
				  	groupBy: "name"
				  	hybrid: {
						alpha: 0.5
        				query: ""
						searches: {
							nearText: {
								concepts: ["Google"]
							}
						}
					}
    			)
				{
					name {
        				topOccurrences {
          					value
        				}
      				}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with grouping, and nearText, moveTo", func(t *testing.T) {
		query := `
	{
		Aggregate {
			Company
			(
				objectLimit: 30
				  groupBy: "name"
				  hybrid: {
					alpha: 0.5
					query: ""
					searches: {
						nearText: {
							concepts: ["Google"]
							certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
						}
					}
				}
			)
			{
				name {
					topOccurrences {
						  value
					}
				  }
			}
		}
	}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with grouping, nearVector and nearText conflict", func(t *testing.T) {
		query := `
	{
		Aggregate {
			Company
			(
				objectLimit: 30
				  groupBy: "name"
				  hybrid: {
					alpha: 0.5
					query: ""
					searches: {
						nearText: {
							concepts: ["Google"]
							certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
						}
						nearVector: {
							vector: [0.1, 0.2, 0.3]
						}
					}
				}
			)
			{
				name {
					topOccurrences {
						  value
					}
				  }
			}
		}
	}`

		graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
	})
}
