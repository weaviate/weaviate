//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// appleVec returns the vector of the "Apple Inc." company, see addTestDataCompanies
func appleVec(t *testing.T) []float32 {
	obj, err := helper.GetObject(t, "Company", "477fec91-1292-4928-8f53-f0ff49c76900", "vector")
	require.NoError(t, err)
	require.NotEmpty(t, obj.Vector)
	return obj.Vector
}

func getWithHybridSearch(t *testing.T) {
	t.Run("without references", func(t *testing.T) {
		query := `
		{
  			Get {
    			Airport
    			(
      				hybrid: {
        				alpha: 0
        				query: "10000"
      				}
				)
    			{
      				code
    			}
  			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Airport").AsSlice()
		require.Len(t, result, 1)
		assert.EqualValues(t, map[string]interface{}{"code": "10000"}, result[0])
	})

	t.Run("with limit and vector", func(t *testing.T) {
		limit := 2
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					limit: %d
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
						vector: %s
					}
				) {
					name
				}
			}
		}`, limit, graphqlhelper.Vec2String(appleVec(t)))
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, limit)
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple",
		})
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple Inc.",
		})
	})

	t.Run("with limit and no vector", func(t *testing.T) {
		limit := 2
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					limit: %d
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					name
				}
			}
		}`, limit)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, limit)
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple",
		})
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple Inc.",
		})
	})

	t.Run("with no limit and vector", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
						vector: %s
					}
				) {
					name
				}
			}
		}`, graphqlhelper.Vec2String(appleVec(t)))
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with no limit and no vector", func(t *testing.T) {
		query := `
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					name
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with _additional{vector}", func(t *testing.T) {
		query := `
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					_additional {
						vector
					}
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
		for _, res := range result {
			company := res.(map[string]interface{})
			addl := company["_additional"].(map[string]interface{})
			vec, found := addl["vector"]
			assert.True(t, found)
			assert.Len(t, vec, len(appleVec(t)))
		}
	})

	t.Run("with references", func(t *testing.T) {
		query := `
		{
  			Get {
    			Airport
    			(
      				hybrid: {
        				alpha: 0.5
        				query: "1000"
      				}
				)
    			{
      				code
      				inCity {
        				... on City {
          					name
        				}
      				}
    			}
  			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Airport").AsSlice()
		require.Len(t, result, 4)
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "10000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "20000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "30000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Dusseldorf"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "40000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Berlin"},
				},
			})
	})
}
