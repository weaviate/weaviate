//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
