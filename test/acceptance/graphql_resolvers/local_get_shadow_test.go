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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// run by setup_test.go
func runningGetNearObjectWithShadowedObjects(t *testing.T) {
	t.Run("running Get nearObject against shadow class", func(t *testing.T) {
		query := `
			{
				Get {
					NearObjectSearch (
						nearObject: {
							id : "aa44bbee-ca5f-4db7-a412-5fc6a2300001"
							certainty: 0.98
						}
					) {
						name
					}
				}
			}
		`

		for i := 0; i < 50; i++ {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			objs := result.Get("Get", "NearObjectSearch").AsSlice()

			expected := []interface{}{
				map[string]interface{}{"name": "Mount Everest"},
			}

			assert.Len(t, objs, 1)
			assert.ElementsMatch(t, expected, objs)
		}
	})
}

func runningAggregateNearObjectWithShadowedObjects(t *testing.T) {
	t.Run("running Aggregate nearObject against shadow class", func(t *testing.T) {
		query := `
			{
				Aggregate {
					NearObjectSearch (
						nearObject: {
							id : "aa44bbee-ca5f-4db7-a412-5fc6a2300001"
							certainty: 0.98
						}
					) {
						meta {
							count
						}
					}
				}
			}
		`

		for i := 0; i < 50; i++ {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			meta := result.Get("Aggregate", "NearObjectSearch").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		}
	})
}

func runningExploreNearObjectWithShadowedObjects(t *testing.T) {
	t.Run("running Explore nearObject against shadow class with same contents", func(t *testing.T) {
		query := `
			{
				Explore (
					nearObject: {
						id : "aa44bbee-ca5f-4db7-a412-5fc6a2300011"
						certainty: 0.98
					}
				) {
					beacon
				}
			}
		`

		for i := 0; i < 50; i++ {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			objs := result.Get("Explore").AsSlice()

			expected := []interface{}{
				map[string]interface{}{"beacon": "weaviate://localhost/NearObjectSearch/aa44bbee-ca5f-4db7-a412-5fc6a2300011"},
				map[string]interface{}{"beacon": "weaviate://localhost/NearObjectSearchShadow/aa44bbee-ca5f-4db7-a412-5fc6a2300011"},
			}

			assert.Len(t, objs, 2)
			assert.ElementsMatch(t, expected, objs)
		}
	})

	t.Run("running Explore nearObject against shadow class with different contents", func(t *testing.T) {
		query := `
			{
				Explore (
					nearObject: {
						id : "aa44bbee-ca5f-4db7-a412-5fc6a2300001"
						certainty: 0.98
					}
				) {
					beacon
				}
			}
		`

		for i := 0; i < 50; i++ {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			objs := result.Get("Explore").AsSlice()

			expected := []interface{}{
				map[string]interface{}{"beacon": "weaviate://localhost/NearObjectSearch/aa44bbee-ca5f-4db7-a412-5fc6a2300001"},
				map[string]interface{}{"beacon": "weaviate://localhost/NearObjectSearchShadow/aa44bbee-ca5f-4db7-a412-5fc6a2300001"},
			}

			assert.Len(t, objs, 2)
			assert.ElementsMatch(t, expected, objs)
		}
	})
}
