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
	"encoding/json"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func gettingObjectsWithAdditionalProps(t *testing.T) {
	t.Run("with vector set", func(t *testing.T) {
		query := `
		{
			Get {
				Company {
					_additional {
						vector
					}
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		require.Greater(t, len(companies), 0)
		for _, comp := range companies {
			vec, ok := comp.(map[string]interface{})["_additional"].(map[string]interface{})["vector"]
			require.True(t, ok)

			vecSlice, ok := vec.([]interface{})
			require.True(t, ok)
			require.Greater(t, len(vecSlice), 0)

			asFloat, err := vecSlice[0].(json.Number).Float64()
			require.Nil(t, err)
			assert.True(t, asFloat >= -1)
			assert.True(t, asFloat <= 1)
		}
	})

	t.Run("with _additional featureProjection set", func(t *testing.T) {
		query := `
		{
			Get {
				Company {
					_additional {
						featureProjection(dimensions:3){
							vector
						}
					}
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		extractProjections := func(in interface{}) []interface{} {
			return in.(map[string]interface{})["_additional"].(map[string]interface{})["featureProjection"].(map[string]interface{})["vector"].([]interface{})
		}

		projections0 := extractProjections(companies[0])
		projections1 := extractProjections(companies[1])
		projections2 := extractProjections(companies[2])

		validateProjections(t, 3, projections0, projections1, projections2)
	})

	t.Run("with _additional vector set in reference", func(t *testing.T) {
		query := `
		{
			Get {
				City {
					_additional {
						vector
					}
					inCountry {
						... on Country {
							_additional {
								vector
							}
						}
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()

		vector := cities[0].(map[string]interface{})["inCountry"].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["vector"]

		assert.NotNil(t, vector)
	})

	t.Run("with _additional creationTimeUnix and lastUpdateTimeUnix set in reference", func(t *testing.T) {
		query := `
		{
			Get {
				City {
					inCountry {
						... on Country {
							_additional {
								creationTimeUnix
								lastUpdateTimeUnix
							}
						}
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()

		created := cities[0].(map[string]interface{})["inCountry"].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["creationTimeUnix"]
		updated := cities[0].(map[string]interface{})["inCountry"].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["lastUpdateTimeUnix"]

		assert.NotNil(t, created)
		assert.NotNil(t, updated)
	})
}

func validateProjections(t *testing.T, dims int, vectors ...[]interface{}) {
	for i := range vectors {
		if len(vectors[i]) != dims {
			t.Fatalf("expected feature projection vector to have length 3, got: %d", len(vectors[i]))
		}
	}
}
