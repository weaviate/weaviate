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

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/test/helper"
)

func gettingObjectsWithGeoFilters(t *testing.T) {
	t.Run("Only Dusseldorf should be within 100km of Dusseldorf", func(t *testing.T) {
		query := `
		{
			Get {
				City(where:{
					operator: WithinGeoRange
					path: ["location"]
					valueGeoRange: { geoCoordinates: {latitude: 51.225556, longitude: 6.782778} distance: { max: 100000 } }
				}){
					name
					location {
						latitude
						longitude
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()

		expectedResults := []interface{}{
			map[string]interface{}{
				"name": "Dusseldorf",
				"location": map[string]interface{}{
					"latitude":  json.Number("51.225555"),
					"longitude": json.Number("6.782778"),
				},
			},
		}

		assert.Equal(t, expectedResults, cities)
	})

	t.Run("Dusseldorf and Amsterdam should be within 200km of Dusseldorf", func(t *testing.T) {
		query := `
		{
			Get {
				City(where:{
					operator: WithinGeoRange
					path: ["location"]
					valueGeoRange: { geoCoordinates: {latitude: 51.225556, longitude: 6.782778} distance: { max: 200000 } }
				}){
					name
					location {
						latitude
						longitude
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()

		expectedResults := []interface{}{
			map[string]interface{}{
				"name": "Dusseldorf",
				"location": map[string]interface{}{
					"latitude":  json.Number("51.225555"),
					"longitude": json.Number("6.782778"),
				},
			},
			map[string]interface{}{
				"name": "Amsterdam",
				"location": map[string]interface{}{
					"latitude":  json.Number("52.36667"),
					"longitude": json.Number("4.9"),
				},
			},
		}

		assert.ElementsMatch(t, expectedResults, cities)
	})

	// This test prevents a regression on gh-825
	t.Run("Missing island is displayed correctly", func(t *testing.T) {
		query := `
		{
			Get {
				City(where:{
					operator: WithinGeoRange
					path: ["location"]
					valueGeoRange: { geoCoordinates: {latitude: 0, longitude: 0} distance: { max: 20 } }
				}){
					name
					location {
						latitude
						longitude
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()

		expectedResults := []interface{}{
			map[string]interface{}{
				"name": "Missing Island",
				"location": map[string]interface{}{
					"latitude":  json.Number("0"),
					"longitude": json.Number("0"),
				},
			},
		}

		assert.ElementsMatch(t, expectedResults, cities)
	})
}
