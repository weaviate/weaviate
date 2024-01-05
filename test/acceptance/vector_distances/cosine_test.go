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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func addTestDataCosine(t *testing.T) {
	createObject(t, &models.Object{
		Class: "Cosine_Class",
		Properties: map[string]interface{}{
			"name": "object_1",
		},
		Vector: []float32{
			0.7, 0.3, // our base object
		},
	})

	createObject(t, &models.Object{
		Class: "Cosine_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			1.4, 0.6, // identical angle to the base
		},
	})

	createObject(t, &models.Object{
		Class: "Cosine_Class",
		Properties: map[string]interface{}{
			"name": "object_3",
		},
		Vector: []float32{
			-0.7, -0.3, // perfect opposite of the base
		},
	})

	createObject(t, &models.Object{
		Class: "Cosine_Class",
		Properties: map[string]interface{}{
			"name": "object_4",
		},
		Vector: []float32{
			1, 1, // somewhere in between
		},
	})
}

func testCosine(t *testing.T) {
	t.Run("without any limiting parameters", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	Get{
			Cosine_Class(nearVector:{vector: [0.7, 0.3]}){
		  	name
		  	_additional{distance certainty}
		  }
		}
	}
	`)
		results := res.Get("Get", "Cosine_Class").AsSlice()
		expectedDistances := []float32{
			0,      // the same vector as the query
			0,      // the same angle as the query vector,
			0.0715, // the vector in between,
			2,      // the perfect opposite vector,
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("limiting by certainty", func(t *testing.T) {
		// cosine is a special case. It still supports certainty for legacy
		// reasons. All other distances do not work with certainty.

		t.Run("Get: with certainty=0 meaning 'match anything'", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], certainty: 0}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				2,      // the perfect opposite vector,
			}

			compareDistances(t, expectedDistances, results)

			expectedCertainties := []float32{
				1,    // the same vector as the query
				1,    // the same angle as the query vector,
				0.96, // the vector in between,
				0,    // the perfect opposite vector,
			}

			compareCertainties(t, expectedCertainties, results)
		})

		t.Run("Explore: with certainty=0 meaning 'match anything'", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], certainty: 0}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				2,      // the perfect opposite vector,
			}

			compareDistancesExplore(t, expectedDistances, results)

			expectedCertainties := []float32{
				1,    // the same vector as the query
				1,    // the same angle as the query vector,
				0.96, // the vector in between,
				0,    // the perfect opposite vector,
			}

			compareCertaintiesExplore(t, expectedCertainties, results)
		})

		t.Run("Get: with certainty=0.95", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], certainty: 0.95}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
			}

			compareDistances(t, expectedDistances, results)

			expectedCertainties := []float32{
				1,    // the same vector as the query
				1,    // the same angle as the query vector,
				0.96, // the vector in between,
				// the last element does not have the required certainty (0<0.95)
			}

			compareCertainties(t, expectedCertainties, results)
		})

		t.Run("Explore: with certainty=0.95", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], certainty: 0.95}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
			}

			compareDistancesExplore(t, expectedDistances, results)

			expectedCertainties := []float32{
				1,    // the same vector as the query
				1,    // the same angle as the query vector,
				0.96, // the vector in between,
				// the last element does not have the required certainty (0<0.95)
			}

			compareCertaintiesExplore(t, expectedCertainties, results)
		})

		t.Run("Get: with certainty=0.97", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], certainty: 0.97}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
			}

			compareDistances(t, expectedDistances, results)

			expectedCertainties := []float32{
				1, // the same vector as the query
				1, // the same angle as the query vector,
				// the last two elements would have certainty of 0.96 and 0, so they won't match
			}

			compareCertainties(t, expectedCertainties, results)
		})

		t.Run("Explore: with certainty=0.97", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], certainty: 0.97}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
			}

			compareDistancesExplore(t, expectedDistances, results)

			expectedCertainties := []float32{
				1, // the same vector as the query
				1, // the same angle as the query vector,
				// the last two elements would have certainty of 0.96 and 0, so they won't match
			}

			compareCertaintiesExplore(t, expectedCertainties, results)
		})

		t.Run("Get: with certainty=1", func(t *testing.T) {
			// only perfect matches should be included now (certainty=1, distance=0)
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], certainty: 1}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
			}

			compareDistances(t, expectedDistances, results)

			expectedCertainties := []float32{
				1, // the same vector as the query
				1, // the same angle as the query vector,
				// the last two elements would have certainty of 0.96 and 0, so they won't match
			}

			compareCertainties(t, expectedCertainties, results)
		})

		t.Run("Explore: with certainty=1", func(t *testing.T) {
			// only perfect matches should be included now (certainty=1, distance=0)
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], certainty: 1}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
			}

			compareDistancesExplore(t, expectedDistances, results)

			expectedCertainties := []float32{
				1, // the same vector as the query
				1, // the same angle as the query vector,
				// the last two elements would have certainty of 0.96 and 0, so they won't match
			}

			compareCertaintiesExplore(t, expectedCertainties, results)
		})
	})

	t.Run("limiting by distance", func(t *testing.T) {
		t.Run("Get: with distance=2, i.e. max distance, should match all", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], distance: 2}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				2,      // the perfect opposite vector,
			}

			compareDistances(t, expectedDistances, results)
		})

		t.Run("Explore: with distance=2, i.e. max distance, should match all", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], distance: 2}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				2,      // the perfect opposite vector,
			}

			compareDistancesExplore(t, expectedDistances, results)
		})

		t.Run("Get: with distance=1.99, should exclude the last", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], distance: 1.99}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				// the vector with the perfect opposite has a distance of 2.00 which is > 1.99
			}

			compareDistances(t, expectedDistances, results)
		})

		t.Run("Explore: with distance=1.99, should exclude the last", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], distance: 1.99}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between,
				// the vector with the perfect opposite has a distance of 2.00 which is > 1.99
			}

			compareDistancesExplore(t, expectedDistances, results)
		})

		t.Run("Get: with distance=0.08, it should barely still match element 3", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], distance: 0.08}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between, just within the allowed range
				// the vector with the perfect opposite has a distance of 2.00 which is > 0.08
			}

			compareDistances(t, expectedDistances, results)
		})

		t.Run("Explore: with distance=0.08, it should barely still match element 3", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], distance: 0.08}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0,      // the same vector as the query
				0,      // the same angle as the query vector,
				0.0715, // the vector in between, just within the allowed range
				// the vector with the perfect opposite has a distance of 2.00 which is > 0.08
			}

			compareDistancesExplore(t, expectedDistances, results)
		})

		t.Run("Get: with distance=0.01, most vectors are excluded", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], distance: 0.01}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
				// the third vector would have had a distance of 0.07... which is more than 0.01
				// the vector with the perfect opposite has a distance of 2.00 which is > 0.08
			}

			compareDistances(t, expectedDistances, results)
		})

		t.Run("Explore: with distance=0.01, most vectors are excluded", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], distance: 0.01}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
				// the third vector would have had a distance of 0.07... which is more than 0.01
				// the vector with the perfect opposite has a distance of 2.00 which is > 0.08
			}

			compareDistancesExplore(t, expectedDistances, results)
		})

		t.Run("Get: with distance=0, only perfect matches are allowed", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Get{
					Cosine_Class(nearVector:{vector: [0.7, 0.3], distance: 0}){
						name
						_additional{distance certainty}
					}
				}
			}
			`)
			results := res.Get("Get", "Cosine_Class").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
				// only the first two vectors are perfect matches
			}

			compareDistances(t, expectedDistances, results)
		})

		t.Run("Explore: with distance=0, only perfect matches are allowed", func(t *testing.T) {
			res := AssertGraphQL(t, nil, `
			{
				Explore(nearVector:{vector: [0.7, 0.3], distance: 0}){
					distance certainty
				}
			}
			`)
			results := res.Get("Explore").AsSlice()
			expectedDistances := []float32{
				0, // the same vector as the query
				0, // the same angle as the query vector,
				// only the first two vectors are perfect matches
			}

			compareDistancesExplore(t, expectedDistances, results)
		})
	})
}

func compareDistances(t *testing.T, expectedDistances []float32, results []interface{}) {
	require.Equal(t, len(expectedDistances), len(results))
	for i, expected := range expectedDistances {
		actual, err := results[i].(map[string]interface{})["_additional"].(map[string]interface{})["distance"].(json.Number).Float64()
		require.Nil(t, err)
		assert.InDelta(t, expected, actual, 0.01)
	}
}

func compareDistancesExplore(t *testing.T, expectedDistances []float32, results []interface{}) {
	require.Equal(t, len(expectedDistances), len(results))
	for i, expected := range expectedDistances {
		actual, err := results[i].(map[string]interface{})["distance"].(json.Number).Float64()
		require.Nil(t, err)
		assert.InDelta(t, expected, actual, 0.01)
	}
}

// unique to cosine for legacy reasons
func compareCertainties(t *testing.T, expectedDistances []float32, results []interface{}) {
	require.Equal(t, len(expectedDistances), len(results))
	for i, expected := range expectedDistances {
		actual, err := results[i].(map[string]interface{})["_additional"].(map[string]interface{})["certainty"].(json.Number).Float64()
		require.Nil(t, err)
		assert.InDelta(t, expected, actual, 0.01)
	}
}

// unique to cosine for legacy reasons
func compareCertaintiesExplore(t *testing.T, expectedDistances []float32, results []interface{}) {
	require.Equal(t, len(expectedDistances), len(results))
	for i, expected := range expectedDistances {
		actual, err := results[i].(map[string]interface{})["certainty"].(json.Number).Float64()
		require.Nil(t, err)
		assert.InDelta(t, expected, actual, 0.01)
	}
}
