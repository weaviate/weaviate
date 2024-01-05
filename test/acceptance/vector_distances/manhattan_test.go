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

	"github.com/weaviate/weaviate/entities/models"
)

func addTestDataManhattan(t *testing.T) {
	createObject(t, &models.Object{
		Class: "Manhattan_Class",
		Properties: map[string]interface{}{
			"name": "object_1",
		},
		Vector: []float32{
			10, 11, 12,
		},
	})

	createObject(t, &models.Object{
		Class: "Manhattan_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			13, 15, 17,
		},
	})

	createObject(t, &models.Object{
		Class: "Manhattan_Class",
		Properties: map[string]interface{}{
			"name": "object_3",
		},
		Vector: []float32{
			0, 0, 0, // a zero vector
		},
	})
}

func testManhattan(t *testing.T) {
	t.Run("without any limiting parameters", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12]}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0,  // the same vector as the query
			12, // distance to the second vector -> abs(10-13)+abs(11-15)+abs(12-17) = 3+4+5 = 12
			33, // manhattan distance to the root -> 10+11+12 = 33
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a certainty arg", func(t *testing.T) {
		// not supported for non-cosine distances
		ErrorGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], certainty:0.3}){
					name 
					_additional{distance}
				}
			}
		}
		`)
	})

	t.Run("with a certainty prop", func(t *testing.T) {
		// not supported for non-cosine distances
		ErrorGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance:0.3}){
					name 
					_additional{certainty}
				}
			}
		}
		`)
	})

	t.Run("a high distance that includes all elements", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance: 365}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0,  // the same vector as the query
			12, // distance to the second vector -> abs(10-13)+abs(11-15)+abs(12-17) = 3+4+5 = 12
			33, // manhattan distance to the root -> 10+11+12 = 33
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance that is too low for the last element", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance: 30}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0,  // the same vector as the query
			12, // distance to the second vector -> abs(10-13)+abs(11-15)+abs(12-17) = 3+4+5 = 12
			// last skipped, as 33 > 30
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance that is too low for the second element", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance: 10}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			// second elem skipped, because 12 > 10
			// last eleme skipped, because 33 > 10
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a really low distance that only matches one elem", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance: 0.001}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			// second elem skipped, because 12 > 0.001
			// last eleme skipped, because 33 > 0.001
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance of 0 only matches exact elements", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Manhattan_Class(nearVector:{vector: [10,11,12], distance: 0}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Manhattan_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			// second elem skipped, because 12 > 0
			// last eleme skipped, because 33 > 0
		}

		compareDistances(t, expectedDistances, results)
	})
}
