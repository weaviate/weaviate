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

func addTestDataHamming(t *testing.T) {
	createObject(t, &models.Object{
		Class: "Hamming_Class",
		Properties: map[string]interface{}{
			"name": "object_1",
		},
		Vector: []float32{
			10, 10, 10,
		},
	})

	createObject(t, &models.Object{
		Class: "Hamming_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			10, 10, 12,
		},
	})

	createObject(t, &models.Object{
		Class: "Hamming_Class",
		Properties: map[string]interface{}{
			"name": "object_3",
		},
		Vector: []float32{
			10, 11, 12,
		},
	})
}

func testHamming(t *testing.T) {
	t.Run("without any limiting parameters", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Hamming_Class(nearVector:{vector: [10,10,10]}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Hamming_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			1, // hamming distance to object_3 vector
			2, // hamming distance to object_4 vector
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a certainty arg", func(t *testing.T) {
		// not supported for non-cosine distances
		ErrorGraphQL(t, nil, `
		{
			Get{
				Hamming_Class(nearVector:{vector: [10,11,12], certainty:0.3}){
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
				Hamming_Class(nearVector:{vector: [10,11,12], distance:0.3}){
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
				Hamming_Class(nearVector:{vector: [10,10,10], distance: 365}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Hamming_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			1, // hamming distance to object_3 vector
			2, // hamming distance to object_4 vector
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance that is too low for the last element", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Hamming_Class(nearVector:{vector: [10,10,10], distance: 1.5}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Hamming_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			1, // hamming distance to object_3 vector
			// hamming distance to object_4 vector skipped because 2>1
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance that is too low for the second element", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Hamming_Class(nearVector:{vector: [10,10,10], distance: 0.5}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Hamming_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			// hamming distance to object_3 vector skipped because 1>0.5
			// hamming distance to object_4 vector skipped because 2>0.5
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("a distance of 0 only matches exact elements", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Get{
				Hamming_Class(nearVector:{vector: [10,10,10], distance: 0}){
					name 
					_additional{distance}
				}
			}
		}
		`)
		results := res.Get("Get", "Hamming_Class").AsSlice()
		expectedDistances := []float32{
			0, // the same vector as the query
			// second elem skipped, because 1 > 0
			// last eleme skipped, because 2 > 0
		}

		compareDistances(t, expectedDistances, results)
	})
}
