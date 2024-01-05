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

func addTestDataDot(t *testing.T) {
	createObject(t, &models.Object{
		Class: "Dot_Class",
		Properties: map[string]interface{}{
			"name": "object_1",
		},
		Vector: []float32{
			3, 4, 5, // our base object
		},
	})

	createObject(t, &models.Object{
		Class: "Dot_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			1, 1, 1, // a length-one vector
		},
	})

	createObject(t, &models.Object{
		Class: "Dot_Class",
		Properties: map[string]interface{}{
			"name": "object_3",
		},
		Vector: []float32{
			0, 0, 0, // a zero vecto
		},
	})

	createObject(t, &models.Object{
		Class: "Dot_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			-3, -4, -5, // negative of the base vector
		},
	})
}

func testDot(t *testing.T) {
	t.Run("without any limiting distance", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	 Get{
			Dot_Class(nearVector:{vector: [3,4,5]}){
		  	name
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			-50, // the same vector as the query
			-12, // the same angle as the query vector,
			0,   // the vector in between,
			50,  // the negative of the query vec
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a specified certainty arg - should error", func(t *testing.T) {
		ErrorGraphQL(t, nil, `
	{
	  Get{
			Dot_Class(nearVector:{certainty: 0.7, vector: [3,4,5]}){
		  	name 
		  	_additional{distance}
		  }
		}
	}
	`)
	})

	t.Run("with a specified certainty prop - should error", func(t *testing.T) {
		ErrorGraphQL(t, nil, `
	{
	  Get{
			Dot_Class(nearVector:{distance: 0.7, vector: [3,4,5]}){
		  	name 
		  	_additional{certainty}
		  }
		}
	}
	`)
	})

	t.Run("with a max distancer higher than all results, should contain all elements", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	 Get{
			Dot_Class(nearVector:{distance: 50, vector: [3,4,5]}){
		  	name
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			-50, // the same vector as the query
			-12, // the same angle as the query vector,
			0,   // the vector in between,
			50,  // the negative of the query vec
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a positive max distance that does not match all results, should contain 3 elems", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	 Get{
			Dot_Class(nearVector:{distance: 30, vector: [3,4,5]}){
		  	name
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			-50, // the same vector as the query
			-12, // the same angle as the query vector,
			0,   // the vector in between,
			// the last one is not contained as it would have a distance of 50, which is > 30
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with distance 0, should contain 3 elems", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	 Get{
			Dot_Class(nearVector:{distance: 0, vector: [3,4,5]}){
		  	name
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			-50, // the same vector as the query
			-12, // the same angle as the query vector,
			0,   // the vector in between,
			// the last one is not contained as it would have a distance of 50, which is > 0
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a negative distance that should only leave the first element", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	  Get{
			Dot_Class(nearVector:{distance: -40, vector: [3,4,5]}){
		  	name 
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			-50, // the same vector as the query
			// the second element's distance would be -12 which is > -40
			// the third element's distance would be 0 which is > -40
			// the last one is not contained as it would have a distance of 50, which is > 0
		}

		compareDistances(t, expectedDistances, results)
	})

	t.Run("with a distance so small that no element should be left", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
	{
	 Get{
			Dot_Class(nearVector:{distance: -60, vector: [3,4,5]}){
		  	name
		  	_additional{distance}
		  }
		}
	}
	`)
		results := res.Get("Get", "Dot_Class").AsSlice()
		expectedDistances := []float32{
			// all elements have a distance > -60, so nothing matches
		}

		compareDistances(t, expectedDistances, results)
	})
}
