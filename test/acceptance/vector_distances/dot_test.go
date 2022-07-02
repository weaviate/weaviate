package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
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
	res := AssertGraphQL(t, nil, `
	{
	  Get{
			Dot_Class(nearVector:{vector: [3,4,5]}){
		  	name 
		  	_additional{distance certainty}
		  }
		}
	}
	`)
	results := res.Get("Get", "Dot_Class").AsSlice()
	expectedDistances := []float32{
		-50, // the same vector as the query
		-12, // the same angle as the query vector,
		0,   // the vector in betwwen,
		50,  // the negative of the query vec
	}

	compareDistances(t, expectedDistances, results)
}
