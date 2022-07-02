package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
)

func addTestDataL2(t *testing.T) {
	createObject(t, &models.Object{
		Class: "L2Squared_Class",
		Properties: map[string]interface{}{
			"name": "object_1",
		},
		Vector: []float32{
			10, 11, 12,
		},
	})

	createObject(t, &models.Object{
		Class: "L2Squared_Class",
		Properties: map[string]interface{}{
			"name": "object_2",
		},
		Vector: []float32{
			13, 15, 17,
		},
	})

	createObject(t, &models.Object{
		Class: "L2Squared_Class",
		Properties: map[string]interface{}{
			"name": "object_3",
		},
		Vector: []float32{
			0, 0, 0, // a zero vecto
		},
	})
}

func testL2(t *testing.T) {
	res := AssertGraphQL(t, nil, `
	{
	  Get{
			L2Squared_Class(nearVector:{vector: [10,11,12]}){
		  	name 
		  	_additional{distance certainty}
		  }
		}
	}
	`)
	results := res.Get("Get", "L2Squared_Class").AsSlice()
	expectedDistances := []float32{
		0,   // the same vector as the query
		50,  // distance to the second vector
		365, // l2 squared distance to the root
	}

	compareDistances(t, expectedDistances, results)
}
