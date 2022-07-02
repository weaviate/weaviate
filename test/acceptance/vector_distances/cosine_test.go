package test

import (
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		0.0715, // the vector in betwwen,
		2,      // the perfect opposite vector,
	}

	require.Equal(t, len(expectedDistances), len(results))
	for i, expected := range expectedDistances {
		actual, err := results[i].(map[string]interface{})["_additional"].(map[string]interface{})["distance"].(json.Number).Float64()
		require.Nil(t, err)
		assert.InDelta(t, expected, actual, 0.01)
	}
}
