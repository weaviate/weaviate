package test

import (
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/require"
)

func searchNeighbors(t *testing.T) {

	listParams := things.NewThingsListParams().WithInclude(ptString("_nearestNeighbors"))
	res, err := helper.Client(t).Things.ThingsList(listParams, nil)
	require.Nil(t, err, "should not error")

	extractNeighbor := func(in *models.Thing) []interface{} {
		// marshalling to JSON and back into an untyped map to make sure we assert
		// on the actual JSON structure. This way if we accidentaly change the
		// goswagger generation so it affects both the client and the server in the
		// same way, this test should catch it
		b, err := json.Marshal(in)
		require.Nil(t, err)

		var untyped map[string]interface{}
		err = json.Unmarshal(b, &untyped)
		require.Nil(t, err)

		return untyped["_nearestNeighbors"].(map[string]interface{})["neighbors"].([]interface{})
	}

	validateNeighbors(t, extractNeighbor(res.Payload.Things[0]), extractNeighbor(res.Payload.Things[1]))
}

func ptString(in string) *string {
	return &in
}

func validateNeighbors(t *testing.T, neighborsGroups ...[]interface{}) {
	for i, group := range neighborsGroups {
		if len(group) == 0 {
			t.Fatalf("group %d: length of neighbors is 0", i)
		}

		for j, neighbor := range group {
			asMap := neighbor.(map[string]interface{})
			if len(asMap["concept"].(string)) == 0 {
				t.Fatalf("group %d: element %d: concept has length 0", i, j)
			}
		}
	}
}
