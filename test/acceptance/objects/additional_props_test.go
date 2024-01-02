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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func searchNeighbors(t *testing.T) {
	listParams := objects.NewObjectsListParams().WithInclude(ptString("nearestNeighbors"))
	res, err := helper.Client(t).Objects.ObjectsList(listParams, nil)
	require.Nil(t, err, "should not error")

	extractNeighbor := func(in *models.Object) []interface{} {
		// marshalling to JSON and back into an untyped map to make sure we assert
		// on the actual JSON structure. This way if we accidentally change the
		// goswagger generation so it affects both the client and the server in the
		// same way, this test should catch it
		b, err := json.Marshal(in)
		require.Nil(t, err)

		var untyped map[string]interface{}
		err = json.Unmarshal(b, &untyped)
		require.Nil(t, err)

		return untyped["additional"].(map[string]interface{})["nearestNeighbors"].(map[string]interface{})["neighbors"].([]interface{})
	}

	validateNeighbors(t, extractNeighbor(res.Payload.Objects[0]), extractNeighbor(res.Payload.Objects[1]))
}

func featureProjection(t *testing.T) {
	listParams := objects.NewObjectsListParams().WithInclude(ptString("featureProjection"))
	res, err := helper.Client(t).Objects.ObjectsList(listParams, nil)
	require.Nil(t, err, "should not error")

	extractProjection := func(in *models.Object) []interface{} {
		// marshalling to JSON and back into an untyped map to make sure we assert
		// on the actual JSON structure. This way if we accidentally change the
		// goswagger generation so it affects both the client and the server in the
		// same way, this test should catch it
		b, err := json.Marshal(in)
		require.Nil(t, err)

		var untyped map[string]interface{}
		err = json.Unmarshal(b, &untyped)
		require.Nil(t, err)

		return untyped["additional"].(map[string]interface{})["featureProjection"].(map[string]interface{})["vector"].([]interface{})
	}

	validateProjections(t, 2, extractProjection(res.Payload.Objects[0]), extractProjection(res.Payload.Objects[1]))
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

func validateProjections(t *testing.T, dims int, vectors ...[]interface{}) {
	for _, vector := range vectors {
		if len(vector) != dims {
			t.Fatalf("expected feature projection vector to have length 3, got: %d", len(vector))
		}
	}
}
