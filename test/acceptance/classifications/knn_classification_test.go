//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/classifications"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func knnClassification(t *testing.T) {
	var id strfmt.UUID

	t.Run("start the classification and wait for completion", func(t *testing.T) {
		res, err := helper.Client(t).Classifications.ClassificationsPost(
			classifications.NewClassificationsPostParams().WithParams(&models.Classification{
				Class:              "Recipe",
				ClassifyProperties: []string{"ofType"},
				BasedOnProperties:  []string{"content"},
				Type:               "knn",
				Settings: map[string]interface{}{
					"k": 5,
				},
			}), nil)
		require.Nil(t, err)
		id = res.Payload.ID

		// wait for classification to be completed
		testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, "completed",
			func() interface{} {
				res, err := helper.Client(t).Classifications.ClassificationsGet(
					classifications.NewClassificationsGetParams().WithID(id.String()), nil)

				require.Nil(t, err)
				return res.Payload.Status
			}, 100*time.Millisecond, 15*time.Second)
	})

	t.Run("assure changes present", func(t *testing.T) {
		// wait for latest changes to be indexed / wait for consistency
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedSavory), nil)
			require.Nil(t, err)
			return res.Payload.Properties.(map[string]interface{})["ofType"] != nil
		})
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedSweet), nil)
			require.Nil(t, err)
			return res.Payload.Properties.(map[string]interface{})["ofType"] != nil
		})
	})

	t.Run("inspect unclassified savory", func(t *testing.T) {
		res, err := helper.Client(t).Objects.
			ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedSavory).
				WithInclude(ptString("classification")), nil)

		require.Nil(t, err)
		schema, ok := res.Payload.Properties.(map[string]interface{})
		require.True(t, ok)

		expectedRefTarget := fmt.Sprintf("weaviate://localhost/%s",
			recipeTypeSavory)
		ref := schema["ofType"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, ref["beacon"].(string), expectedRefTarget)

		verifyMetaDistances(t, ref)
	})

	t.Run("inspect unclassified sweet", func(t *testing.T) {
		res, err := helper.Client(t).Objects.
			ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedSweet).
				WithInclude(ptString("classification")), nil)

		require.Nil(t, err)
		schema, ok := res.Payload.Properties.(map[string]interface{})
		require.True(t, ok)

		expectedRefTarget := fmt.Sprintf("weaviate://localhost/%s",
			recipeTypeSweet)
		ref := schema["ofType"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, ref["beacon"].(string), expectedRefTarget)

		verifyMetaDistances(t, ref)
	})
}

func verifyMetaDistances(t *testing.T, ref map[string]interface{}) {
	classification, ok := ref["classification"].(map[string]interface{})
	require.True(t, ok)

	assert.Equal(t, json.Number("3"), classification["winningCount"])
	assert.Equal(t, json.Number("2"), classification["losingCount"])
	assert.Equal(t, json.Number("5"), classification["overallCount"])

	closestWinning, err := classification["closestWinningDistance"].(json.Number).Float64()
	require.Nil(t, err)
	closestLosing, err := classification["closestLosingDistance"].(json.Number).Float64()
	require.Nil(t, err)
	closestOverall, err := classification["closestOverallDistance"].(json.Number).Float64()
	require.Nil(t, err)
	meanWinning, err := classification["meanWinningDistance"].(json.Number).Float64()
	require.Nil(t, err)
	meanLosing, err := classification["meanLosingDistance"].(json.Number).Float64()
	require.Nil(t, err)

	assert.True(t, closestWinning == closestOverall, "closestWinning == closestOverall")
	assert.True(t, closestWinning < meanWinning, "closestWinning < meanWinning")
	assert.True(t, closestWinning < closestLosing, "closestWinning < closestLosing")
	assert.True(t, closestLosing < meanLosing, "closestLosing < meanLosing")
}
