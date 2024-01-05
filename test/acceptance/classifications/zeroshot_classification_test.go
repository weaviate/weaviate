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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/classifications"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

func zeroshotClassification(t *testing.T) {
	var id strfmt.UUID

	t.Run("start the classification and wait for completion", func(t *testing.T) {
		res, err := helper.Client(t).Classifications.ClassificationsPost(
			classifications.NewClassificationsPostParams().WithParams(&models.Classification{
				Class:              "Recipes",
				ClassifyProperties: []string{"ofFoodType"},
				BasedOnProperties:  []string{"text"},
				Type:               "zeroshot",
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
				WithID(unclassifiedSteak), nil)
			require.Nil(t, err)
			return res.Payload.Properties.(map[string]interface{})["ofFoodType"] != nil
		})
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedIceCreams), nil)
			require.Nil(t, err)
			return res.Payload.Properties.(map[string]interface{})["ofFoodType"] != nil
		})
	})

	t.Run("assure proper classification present", func(t *testing.T) {
		// wait for latest changes to be indexed / wait for consistency
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedSteak), nil)
			require.Nil(t, err)
			return checkOfFoodTypeRef(res.Payload.Properties, foodTypeMeat)
		})
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
				WithID(unclassifiedIceCreams), nil)
			require.Nil(t, err)
			return checkOfFoodTypeRef(res.Payload.Properties, foodTypeIceCream)
		})
	})
}

func checkOfFoodTypeRef(properties interface{}, id strfmt.UUID) bool {
	ofFoodType, ok := properties.(map[string]interface{})["ofFoodType"].([]interface{})
	if !ok || len(ofFoodType) == 0 {
		return false
	}
	ofFoodTypeMap, ok := ofFoodType[0].(map[string]interface{})
	if !ok {
		return false
	}
	beacon, ok := ofFoodTypeMap["beacon"]
	if !ok {
		return false
	}
	return beacon == fmt.Sprintf("weaviate://localhost/FoodType/%s", id)
}
