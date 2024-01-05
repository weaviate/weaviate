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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/classifications"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

func contextualClassification(t *testing.T) {
	var id strfmt.UUID

	res, err := helper.Client(t).Classifications.ClassificationsPost(classifications.NewClassificationsPostParams().
		WithParams(&models.Classification{
			Class:              "Article",
			ClassifyProperties: []string{"ofCategory"},
			BasedOnProperties:  []string{"content"},
			Type:               "text2vec-contextionary-contextual",
		}), nil)
	require.Nil(t, err)
	id = res.Payload.ID

	// wait for classification to be completed
	testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, "completed", func() interface{} {
		res, err := helper.Client(t).Classifications.ClassificationsGet(classifications.NewClassificationsGetParams().
			WithID(id.String()), nil)

		require.Nil(t, err)
		return res.Payload.Status
	}, 100*time.Millisecond, 15*time.Second)

	// wait for latest changes to be indexed / wait for consistency
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
			WithID(article1), nil)
		require.Nil(t, err)
		return res.Payload.Properties.(map[string]interface{})["ofCategory"] != nil
	})
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
			WithID(article2), nil)
		require.Nil(t, err)
		return res.Payload.Properties.(map[string]interface{})["ofCategory"] != nil
	})
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().
			WithID(article3), nil)
		require.Nil(t, err)
		return res.Payload.Properties.(map[string]interface{})["ofCategory"] != nil
	})

	gres := AssertGraphQL(t, nil, `
{
  Get {
		Article {
			_additional {
				id
			}
			ofCategory {
				... on Category {
					name
				}
			}
		}
	}
}`)

	expectedCategoriesByID := map[strfmt.UUID]string{
		article1: "Computers and Technology",
		article2: "Food and Drink",
		article3: "Politics",
	}
	articles := gres.Get("Get", "Article").AsSlice()
	for _, article := range articles {
		actual := article.(map[string]interface{})["ofCategory"].([]interface{})[0].(map[string]interface{})["name"].(string)
		id := article.(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
		assert.Equal(t, expectedCategoriesByID[strfmt.UUID(id)], actual)
	}
}

func ptString(in string) *string {
	return &in
}
