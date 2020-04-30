//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/classifications"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func contextualClassification(t *testing.T) {
	var id strfmt.UUID

	res, err := helper.Client(t).Classifications.ClassificationsPost(classifications.NewClassificationsPostParams().
		WithParams(&models.Classification{
			Class:              "Article",
			ClassifyProperties: []string{"ofCategory"},
			BasedOnProperties:  []string{"content"},
			Type:               ptString("contextual"),
		}), nil)
	require.Nil(t, err)
	id = res.Payload.ID

	// wait for classification to be completed
	testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, "completed", func() interface{} {
		res, err := helper.Client(t).Classifications.ClassificationsGet(classifications.NewClassificationsGetParams().
			WithID(id.String()), nil)

		require.Nil(t, err)
		return res.Payload.Status
	}, 300*time.Millisecond, 15*time.Second)

	// wait for latest changes to be indexed / wait for consistency
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().
			WithID(article1), nil)
		require.Nil(t, err)
		return res.Payload.Schema.(map[string]interface{})["ofCategory"] != nil
	})
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().
			WithID(article2), nil)
		require.Nil(t, err)
		return res.Payload.Schema.(map[string]interface{})["ofCategory"] != nil
	})
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		res, err := helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().
			WithID(article3), nil)
		require.Nil(t, err)
		return res.Payload.Schema.(map[string]interface{})["ofCategory"] != nil
	})

	gres := AssertGraphQL(t, nil, `
{
  Get {
	  Things {
		  Article {
			  uuid
				OfCategory {
				  ... on Category {
					  name
				  }
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
	articles := gres.Get("Get", "Things", "Article").AsSlice()
	for _, article := range articles {
		actual := article.(map[string]interface{})["OfCategory"].([]interface{})[0].(map[string]interface{})["name"].(string)
		id := article.(map[string]interface{})["uuid"].(string)
		assert.Equal(t, expectedCategoriesByID[strfmt.UUID(id)], actual)
	}
}

func ptString(in string) *string {
	return &in
}
