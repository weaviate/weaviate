//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/classifications"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func Test_Text2Vec_ContextualClassification(t *testing.T) {
	const (
		article1 strfmt.UUID = "dcbe5df8-af01-46f1-b45f-bcc9a7a0773d" // apple macbook
		article2 strfmt.UUID = "6a8c7b62-fd45-488f-b884-ec87227f6eb3" // ice cream and steak
		article3 strfmt.UUID = "92f05097-6371-499c-a0fe-3e60ae16fe3d" // president of the us
	)
	articles := map[strfmt.UUID]string{
		article1: "The new Apple Macbook 16 inch provides great performance",
		article2: "I love eating ice cream with my t-bone steak",
		article3: "Barack Obama was the 44th president of the united states",
	}
	expectedCategoriesByID := map[strfmt.UUID]string{
		article1: "Computers and Technology",
		article2: "Food and Drink",
		article3: "Politics",
	}

	helper.SetupClient(os.Getenv(weaviateNode1Endpoint))

	moduleConfig := map[string]interface{}{
		"text2vec-contextionary": map[string]interface{}{
			"vectorizeClassName": true,
		},
	}
	categoryClass := &models.Class{
		Class:        "Category",
		Vectorizer:   "text2vec-contextionary",
		ModuleConfig: moduleConfig,
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	articleClass := &models.Class{
		Class:        "Article",
		Vectorizer:   "text2vec-contextionary",
		ModuleConfig: moduleConfig,
		Properties: []*models.Property{
			{
				Name:     "content",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "ofCategory",
				DataType: []string{"Category"},
			},
		},
	}
	helper.CreateClass(t, categoryClass)
	defer helper.DeleteClass(t, categoryClass.Class)
	helper.CreateClass(t, articleClass)
	defer helper.DeleteClass(t, articleClass.Class)

	t.Run("import data", func(t *testing.T) {
		for _, name := range expectedCategoriesByID {
			require.NoError(t, helper.CreateObject(t, &models.Object{
				Class:      categoryClass.Class,
				Properties: map[string]interface{}{"name": name},
			}))
		}
		for id, content := range articles {
			require.NoError(t, helper.CreateObject(t, &models.Object{
				ID:         id,
				Class:      articleClass.Class,
				Properties: map[string]interface{}{"content": content},
			}))
			helper.AssertGetObjectEventually(t, articleClass.Class, id)
		}
	})

	t.Run("start the classification and wait for completion", func(t *testing.T) {
		res, err := helper.Client(t).Classifications.ClassificationsPost(classifications.NewClassificationsPostParams().
			WithParams(&models.Classification{
				Class:              articleClass.Class,
				ClassifyProperties: []string{"ofCategory"},
				BasedOnProperties:  []string{"content"},
				Type:               "text2vec-contextionary-contextual",
			}), nil)
		require.Nil(t, err)
		id := res.Payload.ID

		helper.AssertEventuallyEqualWithFrequencyAndTimeout(t, "completed", func() interface{} {
			res, err := helper.Client(t).Classifications.ClassificationsGet(classifications.NewClassificationsGetParams().
				WithID(id.String()), nil)
			require.Nil(t, err)
			require.NotEqual(t, "failed", res.Payload.Status, "classification failed: %s", res.Payload.Error)
			return res.Payload.Status
		}, 100*time.Millisecond, 15*time.Second)
	})

	t.Run("assure changes present", func(t *testing.T) {
		for id := range articles {
			helper.AssertEventuallyEqual(t, true, func() interface{} {
				res, err := helper.Client(t).Objects.ObjectsClassGet(objects.NewObjectsClassGetParams().
					WithClassName(articleClass.Class).WithID(id), nil)
				require.Nil(t, err)
				return res.Payload.Properties.(map[string]interface{})["ofCategory"] != nil
			})
		}
	})

	t.Run("assure proper classification present", func(t *testing.T) {
		gres := graphqlhelper.AssertGraphQL(t, nil, `
			{
				Get {
					Article {
						_additional { id }
						ofCategory { ... on Category { name } }
					}
				}
			}`)

		got := gres.Get("Get", "Article").AsSlice()
		require.Len(t, got, len(articles))
		for _, article := range got {
			actual := article.(map[string]interface{})["ofCategory"].([]interface{})[0].(map[string]interface{})["name"].(string)
			id := article.(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
			assert.Equal(t, expectedCategoriesByID[strfmt.UUID(id)], actual)
		}
	})
}
