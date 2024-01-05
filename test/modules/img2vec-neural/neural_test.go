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
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func Test_Img2VecNeural(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))

	fashionItemClass := &models.Class{
		Class:      "FashionItem",
		Vectorizer: "img2vec-neural",
		ModuleConfig: map[string]interface{}{
			"img2vec-neural": map[string]interface{}{
				"imageFields": []string{"image"},
			},
		},
		Properties: []*models.Property{
			{
				Name:     "image",
				DataType: schema.DataTypeBlob.PropString(),
			},
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	helper.CreateClass(t, fashionItemClass)
	defer helper.DeleteClass(t, fashionItemClass.Class)

	getBase64EncodedTestImage := func() string {
		image, err := os.Open("./data/pixel.png")
		require.Nil(t, err)
		require.NotNil(t, image)
		content, err := io.ReadAll(image)
		require.Nil(t, err)
		return base64.StdEncoding.EncodeToString(content)
	}

	t.Run("import data", func(t *testing.T) {
		base64Image := getBase64EncodedTestImage()
		obj := &models.Object{
			Class: fashionItemClass.Class,
			Properties: map[string]interface{}{
				"image":       base64Image,
				"description": "A single black pixel",
			},
		}
		helper.CreateObject(t, obj)
	})

	t.Run("perform nearImage query", func(t *testing.T) {
		queryTemplate := `
		{
			Get {
				FashionItem(
					nearImage: {
						image: "%s"
					}
				){
					image
					description
					_additional{vector}
				}
			}
		}`
		query := fmt.Sprintf(queryTemplate, getBase64EncodedTestImage())
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		fashionItems := result.Get("Get", "FashionItem").AsSlice()
		require.True(t, len(fashionItems) > 0)
		item, ok := fashionItems[0].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, item["image"])
		assert.NotNil(t, item["description"])
		vector := item["_additional"].(map[string]interface{})["vector"]
		assert.NotNil(t, vector)
	})
}
