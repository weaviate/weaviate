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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

var (
	// contextual
	article1 strfmt.UUID = "dcbe5df8-af01-46f1-b45f-bcc9a7a0773d" // apple macbook
	article2 strfmt.UUID = "6a8c7b62-fd45-488f-b884-ec87227f6eb3" // ice cream and steak
	article3 strfmt.UUID = "92f05097-6371-499c-a0fe-3e60ae16fe3d" // president of the us

	// knn
	recipeTypeSavory   strfmt.UUID = "989d792c-b59e-4430-80a3-cf7f320f31b0"
	recipeTypeSweet    strfmt.UUID = "c9dfda02-6b05-4117-9d95-a188342cca48"
	unclassifiedSavory strfmt.UUID = "953c03f8-d61e-44c0-bbf1-2afe0dc1ce87"
	unclassifiedSweet  strfmt.UUID = "04603002-cb66-4fce-bf6d-56bdf9b0b5d4"

	// zeroshot
	foodTypeMeat          strfmt.UUID = "998d792c-b59e-4430-80a3-cf7f320f31b0"
	foodTypeIceCream      strfmt.UUID = "998d792c-b59e-4430-80a3-cf7f320f31b1"
	unclassifiedSteak     strfmt.UUID = "953c03f8-d61e-44c0-bbf1-2afe0dc1ce10"
	unclassifiedIceCreams strfmt.UUID = "953c03f8-d61e-44c0-bbf1-2afe0dc1ce11"
)

func Test_Classifications(t *testing.T) {
	t.Run("article/category setup for contextual classification", setupArticleCategory)
	t.Run("recipe setup for knn classification", setupRecipe)
	t.Run("food types and recipes setup for zeroshot classification", setupFoodTypes)

	// tests
	t.Run("contextual classification", contextualClassification)
	t.Run("knn classification", knnClassification)
	t.Run("zeroshot classification", zeroshotClassification)

	// tear down
	deleteObjectClass(t, "Article")
	deleteObjectClass(t, "Category")
	deleteObjectClass(t, "Recipe")
	deleteObjectClass(t, "RecipeType")
	deleteObjectClass(t, "FoodType")
	deleteObjectClass(t, "Recipes")
}

func setupArticleCategory(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class: "Category",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class: "Article",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "content",
					DataType: []string{"text"},
				},
				{
					Name:     "OfCategory",
					DataType: []string{"Category"},
				},
			},
		})
	})

	t.Run("object setup - categories", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: "Category",
			Properties: map[string]interface{}{
				"name": "Food and Drink",
			},
		})
		createObject(t, &models.Object{
			Class: "Category",
			Properties: map[string]interface{}{
				"name": "Computers and Technology",
			},
		})
		createObject(t, &models.Object{
			Class: "Category",
			Properties: map[string]interface{}{
				"name": "Politics",
			},
		})
	})

	t.Run("object setup - articles", func(t *testing.T) {
		createObject(t, &models.Object{
			ID:    article1,
			Class: "Article",
			Properties: map[string]interface{}{
				"content": "The new Apple Macbook 16 inch provides great performance",
			},
		})
		createObject(t, &models.Object{
			ID:    article2,
			Class: "Article",
			Properties: map[string]interface{}{
				"content": "I love eating ice cream with my t-bone steak",
			},
		})
		createObject(t, &models.Object{
			ID:    article3,
			Class: "Article",
			Properties: map[string]interface{}{
				"content": "Barack Obama was the 44th president of the united states",
			},
		})
	})

	assertGetObjectEventually(t, "92f05097-6371-499c-a0fe-3e60ae16fe3d")
}

func setupRecipe(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class: "RecipeType",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class: "Recipe",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "content",
					DataType: []string{"text"},
				},
				{
					Name:     "OfType",
					DataType: []string{"RecipeType"},
				},
			},
		})
	})

	t.Run("object setup - recipe types", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: "RecipeType",
			ID:    recipeTypeSavory,
			Properties: map[string]interface{}{
				"name": "Savory",
			},
		})

		createObject(t, &models.Object{
			Class: "RecipeType",
			ID:    recipeTypeSweet,
			Properties: map[string]interface{}{
				"name": "Sweet",
			},
		})
	})

	t.Run("object setup - articles", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Mix two eggs with milk and 7 grams of sugar, bake in the oven at 200 degrees",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSweet),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Sautee the apples with sugar and add a dash of milk.",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSweet),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Mix butter, cream and sugar. Make eggwhites fluffy and mix with the batter",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSweet),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Fry the steak in the pan, then sautee the onions in the same pan",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSavory),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Cut the potatoes in half and add salt and pepper. Serve with the meat.",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSavory),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Put the pasta and sauce mix in the oven, top with plenty of cheese",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", recipeTypeSavory),
					},
				},
			},
		})

		createObject(t, &models.Object{
			ID:    unclassifiedSavory,
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Serve the steak with fries and ketchup.",
			},
		})

		createObject(t, &models.Object{
			ID:    unclassifiedSweet,
			Class: "Recipe",
			Properties: map[string]interface{}{
				"content": "Whisk the cream, add sugar and serve with strawberries",
			},
		})
	})

	assertGetObjectEventually(t, unclassifiedSweet)
}

func setupFoodTypes(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class: "FoodType",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "text",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class: "Recipes",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "text",
					DataType: []string{"text"},
				},
				{
					Name:     "ofFoodType",
					DataType: []string{"FoodType"},
				},
			},
		})
	})

	t.Run("object setup - food types", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: "FoodType",
			ID:    foodTypeIceCream,
			Properties: map[string]interface{}{
				"text": "Ice cream",
			},
		})

		createObject(t, &models.Object{
			Class: "FoodType",
			ID:    foodTypeMeat,
			Properties: map[string]interface{}{
				"text": "Meat",
			},
		})
	})

	t.Run("object setup - recipes", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: "Recipes",
			ID:    unclassifiedSteak,
			Properties: map[string]interface{}{
				"text": "Cut the steak in half and put it into pan",
			},
		})

		createObject(t, &models.Object{
			Class: "Recipes",
			ID:    unclassifiedIceCreams,
			Properties: map[string]interface{}{
				"text": "There are flavors of vanilla, chocolate and strawberry",
			},
		})
	})
}

func createObjectClass(t *testing.T, class *models.Class) {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func assertGetObjectEventually(t *testing.T, uuid strfmt.UUID) *models.Object {
	var (
		resp *objects.ObjectsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var object *models.Object

	helper.AssertRequestOk(t, resp, err, func() {
		object = resp.Payload
	})

	return object
}
