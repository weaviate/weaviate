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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

var (
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
	t.Run("recipe setup for knn classification", setupRecipe)
	t.Run("food types and recipes setup for zeroshot classification", setupFoodTypes)

	// tests
	t.Run("knn classification", knnClassification)
	t.Run("zeroshot classification", zeroshotClassification)

	// tear down
	deleteObjectClass(t, "Recipe")
	deleteObjectClass(t, "RecipeType")
	deleteObjectClass(t, "FoodType")
	deleteObjectClass(t, "Recipes")
}

func setupRecipe(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class:        "RecipeType",
			ModuleConfig: vectorizeClassName(),
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class:        "Recipe",
			ModuleConfig: vectorizeClassName(),
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
			Class:        "FoodType",
			ModuleConfig: vectorizeClassName(),
			Properties: []*models.Property{
				{
					Name:         "text",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class:        "Recipes",
			ModuleConfig: vectorizeClassName(),
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

func vectorizeClassName() map[string]interface{} {
	return map[string]interface{}{
		"text2vec-model2vec": map[string]interface{}{"vectorizeClassName": true},
	}
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

	helper.AssertEventuallyEqual(t, true, checkThunk)

	var object *models.Object

	helper.AssertRequestOk(t, resp, err, func() {
		object = resp.Payload
	})

	return object
}

func ptString(in string) *string {
	return &in
}
