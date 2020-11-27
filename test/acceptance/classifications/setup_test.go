//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
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
)

func Test_Classifications(t *testing.T) {
	t.Run("article/category setup for contextual classification", setupArticleCategory)
	t.Run("recipe setup for knn classification", setupRecipe)

	// tests
	t.Run("contextual classification", contextualClassification)
	t.Run("knn classification", knnClassification)

	// tear down
	deleteThingClass(t, "Article")
	deleteThingClass(t, "Category")
	deleteThingClass(t, "Recipe")
	deleteThingClass(t, "RecipeType")
}

func setupArticleCategory(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createThingClass(t, &models.Class{
			Class:              "Category",
			VectorizeClassName: ptBool(true),
			Properties: []*models.Property{
				&models.Property{
					Name:     "name",
					DataType: []string{"string"},
				},
			},
		})
		createThingClass(t, &models.Class{
			Class:              "Article",
			VectorizeClassName: ptBool(true),
			Properties: []*models.Property{
				&models.Property{
					Name:     "content",
					DataType: []string{"text"},
				},
				&models.Property{
					Name:     "OfCategory",
					DataType: []string{"Category"},
				},
			},
		})
	})

	t.Run("object setup - categories", func(t *testing.T) {
		createThing(t, &models.Thing{
			Class: "Category",
			Schema: map[string]interface{}{
				"name": "Food and Drink",
			},
		})
		createThing(t, &models.Thing{
			Class: "Category",
			Schema: map[string]interface{}{
				"name": "Computers and Technology",
			},
		})
		createThing(t, &models.Thing{
			Class: "Category",
			Schema: map[string]interface{}{
				"name": "Politics",
			},
		})
	})

	t.Run("object setup - articles", func(t *testing.T) {
		createThing(t, &models.Thing{
			ID:    article1,
			Class: "Article",
			Schema: map[string]interface{}{
				"content": "The new Apple Macbook 16 inch provides great performance",
			},
		})
		createThing(t, &models.Thing{
			ID:    article2,
			Class: "Article",
			Schema: map[string]interface{}{
				"content": "I love eating ice cream with my t-bone steak",
			},
		})
		createThing(t, &models.Thing{
			ID:    article3,
			Class: "Article",
			Schema: map[string]interface{}{
				"content": "Barack Obama was the 44th president of the united states",
			},
		})
	})

	assertGetThingEventually(t, "92f05097-6371-499c-a0fe-3e60ae16fe3d")
}

func setupRecipe(t *testing.T) {
	t.Run("schema setup", func(t *testing.T) {
		createThingClass(t, &models.Class{
			Class:              "RecipeType",
			VectorizeClassName: ptBool(true),
			Properties: []*models.Property{
				&models.Property{
					Name:     "name",
					DataType: []string{"string"},
				},
			},
		})
		createThingClass(t, &models.Class{
			Class:              "Recipe",
			VectorizeClassName: ptBool(true),
			Properties: []*models.Property{
				&models.Property{
					Name:     "content",
					DataType: []string{"text"},
				},
				&models.Property{
					Name:     "OfType",
					DataType: []string{"RecipeType"},
				},
			},
		})
	})

	t.Run("object setup - recipe types", func(t *testing.T) {
		createThing(t, &models.Thing{
			Class: "RecipeType",
			ID:    recipeTypeSavory,
			Schema: map[string]interface{}{
				"name": "Savory",
			},
		})

		createThing(t, &models.Thing{
			Class: "RecipeType",
			ID:    recipeTypeSweet,
			Schema: map[string]interface{}{
				"name": "Sweet",
			},
		})
	})

	t.Run("object setup - articles", func(t *testing.T) {
		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Mix two eggs with milk and 7 grams of sugar, bake in the oven at 200 degrees",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSweet),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Sautee the apples with sugar and add a dash of milk.",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSweet),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Mix butter, cream and sugar. Make eggwhites fluffy and mix with the batter",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSweet),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Fry the steak in the pan, then sautee the onions in the same pan",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSavory),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Cut the potatoes in half and add salt and pepper. Serve with the meat.",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSavory),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Put the pasta and sauce mix in the oven, top with plenty of cheese",
				"ofType": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", recipeTypeSavory),
					},
				},
			},
		})

		createThing(t, &models.Thing{
			ID:    unclassifiedSavory,
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Serve the steak with fries and ketchup.",
			},
		})

		createThing(t, &models.Thing{
			ID:    unclassifiedSweet,
			Class: "Recipe",
			Schema: map[string]interface{}{
				"content": "Whisk the cream, add sugar and serve with strawberries",
			},
		})
	})

	assertGetThingEventually(t, unclassifiedSweet)
}

func createThingClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaThingsCreateParams().WithThingClass(class)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createThing(t *testing.T, thing *models.Thing) {
	params := things.NewThingsCreateParams().WithBody(thing)
	resp, err := helper.Client(t).Things.ThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteThingClass(t *testing.T, class string) {
	delParams := schema.NewSchemaThingsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func ptBool(in bool) *bool {
	return &in
}

func assertGetThingEventually(t *testing.T, uuid strfmt.UUID) *models.Thing {
	var (
		resp *things.ThingsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var thing *models.Thing

	helper.AssertRequestOk(t, resp, err, func() {
		thing = resp.Payload
	})

	return thing
}
