//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

var (
	article1 strfmt.UUID = "dcbe5df8-af01-46f1-b45f-bcc9a7a0773d" // apple macbook
	article2 strfmt.UUID = "6a8c7b62-fd45-488f-b884-ec87227f6eb3" // ice cream and steak
	article3 strfmt.UUID = "92f05097-6371-499c-a0fe-3e60ae16fe3d" // president of the us
)

func Test_Classifications(t *testing.T) {
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

	// tests
	t.Run("contextual classification", contextualClassification)

	// tear down
	deleteThingClass(t, "Article")
	deleteThingClass(t, "Category")
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
