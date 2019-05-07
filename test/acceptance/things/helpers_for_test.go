/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
)

func assertCreateThing(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := things.NewWeaviateThingsCreateParams().WithBody(
		&models.Thing{
			Class:  className,
			Schema: schema,
		})

	resp, err := helper.Client(t).Things.WeaviateThingsCreate(params, nil)

	var thingID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thingID = resp.Payload.ID
	})

	return thingID
}

func assertGetThing(t *testing.T, uuid strfmt.UUID) *models.Thing {
	getResp, err := helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithID(uuid), nil)

	var thing *models.Thing

	helper.AssertRequestOk(t, getResp, err, func() {
		thing = getResp.Payload
	})

	return thing
}

func assertGetThingEventually(t *testing.T, uuid strfmt.UUID) *models.Thing {
	var (
		resp *things.WeaviateThingsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithID(uuid), nil)
		return err == nil
	}

	helper.AssertEventuallyEqual(t, true, checkThunk)

	var thing *models.Thing

	helper.AssertRequestOk(t, resp, err, func() {
		thing = resp.Payload
	})

	return thing
}

func assertGetSchema(t *testing.T) *schema.WeaviateSchemaDumpOKBody {
	getResp, err := helper.Client(t).Schema.WeaviateSchemaDump(schema.NewWeaviateSchemaDumpParams(), nil)
	var schema *schema.WeaviateSchemaDumpOKBody
	helper.AssertRequestOk(t, getResp, err, func() {
		schema = getResp.Payload
	})

	return schema
}

func assertClassInSchema(t *testing.T, schema *models.SemanticSchema, className string) *models.SemanticSchemaClass {
	for _, class := range schema.Classes {
		if class.Class == className {
			return class
		}
	}

	t.Fatalf("class %s not found in schema", className)
	return nil
}

func assertPropertyInClass(t *testing.T, class *models.SemanticSchemaClass, propertyName string) *models.SemanticSchemaClassProperty {
	for _, prop := range class.Properties {
		if prop.Name == propertyName {
			return prop
		}
	}

	t.Fatalf("property %s not found in class", propertyName)
	return nil
}
