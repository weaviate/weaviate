package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/schema"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
)

func assertCreateThing(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{
		Thing: &models.ThingCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    schema,
		},
		Async: false,
	})

	resp, _, err := helper.Client(t).Things.WeaviateThingsCreate(params, helper.RootAuth)

	var thingID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thingID = resp.Payload.ThingID
	})

	return thingID
}

func assertGetThing(t *testing.T, uuid strfmt.UUID) *models.ThingGetResponse {
	getResp, err := helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithThingID(uuid), helper.RootAuth)

	var thing *models.ThingGetResponse

	helper.AssertRequestOk(t, getResp, err, func() {
		thing = getResp.Payload
	})

	return thing
}

func assertGetSchema(t *testing.T) *schema.WeaviateSchemaDumpOKBody {
	getResp, err := helper.Client(t).Schema.WeaviateSchemaDump(schema.NewWeaviateSchemaDumpParams(), helper.RootAuth)
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
