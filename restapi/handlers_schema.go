package restapi

import (
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/schema"
	middleware "github.com/go-openapi/runtime/middleware"
)

func setupSchemaHandlers(api *operations.WeaviateAPI) {
	api.SchemaWeaviateSchemaDumpHandler = schema.WeaviateSchemaDumpHandlerFunc(func(params schema.WeaviateSchemaDumpParams, principal interface{}) middleware.Responder {
		// TODO: hack; should go through schema manager
		payload := &schema.WeaviateSchemaDumpOKBody{
			Actions: databaseSchema.ActionSchema.Schema,
			Things:  databaseSchema.ThingSchema.Schema,
		}
		return schema.NewWeaviateSchemaDumpOK().WithPayload(payload)
	})
}
