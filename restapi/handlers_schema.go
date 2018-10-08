package restapi

import (
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/schema"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
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

	api.SchemaWeaviateSchemaThingsCreateHandler = schema.WeaviateSchemaThingsCreateHandlerFunc(func(params schema.WeaviateSchemaThingsCreateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := (*schemaManager).AddClass(kind.THING_KIND, params.ThingClass)

		if err == nil {
			return schema.NewWeaviateSchemaThingsCreateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: &models.ErrorResponseError{Message: err.Error()}}
			return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})
}
