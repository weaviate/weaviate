/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
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
		//TODO: auth

		connectorLock := db.ConnectorLock()
		defer connectorLock.Unlock()

		dbSchema := connectorLock.GetSchema()

		payload := &schema.WeaviateSchemaDumpOKBody{
			Actions: dbSchema.Actions,
			Things:  dbSchema.Things,
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

	api.SchemaWeaviateSchemaThingsDeleteHandler = schema.WeaviateSchemaThingsDeleteHandlerFunc(func(params schema.WeaviateSchemaThingsDeleteParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := (*schemaManager).DropClass(kind.THING_KIND, params.ClassName)

		if err == nil {
			return schema.NewWeaviateSchemaThingsDeleteOK()
		} else {
			errorResponse := models.ErrorResponse{Error: &models.ErrorResponseError{Message: err.Error()}}
			return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(&errorResponse)
		}
	})
}
