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
	api.SchemaWeaviateSchemaActionsCreateHandler = schema.WeaviateSchemaActionsCreateHandlerFunc(func(params schema.WeaviateSchemaActionsCreateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := schemaManager.AddClass(kind.ACTION_KIND, params.ActionClass)

		if err == nil {
			return schema.NewWeaviateSchemaActionsCreateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsCreateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsDeleteHandler = schema.WeaviateSchemaActionsDeleteHandlerFunc(func(params schema.WeaviateSchemaActionsDeleteParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := schemaManager.DropClass(kind.ACTION_KIND, params.ClassName)

		if err == nil {
			return schema.NewWeaviateSchemaActionsDeleteOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsDeleteBadRequest().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsPropertiesAddHandler = schema.WeaviateSchemaActionsPropertiesAddHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesAddParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := schemaManager.AddProperty(kind.ACTION_KIND, params.ClassName, params.Body)

		if err == nil {
			return schema.NewWeaviateSchemaActionsPropertiesAddOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsPropertiesAddUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsPropertiesDeleteHandler = schema.WeaviateSchemaActionsPropertiesDeleteHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesDeleteParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		_ = schemaManager.DropProperty(kind.ACTION_KIND, params.ClassName, params.PropertyName)

		return schema.NewWeaviateSchemaActionsPropertiesDeleteOK()
	})

	api.SchemaWeaviateSchemaActionsPropertiesUpdateHandler = schema.WeaviateSchemaActionsPropertiesUpdateHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesUpdateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		err := schemaManager.UpdateProperty(kind.ACTION_KIND, params.ClassName, params.PropertyName, newName, newKeywords)

		if err == nil {
			return schema.NewWeaviateSchemaActionsPropertiesUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsPropertiesUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsUpdateHandler = schema.WeaviateSchemaActionsUpdateHandlerFunc(func(params schema.WeaviateSchemaActionsUpdateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		err := schemaManager.UpdateClass(kind.ACTION_KIND, params.ClassName, newName, newKeywords)

		if err == nil {
			return schema.NewWeaviateSchemaActionsUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})
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
		err := schemaManager.AddClass(kind.THING_KIND, params.ThingClass)

		if err == nil {
			return schema.NewWeaviateSchemaThingsCreateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsDeleteHandler = schema.WeaviateSchemaThingsDeleteHandlerFunc(func(params schema.WeaviateSchemaThingsDeleteParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := schemaManager.DropClass(kind.THING_KIND, params.ClassName)

		if err == nil {
			return schema.NewWeaviateSchemaThingsDeleteOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsPropertiesAddHandler = schema.WeaviateSchemaThingsPropertiesAddHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesAddParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		err := schemaManager.AddProperty(kind.THING_KIND, params.ClassName, params.Body)

		if err == nil {
			return schema.NewWeaviateSchemaThingsPropertiesAddOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsPropertiesAddUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsPropertiesDeleteHandler = schema.WeaviateSchemaThingsPropertiesDeleteHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesDeleteParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()
		_ = schemaManager.DropProperty(kind.THING_KIND, params.ClassName, params.PropertyName)

		return schema.NewWeaviateSchemaThingsPropertiesDeleteOK()
	})

	api.SchemaWeaviateSchemaThingsPropertiesUpdateHandler = schema.WeaviateSchemaThingsPropertiesUpdateHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesUpdateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		err := schemaManager.UpdateProperty(kind.THING_KIND, params.ClassName, params.PropertyName, newName, newKeywords)

		if err == nil {
			return schema.NewWeaviateSchemaThingsPropertiesUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsPropertiesUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsUpdateHandler = schema.WeaviateSchemaThingsUpdateHandlerFunc(func(params schema.WeaviateSchemaThingsUpdateParams, principal interface{}) middleware.Responder {
		//TODO: auth

		schemaLock := db.SchemaLock()
		defer schemaLock.Unlock()

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		err := schemaManager.UpdateClass(kind.THING_KIND, params.ClassName, newName, newKeywords)

		if err == nil {
			return schema.NewWeaviateSchemaThingsUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})
}
