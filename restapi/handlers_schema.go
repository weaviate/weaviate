/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package restapi

import (
	"log"

	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/schema"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
)

func setupSchemaHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	api.SchemaWeaviateSchemaActionsCreateHandler = schema.WeaviateSchemaActionsCreateHandlerFunc(func(params schema.WeaviateSchemaActionsCreateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.AddClass(ctx, kind.ACTION_KIND, params.ActionClass)

		if err == nil {

			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalAddMeta))
			}()

			return schema.NewWeaviateSchemaActionsCreateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsCreateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsDeleteHandler = schema.WeaviateSchemaActionsDeleteHandlerFunc(func(params schema.WeaviateSchemaActionsDeleteParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.DropClass(ctx, kind.ACTION_KIND, params.ClassName)

		if err == nil {

			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaActionsDeleteOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsDeleteBadRequest().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsPropertiesAddHandler = schema.WeaviateSchemaActionsPropertiesAddHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesAddParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesAddInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.AddProperty(ctx, kind.ACTION_KIND, params.ClassName, params.Body)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()

			return schema.NewWeaviateSchemaActionsPropertiesAddOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsPropertiesAddUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsPropertiesDeleteHandler = schema.WeaviateSchemaActionsPropertiesDeleteHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesDeleteParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		_ = schemaManager.DropProperty(ctx, kind.ACTION_KIND, params.ClassName, params.PropertyName)

		// Register the function call
		go func() {
			requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
		}()

		return schema.NewWeaviateSchemaActionsPropertiesDeleteOK()
	})

	api.SchemaWeaviateSchemaActionsPropertiesUpdateHandler = schema.WeaviateSchemaActionsPropertiesUpdateHandlerFunc(func(params schema.WeaviateSchemaActionsPropertiesUpdateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		ctx := params.HTTPRequest.Context()
		err = schemaManager.UpdateProperty(ctx, kind.ACTION_KIND, params.ClassName, params.PropertyName, newName, newKeywords)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()

			return schema.NewWeaviateSchemaActionsPropertiesUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsPropertiesUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaActionsUpdateHandler = schema.WeaviateSchemaActionsUpdateHandlerFunc(func(params schema.WeaviateSchemaActionsUpdateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaActionsUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		ctx := params.HTTPRequest.Context()
		err = schemaManager.UpdateClass(ctx, kind.ACTION_KIND, params.ClassName, newName, newKeywords)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaActionsUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaActionsUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})
	api.SchemaWeaviateSchemaDumpHandler = schema.WeaviateSchemaDumpHandlerFunc(func(params schema.WeaviateSchemaDumpParams) middleware.Responder {
		connectorLock, err := db.ConnectorLock()
		if err != nil {
			return schema.NewWeaviateSchemaDumpInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}

		defer connectorLock.Unlock()

		dbSchema := connectorLock.GetSchema()

		payload := &schema.WeaviateSchemaDumpOKBody{
			Actions: dbSchema.Actions,
			Things:  dbSchema.Things,
		}
		// TODO: validate this serviceID
		// Register the function call
		go func() {
			requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
		}()
		return schema.NewWeaviateSchemaDumpOK().WithPayload(payload)
	})

	api.SchemaWeaviateSchemaThingsCreateHandler = schema.WeaviateSchemaThingsCreateHandlerFunc(func(params schema.WeaviateSchemaThingsCreateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.AddClass(ctx, kind.THING_KIND, params.ThingClass)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalAddMeta))
			}()
			return schema.NewWeaviateSchemaThingsCreateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsDeleteHandler = schema.WeaviateSchemaThingsDeleteHandlerFunc(func(params schema.WeaviateSchemaThingsDeleteParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.DropClass(ctx, kind.THING_KIND, params.ClassName)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaThingsDeleteOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsPropertiesAddHandler = schema.WeaviateSchemaThingsPropertiesAddHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesAddParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesAddInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		err = schemaManager.AddProperty(ctx, kind.THING_KIND, params.ClassName, params.Body)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaThingsPropertiesAddOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsPropertiesAddUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsPropertiesDeleteHandler = schema.WeaviateSchemaThingsPropertiesDeleteHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesDeleteParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()
		ctx := params.HTTPRequest.Context()
		_ = schemaManager.DropProperty(ctx, kind.THING_KIND, params.ClassName, params.PropertyName)

		// Register the function call
		go func() {
			requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
		}()
		return schema.NewWeaviateSchemaThingsPropertiesDeleteOK()
	})

	api.SchemaWeaviateSchemaThingsPropertiesUpdateHandler = schema.WeaviateSchemaThingsPropertiesUpdateHandlerFunc(func(params schema.WeaviateSchemaThingsPropertiesUpdateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		ctx := params.HTTPRequest.Context()
		err = schemaManager.UpdateProperty(ctx, kind.THING_KIND, params.ClassName, params.PropertyName, newName, newKeywords)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaThingsPropertiesUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsPropertiesUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})

	api.SchemaWeaviateSchemaThingsUpdateHandler = schema.WeaviateSchemaThingsUpdateHandlerFunc(func(params schema.WeaviateSchemaThingsUpdateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return schema.NewWeaviateSchemaThingsUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(schemaLock)

		schemaManager := schemaLock.SchemaManager()

		var newName *string
		var newKeywords *models.SemanticSchemaKeywords

		if params.Body.NewName != "" {
			newName = &params.Body.NewName
		}

		// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
		if len(params.Body.Keywords) > 0 {
			newKeywords = &params.Body.Keywords
		}
		ctx := params.HTTPRequest.Context()
		err = schemaManager.UpdateClass(ctx, kind.THING_KIND, params.ClassName, newName, newKeywords)

		if err == nil {
			// Register the function call
			go func() {
				requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulateMeta))
			}()
			return schema.NewWeaviateSchemaThingsUpdateOK()
		} else {
			errorResponse := models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{&models.ErrorResponseErrorItems0{Message: err.Error()}}}
			return schema.NewWeaviateSchemaThingsUpdateUnprocessableEntity().WithPayload(&errorResponse)
		}
	})
}

type unlocker interface {
	Unlock() error
}

func unlock(l unlocker) {
	err := l.Unlock()
	if err != nil {
		log.Fatal(err)
	}
}
