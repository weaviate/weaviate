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
	schemaUC "github.com/creativesoftwarefdn/weaviate/schema"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
)

func setupSchemaHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *schemaUC.Manager) {

	addAction := func(params schema.WeaviateSchemaActionsCreateParams, principal *models.Principal) middleware.Responder {
		err := manager.AddAction(params.HTTPRequest.Context(), params.ActionClass)
		if err != nil {
			return schema.NewWeaviateSchemaActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalAddMeta)
		}()

		return schema.NewWeaviateSchemaActionsCreateOK().WithPayload(params.ActionClass)
	}

	deleteAction := func(params schema.WeaviateSchemaActionsDeleteParams, principal *models.Principal) middleware.Responder {
		err := manager.DeleteAction(params.HTTPRequest.Context(), params.ClassName)
		if err != nil {
			return schema.NewWeaviateSchemaActionsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaActionsDeleteOK()
	}

	addActionProperty := func(params schema.WeaviateSchemaActionsPropertiesAddParams,
		principal *models.Principal) middleware.Responder {
		err := manager.AddActionProperty(params.HTTPRequest.Context(), params.ClassName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaActionsPropertiesAddOK().WithPayload(params.Body)
	}

	deleteActionProperty := func(params schema.WeaviateSchemaActionsPropertiesDeleteParams,
		principal *models.Principal) middleware.Responder {
		err := manager.DeleteActionProperty(params.HTTPRequest.Context(), params.ClassName, params.PropertyName)
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaActionsPropertiesDeleteOK()
	}

	updateActionProperty := func(params schema.WeaviateSchemaActionsPropertiesUpdateParams,
		principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		err := manager.UpdateActionProperty(ctx, params.ClassName, params.PropertyName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaActionsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()
		return schema.NewWeaviateSchemaActionsPropertiesUpdateOK()
	}

	updateAction := func(params schema.WeaviateSchemaActionsUpdateParams, principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		err := manager.UpdateAction(ctx, params.ClassName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaActionsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaActionsUpdateOK()
	}

	getSchema := func(params schema.WeaviateSchemaDumpParams, principal *models.Principal) middleware.Responder {
		dbSchema, err := manager.GetSchema()
		if err != nil {
			return schema.NewWeaviateSchemaDumpInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}

		payload := &schema.WeaviateSchemaDumpOKBody{
			Actions: dbSchema.Actions,
			Things:  dbSchema.Things,
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()
		return schema.NewWeaviateSchemaDumpOK().WithPayload(payload)
	}

	addThing := func(params schema.WeaviateSchemaThingsCreateParams, principal *models.Principal) middleware.Responder {
		err := manager.AddThing(params.HTTPRequest.Context(), params.ThingClass)
		if err != nil {
			return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalAddMeta)
		}()

		return schema.NewWeaviateSchemaThingsCreateOK().WithPayload(params.ThingClass)
	}

	deleteThing := func(params schema.WeaviateSchemaThingsDeleteParams, principal *models.Principal) middleware.Responder {
		err := manager.DeleteThing(params.HTTPRequest.Context(), params.ClassName)
		if err != nil {
			return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaThingsDeleteOK()
	}

	addThingProperty := func(params schema.WeaviateSchemaThingsPropertiesAddParams,
		principal *models.Principal) middleware.Responder {
		err := manager.AddThingProperty(params.HTTPRequest.Context(), params.ClassName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaThingsPropertiesAddOK().WithPayload(params.Body)
	}

	deleteThingProperty := func(params schema.WeaviateSchemaThingsPropertiesDeleteParams, principal *models.Principal) middleware.Responder {
		err := manager.DeleteThingProperty(params.HTTPRequest.Context(), params.ClassName, params.PropertyName)
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaThingsPropertiesDeleteOK()
	}

	updateThingProperty := func(params schema.WeaviateSchemaThingsPropertiesUpdateParams,
		principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		err := manager.UpdateThingProperty(ctx, params.ClassName, params.PropertyName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaThingsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()
		return schema.NewWeaviateSchemaThingsPropertiesUpdateOK()
	}

	updateThing := func(params schema.WeaviateSchemaThingsUpdateParams, principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		err := manager.UpdateThing(ctx, params.ClassName, params.Body)
		if err != nil {
			return schema.NewWeaviateSchemaThingsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulateMeta)
		}()

		return schema.NewWeaviateSchemaThingsUpdateOK()
	}

	api.SchemaWeaviateSchemaActionsCreateHandler = schema.WeaviateSchemaActionsCreateHandlerFunc(addAction)
	api.SchemaWeaviateSchemaActionsUpdateHandler = schema.WeaviateSchemaActionsUpdateHandlerFunc(updateAction)
	api.SchemaWeaviateSchemaActionsDeleteHandler = schema.WeaviateSchemaActionsDeleteHandlerFunc(deleteAction)
	api.SchemaWeaviateSchemaActionsPropertiesAddHandler = schema.WeaviateSchemaActionsPropertiesAddHandlerFunc(addActionProperty)
	api.SchemaWeaviateSchemaActionsPropertiesDeleteHandler = schema.WeaviateSchemaActionsPropertiesDeleteHandlerFunc(deleteActionProperty)
	api.SchemaWeaviateSchemaActionsPropertiesUpdateHandler = schema.WeaviateSchemaActionsPropertiesUpdateHandlerFunc(updateActionProperty)

	api.SchemaWeaviateSchemaThingsCreateHandler = schema.WeaviateSchemaThingsCreateHandlerFunc(addThing)
	api.SchemaWeaviateSchemaThingsUpdateHandler = schema.WeaviateSchemaThingsUpdateHandlerFunc(updateThing)
	api.SchemaWeaviateSchemaThingsDeleteHandler = schema.WeaviateSchemaThingsDeleteHandlerFunc(deleteThing)
	api.SchemaWeaviateSchemaThingsPropertiesAddHandler = schema.WeaviateSchemaThingsPropertiesAddHandlerFunc(addThingProperty)
	api.SchemaWeaviateSchemaThingsPropertiesDeleteHandler = schema.WeaviateSchemaThingsPropertiesDeleteHandlerFunc(deleteThingProperty)
	api.SchemaWeaviateSchemaThingsPropertiesUpdateHandler = schema.WeaviateSchemaThingsPropertiesUpdateHandlerFunc(updateThingProperty)
	api.SchemaWeaviateSchemaDumpHandler = schema.WeaviateSchemaDumpHandlerFunc(getSchema)
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
