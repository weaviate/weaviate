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

package rest

import (
	"log"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
)

type schemaHandlers struct {
	telemetry *telemetry.RequestsLog
	manager   *schemaUC.Manager
}

func (s *schemaHandlers) telemetryLogAsync(requestType, identifier string) {
	go func() {
		s.telemetry.Register(requestType, identifier)
	}()
}

func (s *schemaHandlers) addAction(params schema.WeaviateSchemaActionsCreateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddAction(params.HTTPRequest.Context(), params.ActionClass)
	if err != nil {
		return schema.NewWeaviateSchemaActionsCreateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewWeaviateSchemaActionsCreateOK().WithPayload(params.ActionClass)
}

func (s *schemaHandlers) deleteAction(params schema.WeaviateSchemaActionsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteAction(params.HTTPRequest.Context(), params.ClassName)
	if err != nil {
		return schema.NewWeaviateSchemaActionsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsDeleteOK()
}

func (s *schemaHandlers) addActionProperty(params schema.WeaviateSchemaActionsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddActionProperty(params.HTTPRequest.Context(), params.ClassName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaActionsPropertiesAddUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteActionProperty(params schema.WeaviateSchemaActionsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteActionProperty(params.HTTPRequest.Context(), params.ClassName, params.PropertyName)
	if err != nil {
		return schema.NewWeaviateSchemaActionsPropertiesDeleteInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateActionProperty(params schema.WeaviateSchemaActionsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateActionProperty(ctx, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaActionsPropertiesUpdateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateAction(params schema.WeaviateSchemaActionsUpdateParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateAction(ctx, params.ClassName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaActionsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsUpdateOK()
}

func (s *schemaHandlers) getSchema(params schema.WeaviateSchemaDumpParams, principal *models.Principal) middleware.Responder {
	dbSchema := s.manager.GetSchema()
	payload := &schema.WeaviateSchemaDumpOKBody{
		Actions: dbSchema.Actions,
		Things:  dbSchema.Things,
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) addThing(params schema.WeaviateSchemaThingsCreateParams, principal *models.Principal) middleware.Responder {
	err := s.manager.AddThing(params.HTTPRequest.Context(), params.ThingClass)
	if err != nil {
		return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewWeaviateSchemaThingsCreateOK().WithPayload(params.ThingClass)
}

func (s *schemaHandlers) deleteThing(params schema.WeaviateSchemaThingsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThing(params.HTTPRequest.Context(), params.ClassName)
	if err != nil {
		return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsDeleteOK()
}

func (s *schemaHandlers) addThingProperty(params schema.WeaviateSchemaThingsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddThingProperty(params.HTTPRequest.Context(), params.ClassName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaThingsPropertiesAddUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteThingProperty(params schema.WeaviateSchemaThingsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThingProperty(params.HTTPRequest.Context(), params.ClassName, params.PropertyName)
	if err != nil {
		return schema.NewWeaviateSchemaThingsPropertiesDeleteInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateThingProperty(params schema.WeaviateSchemaThingsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThingProperty(ctx, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaThingsPropertiesUpdateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateThing(params schema.WeaviateSchemaThingsUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThing(ctx, params.ClassName, params.Body)
	if err != nil {
		return schema.NewWeaviateSchemaThingsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsUpdateOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *schemaUC.Manager) {
	h := &schemaHandlers{requestsLog, manager}

	api.SchemaWeaviateSchemaActionsCreateHandler = schema.
		WeaviateSchemaActionsCreateHandlerFunc(h.addAction)
	api.SchemaWeaviateSchemaActionsUpdateHandler = schema.
		WeaviateSchemaActionsUpdateHandlerFunc(h.updateAction)
	api.SchemaWeaviateSchemaActionsDeleteHandler = schema.
		WeaviateSchemaActionsDeleteHandlerFunc(h.deleteAction)
	api.SchemaWeaviateSchemaActionsPropertiesAddHandler = schema.
		WeaviateSchemaActionsPropertiesAddHandlerFunc(h.addActionProperty)
	api.SchemaWeaviateSchemaActionsPropertiesDeleteHandler = schema.
		WeaviateSchemaActionsPropertiesDeleteHandlerFunc(h.deleteActionProperty)
	api.SchemaWeaviateSchemaActionsPropertiesUpdateHandler = schema.
		WeaviateSchemaActionsPropertiesUpdateHandlerFunc(h.updateActionProperty)

	api.SchemaWeaviateSchemaThingsCreateHandler = schema.
		WeaviateSchemaThingsCreateHandlerFunc(h.addThing)
	api.SchemaWeaviateSchemaThingsUpdateHandler = schema.
		WeaviateSchemaThingsUpdateHandlerFunc(h.updateThing)
	api.SchemaWeaviateSchemaThingsDeleteHandler = schema.
		WeaviateSchemaThingsDeleteHandlerFunc(h.deleteThing)
	api.SchemaWeaviateSchemaThingsPropertiesAddHandler = schema.
		WeaviateSchemaThingsPropertiesAddHandlerFunc(h.addThingProperty)
	api.SchemaWeaviateSchemaThingsPropertiesDeleteHandler = schema.
		WeaviateSchemaThingsPropertiesDeleteHandlerFunc(h.deleteThingProperty)
	api.SchemaWeaviateSchemaThingsPropertiesUpdateHandler = schema.
		WeaviateSchemaThingsPropertiesUpdateHandlerFunc(h.updateThingProperty)

	api.SchemaWeaviateSchemaDumpHandler = schema.
		WeaviateSchemaDumpHandlerFunc(h.getSchema)
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
