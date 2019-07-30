//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	"log"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
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
	err := s.manager.AddAction(params.HTTPRequest.Context(), principal, params.ActionClass)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewWeaviateSchemaActionsCreateOK().WithPayload(params.ActionClass)
}

func (s *schemaHandlers) deleteAction(params schema.WeaviateSchemaActionsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteAction(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsDeleteOK()
}

func (s *schemaHandlers) addActionProperty(params schema.WeaviateSchemaActionsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddActionProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteActionProperty(params schema.WeaviateSchemaActionsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteActionProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.PropertyName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsPropertiesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateActionProperty(params schema.WeaviateSchemaActionsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateActionProperty(ctx, principal, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsPropertiesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateAction(params schema.WeaviateSchemaActionsUpdateParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateAction(ctx, principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaActionsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaActionsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaActionsUpdateOK()
}

func (s *schemaHandlers) getSchema(params schema.WeaviateSchemaDumpParams, principal *models.Principal) middleware.Responder {
	dbSchema, err := s.manager.GetSchema(principal)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaDumpForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaDumpForbidden().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := &schema.WeaviateSchemaDumpOKBody{
		Actions: dbSchema.Actions,
		Things:  dbSchema.Things,
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) addThing(params schema.WeaviateSchemaThingsCreateParams, principal *models.Principal) middleware.Responder {
	err := s.manager.AddThing(params.HTTPRequest.Context(), principal, params.ThingClass)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewWeaviateSchemaThingsCreateOK().WithPayload(params.ThingClass)
}

func (s *schemaHandlers) deleteThing(params schema.WeaviateSchemaThingsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThing(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsDeleteOK()
}

func (s *schemaHandlers) addThingProperty(params schema.WeaviateSchemaThingsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddThingProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteThingProperty(params schema.WeaviateSchemaThingsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThingProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.PropertyName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsPropertiesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateThingProperty(params schema.WeaviateSchemaThingsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThingProperty(ctx, principal, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsPropertiesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewWeaviateSchemaThingsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateThing(params schema.WeaviateSchemaThingsUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThing(ctx, principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewWeaviateSchemaThingsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewWeaviateSchemaThingsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}
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
