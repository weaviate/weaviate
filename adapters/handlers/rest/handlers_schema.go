//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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

func (s *schemaHandlers) addAction(params schema.SchemaActionsCreateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddAction(params.HTTPRequest.Context(), principal, params.ActionClass)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewSchemaActionsCreateOK().WithPayload(params.ActionClass)
}

func (s *schemaHandlers) deleteAction(params schema.SchemaActionsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteAction(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaActionsDeleteOK()
}

func (s *schemaHandlers) addActionProperty(params schema.SchemaActionsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddActionProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaActionsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteActionProperty(params schema.SchemaActionsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteActionProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.PropertyName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsPropertiesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaActionsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateActionProperty(params schema.SchemaActionsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateActionProperty(ctx, principal, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsPropertiesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaActionsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateAction(params schema.SchemaActionsUpdateParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateAction(ctx, principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaActionsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaActionsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaActionsUpdateOK()
}

func (s *schemaHandlers) getSchema(params schema.SchemaDumpParams, principal *models.Principal) middleware.Responder {
	dbSchema, err := s.manager.GetSchema(principal)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaDumpForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaDumpForbidden().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := &schema.SchemaDumpOKBody{
		Actions: dbSchema.Actions,
		Things:  dbSchema.Things,
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) addThing(params schema.SchemaThingsCreateParams, principal *models.Principal) middleware.Responder {
	err := s.manager.AddThing(params.HTTPRequest.Context(), principal, params.ThingClass)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAddMeta)
	return schema.NewSchemaThingsCreateOK().WithPayload(params.ThingClass)
}

func (s *schemaHandlers) deleteThing(params schema.SchemaThingsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThing(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaThingsDeleteOK()
}

func (s *schemaHandlers) addThingProperty(params schema.SchemaThingsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddThingProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaThingsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) deleteThingProperty(params schema.SchemaThingsPropertiesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteThingProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.PropertyName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsPropertiesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsPropertiesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaThingsPropertiesDeleteOK()
}

func (s *schemaHandlers) updateThingProperty(params schema.SchemaThingsPropertiesUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThingProperty(ctx, principal, params.ClassName, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsPropertiesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsPropertiesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaThingsPropertiesUpdateOK()
}

func (s *schemaHandlers) updateThing(params schema.SchemaThingsUpdateParams,
	principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	err := s.manager.UpdateThing(ctx, principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaThingsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaThingsUpdateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulateMeta)
	return schema.NewSchemaThingsUpdateOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *schemaUC.Manager) {
	h := &schemaHandlers{requestsLog, manager}

	api.SchemaSchemaActionsCreateHandler = schema.
		SchemaActionsCreateHandlerFunc(h.addAction)
	api.SchemaSchemaActionsUpdateHandler = schema.
		SchemaActionsUpdateHandlerFunc(h.updateAction)
	api.SchemaSchemaActionsDeleteHandler = schema.
		SchemaActionsDeleteHandlerFunc(h.deleteAction)
	api.SchemaSchemaActionsPropertiesAddHandler = schema.
		SchemaActionsPropertiesAddHandlerFunc(h.addActionProperty)
	api.SchemaSchemaActionsPropertiesDeleteHandler = schema.
		SchemaActionsPropertiesDeleteHandlerFunc(h.deleteActionProperty)
	api.SchemaSchemaActionsPropertiesUpdateHandler = schema.
		SchemaActionsPropertiesUpdateHandlerFunc(h.updateActionProperty)

	api.SchemaSchemaThingsCreateHandler = schema.
		SchemaThingsCreateHandlerFunc(h.addThing)
	api.SchemaSchemaThingsUpdateHandler = schema.
		SchemaThingsUpdateHandlerFunc(h.updateThing)
	api.SchemaSchemaThingsDeleteHandler = schema.
		SchemaThingsDeleteHandlerFunc(h.deleteThing)
	api.SchemaSchemaThingsPropertiesAddHandler = schema.
		SchemaThingsPropertiesAddHandlerFunc(h.addThingProperty)
	api.SchemaSchemaThingsPropertiesDeleteHandler = schema.
		SchemaThingsPropertiesDeleteHandlerFunc(h.deleteThingProperty)
	api.SchemaSchemaThingsPropertiesUpdateHandler = schema.
		SchemaThingsPropertiesUpdateHandlerFunc(h.updateThingProperty)

	api.SchemaSchemaDumpHandler = schema.
		SchemaDumpHandlerFunc(h.getSchema)
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
