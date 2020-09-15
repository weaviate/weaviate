//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
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

func setupSchemaHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *schemaUC.Manager) {
	h := &schemaHandlers{requestsLog, manager}

	api.SchemaSchemaActionsCreateHandler = schema.
		SchemaActionsCreateHandlerFunc(h.addAction)
	api.SchemaSchemaActionsDeleteHandler = schema.
		SchemaActionsDeleteHandlerFunc(h.deleteAction)
	api.SchemaSchemaActionsPropertiesAddHandler = schema.
		SchemaActionsPropertiesAddHandlerFunc(h.addActionProperty)

	api.SchemaSchemaThingsCreateHandler = schema.
		SchemaThingsCreateHandlerFunc(h.addThing)
	api.SchemaSchemaThingsDeleteHandler = schema.
		SchemaThingsDeleteHandlerFunc(h.deleteThing)
	api.SchemaSchemaThingsPropertiesAddHandler = schema.
		SchemaThingsPropertiesAddHandlerFunc(h.addThingProperty)

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
