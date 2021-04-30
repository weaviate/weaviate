//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"

	"github.com/semi-technologies/weaviate/entities/models"
)

type schemaHandlers struct {
	manager *schemaUC.Manager
}

func (s *schemaHandlers) addObject(params schema.SchemaObjectsCreateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddObject(params.HTTPRequest.Context(), principal, params.ObjectClass)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaObjectsCreateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) updateObject(params schema.SchemaObjectsUpdateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.UpdateClass(params.HTTPRequest.Context(), principal, params.ClassName,
		params.ObjectClass)
	if err != nil {
		if err == schemaUC.ErrNotFound {
			return schema.NewSchemaObjectsUpdateNotFound()
		}

		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaObjectsUpdateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) getObject(params schema.SchemaObjectsGetParams,
	principal *models.Principal) middleware.Responder {
	class, err := s.manager.GetClass(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if class == nil {
		return schema.NewSchemaObjectsGetNotFound()
	}

	return schema.NewSchemaObjectsGetOK().WithPayload(class)
}

func (s *schemaHandlers) deleteObject(params schema.SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteObject(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaObjectsDeleteOK()
}

func (s *schemaHandlers) addObjectProperty(params schema.SchemaObjectsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddObjectProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaObjectsPropertiesAddOK().WithPayload(params.Body)
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

	payload := dbSchema.Objects

	return schema.NewSchemaDumpOK().WithPayload(payload)
}

func setupSchemaHandlers(api *operations.WeaviateAPI, manager *schemaUC.Manager) {
	h := &schemaHandlers{manager}

	api.SchemaSchemaObjectsCreateHandler = schema.
		SchemaObjectsCreateHandlerFunc(h.addObject)
	api.SchemaSchemaObjectsDeleteHandler = schema.
		SchemaObjectsDeleteHandlerFunc(h.deleteObject)
	api.SchemaSchemaObjectsPropertiesAddHandler = schema.
		SchemaObjectsPropertiesAddHandlerFunc(h.addObjectProperty)

	api.SchemaSchemaObjectsUpdateHandler = schema.
		SchemaObjectsUpdateHandlerFunc(h.updateObject)

	api.SchemaSchemaObjectsGetHandler = schema.
		SchemaObjectsGetHandlerFunc(h.getObject)
	api.SchemaSchemaDumpHandler = schema.
		SchemaDumpHandlerFunc(h.getSchema)
}
