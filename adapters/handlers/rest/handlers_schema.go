//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type schemaHandlers struct {
	manager *schemaUC.Manager
}

func (s *schemaHandlers) addClass(params schema.SchemaObjectsCreateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddClass(params.HTTPRequest.Context(), principal, params.ObjectClass)
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

func (s *schemaHandlers) updateClass(params schema.SchemaObjectsUpdateParams,
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

func (s *schemaHandlers) getClass(params schema.SchemaObjectsGetParams,
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

func (s *schemaHandlers) deleteClass(params schema.SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteClass(params.HTTPRequest.Context(), principal, params.ClassName)
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

func (s *schemaHandlers) addClassProperty(params schema.SchemaObjectsPropertiesAddParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.AddClassProperty(params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
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

func (s *schemaHandlers) getShardsStatus(params schema.SchemaObjectsShardsGetParams,
	principal *models.Principal) middleware.Responder {
	status, err := s.manager.GetShardsStatus(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsGetNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := status

	return schema.NewSchemaObjectsShardsGetOK().WithPayload(payload)
}

func (s *schemaHandlers) updateShardStatus(params schema.SchemaObjectsShardsUpdateParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.UpdateShardStatus(
		params.HTTPRequest.Context(), principal, params.ClassName, params.ShardName, params.Body.Status)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := params.Body

	return schema.NewSchemaObjectsShardsUpdateOK().WithPayload(payload)
}

func (s *schemaHandlers) createSnapshot(params schema.SchemaObjectsSnapshotsCreateParams,
	principal *models.Principal) middleware.Responder {
	snapshot, err := s.manager.CreateSnapshot(params.HTTPRequest.Context(), principal,
		params.ClassName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsSnapshotsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsSnapshotsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaObjectsSnapshotsCreateOK().WithPayload(snapshot)
}

func (s *schemaHandlers) restoreSnapshot(params schema.SchemaObjectsSnapshotsRestoreParams,
	principal *models.Principal) middleware.Responder {
	err := s.manager.RestoreSnapshot(params.HTTPRequest.Context(), principal,
		params.ClassName, params.ID)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsSnapshotsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			switch err {
			case schemaUC.ErrNotFound:
				return schema.NewSchemaObjectsSnapshotsRestoreNotFound()
			default:
				return schema.NewSchemaObjectsSnapshotsRestoreUnprocessableEntity().
					WithPayload(errPayloadFromSingleErr(err))
			}
		}
	}

	return schema.NewSchemaObjectsSnapshotsRestoreOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, manager *schemaUC.Manager) {
	h := &schemaHandlers{manager}

	api.SchemaSchemaObjectsCreateHandler = schema.
		SchemaObjectsCreateHandlerFunc(h.addClass)
	api.SchemaSchemaObjectsDeleteHandler = schema.
		SchemaObjectsDeleteHandlerFunc(h.deleteClass)
	api.SchemaSchemaObjectsPropertiesAddHandler = schema.
		SchemaObjectsPropertiesAddHandlerFunc(h.addClassProperty)

	api.SchemaSchemaObjectsUpdateHandler = schema.
		SchemaObjectsUpdateHandlerFunc(h.updateClass)

	api.SchemaSchemaObjectsGetHandler = schema.
		SchemaObjectsGetHandlerFunc(h.getClass)
	api.SchemaSchemaDumpHandler = schema.
		SchemaDumpHandlerFunc(h.getSchema)

	api.SchemaSchemaObjectsShardsGetHandler = schema.
		SchemaObjectsShardsGetHandlerFunc(h.getShardsStatus)
	api.SchemaSchemaObjectsShardsUpdateHandler = schema.
		SchemaObjectsShardsUpdateHandlerFunc(h.updateShardStatus)

	api.SchemaSchemaObjectsSnapshotsCreateHandler = schema.
		SchemaObjectsSnapshotsCreateHandlerFunc(h.createSnapshot)
	api.SchemaSchemaObjectsSnapshotsRestoreHandler = schema.
		SchemaObjectsSnapshotsRestoreHandlerFunc(h.restoreSnapshot)
}
