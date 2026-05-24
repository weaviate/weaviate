//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	reindexusecase "github.com/weaviate/weaviate/usecases/reindex"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// The reindex submit / cancel / status business logic lives in
// [reindexusecase.Service]. This file is a thin HTTP shell:
//
//   - authorize the request (REST concern, not business logic);
//   - parse the swagger-generated params struct;
//   - delegate to the service;
//   - map [reindexusecase] sentinel errors to HTTP status codes via
//     [reindexErrorResponse].
//
// No domain rules should live in this file. If you find yourself
// editing migration-type dispatch, validation, conflict checks, or
// status merging here, that belongs in the usecases package.

func setupIndexesHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := &indexesHandlers{appState: appState}
	api.SchemaSchemaObjectsIndexesGetHandler = schema.SchemaObjectsIndexesGetHandlerFunc(h.getIndexes)
	api.SchemaSchemaObjectsIndexesUpdateHandler = schema.SchemaObjectsIndexesUpdateHandlerFunc(h.updateIndex)
}

type indexesHandlers struct {
	appState *state.State
}

func (h *indexesHandlers) getIndexes(params schema.SchemaObjectsIndexesGetParams, principal *models.Principal) middleware.Responder {
	collection := params.ClassName

	// Require READ on the collection's metadata: this endpoint exposes
	// per-property index state, which is collection-internal information.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.READ, authorization.CollectionsMetadata(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	status, err := h.appState.ReindexService.CollectionStatus(
		params.HTTPRequest.Context(),
		collection,
		h.appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval,
	)
	if err != nil {
		if errors.Is(err, reindexusecase.ErrNotFound) {
			return schema.NewSchemaObjectsIndexesGetNotFound()
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errorResponse(principal, err.Error()))
	}

	props := make([]*models.PropertyIndexStatus, 0, len(status.Properties))
	for _, p := range status.Properties {
		props = append(props, &models.PropertyIndexStatus{
			Name:        p.Name,
			DataType:    p.DataType,
			Description: p.Description,
			Indexes:     p.Indexes,
		})
	}
	return schema.NewSchemaObjectsIndexesGetOK().WithPayload(&models.IndexStatusResponse{
		Collection: collection,
		Properties: props,
	})
}

// updateIndex implements PUT /v1/schema/{className}/indexes/{propertyName}.
// Authorize → delegate → translate. No business logic in this method.
func (h *indexesHandlers) updateIndex(params schema.SchemaObjectsIndexesUpdateParams, principal *models.Principal) middleware.Responder {
	collection := params.ClassName
	propertyName := params.PropertyName

	// UPDATE on the collection itself: submitting a reindex task is a
	// privileged, cluster-wide, destructive operation (rebuilds
	// buckets on every replica, flips schema flags).
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.UPDATE, authorization.Collections(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesUpdateForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	result, err := h.appState.ReindexService.Submit(params.HTTPRequest.Context(), reindexusecase.SubmitRequest{
		Collection:        collection,
		PropertyName:      propertyName,
		Body:              params.Body,
		Tenants:           params.Tenants,
		PrincipalUsername: principalUsername(principal),
	})
	if err != nil {
		return reindexUpdateResponse(principal, err)
	}
	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		TaskID: result.TaskID,
		Status: result.Status,
	})
}

// reindexUpdateResponse maps the service's sentinel errors to the
// generated swagger response shapes. Centralised here so handlers
// don't sprinkle status-code policy through their bodies. Every
// failure path carries the error message body — operators rely on
// the structured payload to find out which (collection, property,
// indexType) tuple was missing (pinned by
// TestBackupVsReindexSuite/CancelOnNoInFlightReturnsStructured404).
func reindexUpdateResponse(principal *models.Principal, err error) middleware.Responder {
	payload := errorResponse(principal, err.Error())
	switch {
	case errors.Is(err, reindexusecase.ErrBadRequest):
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(payload)
	case errors.Is(err, reindexusecase.ErrNotFound):
		return schema.NewSchemaObjectsIndexesUpdateNotFound().WithPayload(payload)
	case errors.Is(err, reindexusecase.ErrConflict):
		return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(payload)
	case errors.Is(err, reindexusecase.ErrServiceUnavailable):
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(payload)
	default:
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(payload)
	}
}

// principalUsername extracts the user-facing identifier for the audit
// log line the service emits at submit/cancel time. Falls back to
// "anonymous" when the principal is nil.
func principalUsername(principal *models.Principal) string {
	if principal == nil {
		return "anonymous"
	}
	return principal.Username
}

// errorResponse wraps a single-error response with the namespace-aware
// message stripper.
func errorResponse(principal *models.Principal, msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: namespacing.StripErrorMessage(principal, msg)},
		},
	}
}
