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
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/batching"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

type batchKindHandlers struct {
	manager *kinds.BatchManager
}

func (h *batchKindHandlers) addObjects(params batching.BatchingObjectsCreateParams,
	principal *models.Principal) middleware.Responder {
	objects, err := h.manager.AddObjects(params.HTTPRequest.Context(), principal,
		params.Body.Objects, params.Body.Fields)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return batching.NewBatchingObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return batching.NewBatchingObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batching.NewBatchingObjectsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batching.NewBatchingObjectsCreateOK().
		WithPayload(h.objectsResponse(objects))
}

func (h *batchKindHandlers) objectsResponse(input kinds.BatchObjects) []*models.ObjectsGetResponse {
	response := make([]*models.ObjectsGetResponse, len(input))
	for i, object := range input {
		var errorResponse *models.ErrorResponse
		if object.Err != nil {
			errorResponse = errPayloadFromSingleErr(object.Err)
		}

		object.Object.ID = object.UUID
		response[i] = &models.ObjectsGetResponse{
			Object: *object.Object,
			Result: &models.ObjectsGetResponseAO2Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}

func (h *batchKindHandlers) addReferences(params batching.BatchingReferencesCreateParams,
	principal *models.Principal) middleware.Responder {
	references, err := h.manager.AddReferences(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return batching.NewBatchingReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return batching.NewBatchingReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batching.NewBatchingReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batching.NewBatchingReferencesCreateOK().
		WithPayload(h.referencesResponse(references))
}

func (h *batchKindHandlers) referencesResponse(input kinds.BatchReferences) []*models.BatchReferenceResponse {
	response := make([]*models.BatchReferenceResponse, len(input))
	for i, ref := range input {
		var errorResponse *models.ErrorResponse
		var reference models.BatchReference

		status := models.BatchReferenceResponseAO1ResultStatusSUCCESS
		if ref.Err != nil {
			errorResponse = errPayloadFromSingleErr(ref.Err)
			status = models.BatchReferenceResponseAO1ResultStatusFAILED
		} else {
			reference.From = strfmt.URI(ref.From.String())
			reference.To = strfmt.URI(ref.To.String())
		}

		response[i] = &models.BatchReferenceResponse{
			BatchReference: reference,
			Result: &models.BatchReferenceResponseAO1Result{
				Errors: errorResponse,
				Status: &status,
			},
		}
	}

	return response
}

func setupKindBatchHandlers(api *operations.WeaviateAPI, manager *kinds.BatchManager) {
	h := &batchKindHandlers{manager}

	api.BatchingBatchingObjectsCreateHandler = batching.
		BatchingObjectsCreateHandlerFunc(h.addObjects)
	api.BatchingBatchingReferencesCreateHandler = batching.
		BatchingReferencesCreateHandlerFunc(h.addReferences)
}
