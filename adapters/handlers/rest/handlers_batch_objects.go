//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"errors"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/batch"
	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchObjectHandlers struct {
	manager *objects.BatchManager
}

func (h *batchObjectHandlers) addObjects(params batch.BatchObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		return batch.NewBatchObjectsCreateBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	objs, err := h.manager.AddObjects(params.HTTPRequest.Context(), principal,
		params.Body.Objects, params.Body.Fields, repl)
	if err != nil {
		switch err.(type) {
		case autherrs.Forbidden:
			return batch.NewBatchObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case objects.ErrInvalidUserInput:
			return batch.NewBatchObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batch.NewBatchObjectsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batch.NewBatchObjectsCreateOK().
		WithPayload(h.objectsResponse(objs))
}

func (h *batchObjectHandlers) objectsResponse(input objects.BatchObjects) []*models.ObjectsGetResponse {
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

func (h *batchObjectHandlers) addReferences(params batch.BatchReferencesCreateParams,
	principal *models.Principal,
) middleware.Responder {
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		return batch.NewBatchReferencesCreateBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	tenantKey := getTenantKey(params.TenantKey)

	references, err := h.manager.AddReferences(
		params.HTTPRequest.Context(), principal, params.Body, repl, tenantKey)
	if err != nil {
		switch err.(type) {
		case autherrs.Forbidden:
			return batch.NewBatchReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case objects.ErrInvalidUserInput:
			return batch.NewBatchReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batch.NewBatchReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batch.NewBatchReferencesCreateOK().
		WithPayload(h.referencesResponse(references))
}

func (h *batchObjectHandlers) referencesResponse(input objects.BatchReferences) []*models.BatchReferenceResponse {
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

func (h *batchObjectHandlers) deleteObjects(params batch.BatchObjectsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	repl, err := getReplicationProperties(params.ConsistencyLevel, nil)
	if err != nil {
		return batch.NewBatchObjectsDeleteBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	tenantKey := getTenantKey(params.TenantKey)

	res, err := h.manager.DeleteObjects(params.HTTPRequest.Context(), principal,
		params.Body.Match, params.Body.DryRun, params.Body.Output, repl, tenantKey)
	if err != nil {
		if errors.As(err, &objects.ErrInvalidUserInput{}) {
			return batch.NewBatchObjectsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		} else if errors.As(err, &autherrs.Forbidden{}) {
			return batch.NewBatchObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		} else {
			return batch.NewBatchObjectsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batch.NewBatchObjectsDeleteOK().
		WithPayload(h.objectsDeleteResponse(res))
}

func (h *batchObjectHandlers) objectsDeleteResponse(input *objects.BatchDeleteResponse) *models.BatchDeleteResponse {
	var successful, failed int64
	output := input.Output
	var objects []*models.BatchDeleteResponseResultsObjectsItems0
	for _, obj := range input.Result.Objects {
		var errorResponse *models.ErrorResponse

		status := models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS
		if input.DryRun {
			status = models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN
		} else if obj.Err != nil {
			status = models.BatchDeleteResponseResultsObjectsItems0StatusFAILED
			errorResponse = errPayloadFromSingleErr(obj.Err)
			failed += 1
		} else {
			successful += 1
		}

		if output == "minimal" &&
			(status == models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS ||
				status == models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN) {
			// only add SUCCESS and DRYRUN results if output is "verbose"
			continue
		}

		objects = append(objects, &models.BatchDeleteResponseResultsObjectsItems0{
			ID:     obj.UUID,
			Status: &status,
			Errors: errorResponse,
		})
	}

	response := &models.BatchDeleteResponse{
		Match: &models.BatchDeleteResponseMatch{
			Class: input.Match.Class,
			Where: input.Match.Where,
		},
		DryRun: &input.DryRun,
		Output: &output,
		Results: &models.BatchDeleteResponseResults{
			Matches:    input.Result.Matches,
			Limit:      input.Result.Limit,
			Successful: successful,
			Failed:     failed,
			Objects:    objects,
		},
	}
	return response
}

func setupObjectBatchHandlers(api *operations.WeaviateAPI, manager *objects.BatchManager) {
	h := &batchObjectHandlers{manager}

	api.BatchBatchObjectsCreateHandler = batch.
		BatchObjectsCreateHandlerFunc(h.addObjects)
	api.BatchBatchReferencesCreateHandler = batch.
		BatchReferencesCreateHandlerFunc(h.addReferences)
	api.BatchBatchObjectsDeleteHandler = batch.
		BatchObjectsDeleteHandlerFunc(h.deleteObjects)
}
