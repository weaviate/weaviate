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

func (h *batchKindHandlers) addThings(params batching.BatchingThingsCreateParams,
	principal *models.Principal) middleware.Responder {
	things, err := h.manager.AddThings(params.HTTPRequest.Context(), principal,
		params.Body.Things, params.Body.Fields)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return batching.NewBatchingThingsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return batching.NewBatchingThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batching.NewBatchingThingsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batching.NewBatchingThingsCreateOK().
		WithPayload(h.thingsResponse(things))
}

func (h *batchKindHandlers) thingsResponse(input kinds.BatchThings) []*models.ThingsGetResponse {
	response := make([]*models.ThingsGetResponse, len(input))
	for i, thing := range input {
		var errorResponse *models.ErrorResponse
		if thing.Err != nil {
			errorResponse = errPayloadFromSingleErr(thing.Err)
		}

		thing.Thing.ID = thing.UUID
		response[i] = &models.ThingsGetResponse{
			Thing: *thing.Thing,
			Result: &models.ThingsGetResponseAO2Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}

func (h *batchKindHandlers) addActions(params batching.BatchingActionsCreateParams,
	principal *models.Principal) middleware.Responder {
	actions, err := h.manager.AddActions(params.HTTPRequest.Context(), principal,
		params.Body.Actions, params.Body.Fields)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return batching.NewBatchingActionsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return batching.NewBatchingActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return batching.NewBatchingActionsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return batching.NewBatchingActionsCreateOK().
		WithPayload(h.actionsResponse(actions))
}

func (h *batchKindHandlers) actionsResponse(input kinds.BatchActions) []*models.ActionsGetResponse {
	response := make([]*models.ActionsGetResponse, len(input))
	for i, action := range input {
		var errorResponse *models.ErrorResponse
		if action.Err != nil {
			errorResponse = errPayloadFromSingleErr(action.Err)
		}

		action.Action.ID = action.UUID
		response[i] = &models.ActionsGetResponse{
			Action: *action.Action,
			Result: &models.ActionsGetResponseAO2Result{
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

	api.BatchingBatchingThingsCreateHandler = batching.
		BatchingThingsCreateHandlerFunc(h.addThings)
	api.BatchingBatchingActionsCreateHandler = batching.
		BatchingActionsCreateHandlerFunc(h.addActions)
	api.BatchingBatchingReferencesCreateHandler = batching.
		BatchingReferencesCreateHandlerFunc(h.addReferences)
}
