/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package rest

import (
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
	"github.com/creativesoftwarefdn/weaviate/usecases/telemetry"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

type batchKindHandlers struct {
	manager     *kinds.BatchManager
	requestsLog *telemetry.RequestsLog
}

func (h *batchKindHandlers) addThings(params operations.WeaviateBatchingThingsCreateParams, principal *models.Principal) middleware.Responder {
	things, err := h.manager.AddThings(params.HTTPRequest.Context(), params.Body.Things, params.Body.Fields)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return operations.NewWeaviateBatchingThingsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	for range params.Body.Things {
		h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	}
	return operations.NewWeaviateBatchingThingsCreateOK().
		WithPayload(h.thingsResponse(things))
}

func (h *batchKindHandlers) thingsResponse(input kinds.BatchThings) []*models.ThingsGetResponse {
	response := make([]*models.ThingsGetResponse, len(input), len(input))
	for i, thing := range input {
		var errorResponse *models.ErrorResponse
		if thing.Err != nil {
			errorResponse = errPayloadFromSingleErr(thing.Err)
		}

		thing.Thing.ID = thing.UUID
		response[i] = &models.ThingsGetResponse{
			Thing: *thing.Thing,
			Result: &models.ThingsGetResponseAO1Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}

func (h *batchKindHandlers) addActions(params operations.WeaviateBatchingActionsCreateParams, principal *models.Principal) middleware.Responder {
	actions, err := h.manager.AddActions(params.HTTPRequest.Context(), params.Body.Actions, params.Body.Fields)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return operations.NewWeaviateBatchingActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return operations.NewWeaviateBatchingActionsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	for range params.Body.Actions {
		h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	}
	return operations.NewWeaviateBatchingActionsCreateOK().
		WithPayload(h.actionsResponse(actions))
}

func (h *batchKindHandlers) actionsResponse(input kinds.BatchActions) []*models.ActionsGetResponse {
	response := make([]*models.ActionsGetResponse, len(input), len(input))
	for i, action := range input {
		var errorResponse *models.ErrorResponse
		if action.Err != nil {
			errorResponse = errPayloadFromSingleErr(action.Err)
		}

		action.Action.ID = action.UUID
		response[i] = &models.ActionsGetResponse{
			Action: *action.Action,
			Result: &models.ActionsGetResponseAO1Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}

func (h *batchKindHandlers) addReferences(params operations.WeaviateBatchingReferencesCreateParams, principal *models.Principal) middleware.Responder {
	references, err := h.manager.AddReferences(params.HTTPRequest.Context(), params.Body)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return operations.NewWeaviateBatchingReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return operations.NewWeaviateBatchingReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	for range params.Body {
		h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	}
	return operations.NewWeaviateBatchingReferencesCreateOK().
		WithPayload(h.referencesResponse(references))
}

func (h *batchKindHandlers) referencesResponse(input kinds.BatchReferences) []*models.BatchReferenceResponse {
	response := make([]*models.BatchReferenceResponse, len(input), len(input))
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

func setupKindBatchHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *kinds.BatchManager) {
	h := &batchKindHandlers{manager, requestsLog}

	api.WeaviateBatchingThingsCreateHandler = operations.
		WeaviateBatchingThingsCreateHandlerFunc(h.addThings)
	api.WeaviateBatchingActionsCreateHandler = operations.
		WeaviateBatchingActionsCreateHandlerFunc(h.addActions)
	api.WeaviateBatchingReferencesCreateHandler = operations.
		WeaviateBatchingReferencesCreateHandlerFunc(h.addReferences)
}

func (h *batchKindHandlers) telemetryLogAsync(requestType, identifier string) {
	go func() {
		h.requestsLog.Register(requestType, identifier)
	}()
}
