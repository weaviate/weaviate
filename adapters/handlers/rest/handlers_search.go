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
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	searchops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/search"
	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
)

// setupSearchHandlers wires the REST search API (search.nearText operation,
// POST /v1/search/{collection}/near-text). The handler logic lives in
// adapters/handlers/rest/search. DISABLE_REST_SEARCH rejects requests with
// 422.
func setupSearchHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := restsearch.NewHandler(restsearch.HandlerConfig{
		Traverser:         appState.Traverser,
		SchemaReader:      appState.SchemaManager,
		Authorizer:        appState.Authorizer,
		NamespacesEnabled: appState.ServerConfig.Config.Namespaces.Enabled,
		DefaultLimit:      appState.ServerConfig.Config.QueryDefaults.Limit,
		Disabled:          appState.ServerConfig.Config.DisableRESTSearch,
		Logger:            appState.Logger,
	})

	api.SearchSearchNearTextHandler = searchops.SearchNearTextHandlerFunc(
		func(params searchops.SearchNearTextParams, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.NearText(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return searchNearTextErrResponder(apiErr)
			}
			return searchops.NewSearchNearTextOK().WithPayload(payload)
		})
}

// searchNearTextErrResponder translates a search APIError into the generated
// responder for its status, keeping the standard REST error shape.
func searchNearTextErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{{Message: apiErr.Error()}},
	}

	switch apiErr.Status {
	case http.StatusBadRequest:
		return searchops.NewSearchNearTextBadRequest().WithPayload(payload)
	case http.StatusForbidden:
		return searchops.NewSearchNearTextForbidden().WithPayload(payload)
	case http.StatusNotFound:
		return searchops.NewSearchNearTextNotFound().WithPayload(payload)
	case http.StatusUnprocessableEntity:
		return searchops.NewSearchNearTextUnprocessableEntity().WithPayload(payload)
	case http.StatusTooManyRequests:
		return searchops.NewSearchNearTextTooManyRequests().WithPayload(payload)
	case http.StatusBadGateway:
		return searchops.NewSearchNearTextBadGateway().WithPayload(payload)
	case http.StatusInternalServerError:
		return searchops.NewSearchNearTextInternalServerError().WithPayload(payload)
	default:
		// statuses without a declared response
		return middleware.Error(apiErr.Status, payload)
	}
}
