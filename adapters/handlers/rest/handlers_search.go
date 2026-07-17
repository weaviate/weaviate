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

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	aggregateops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/aggregate"
	searchops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/search"
	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
)

// setupSearchHandlers wires the REST search API (operations search.nearText,
// search.bm25, search.hybrid and search.nearObject, POST
// /v1/search/{collection}/{near-text,bm25,hybrid,near-object}) and its
// sibling aggregate API (operation aggregate, POST
// /v1/aggregate/{collection}). The handler logic lives in
// adapters/handlers/rest/search. The endpoints are experimental and off by
// default; EXPERIMENTAL_REST_SEARCH_ENABLED=true enables them. When disabled
// they reject requests with 422.
func setupSearchHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := restsearch.NewHandler(restsearch.HandlerConfig{
		Traverser:         appState.Traverser,
		SchemaReader:      appState.SchemaManager,
		Authorizer:        appState.Authorizer,
		NamespacesEnabled: appState.ServerConfig.Config.Namespaces.Enabled,
		DefaultLimit:      appState.ServerConfig.Config.QueryDefaults.Limit,
		MaximumResults:    appState.ServerConfig.Config.QueryMaximumResults,
		Enabled:           appState.ServerConfig.Config.ExperimentalRESTSearchEnabled,
		Logger:            appState.Logger,
	})

	// swagger-layer errors (bind validation, security, routing) on search
	// and aggregate routes use the same ErrorResponse shape as handler errors
	defaultServeError := api.ServeError
	if defaultServeError == nil {
		defaultServeError = openapierrors.ServeError
	}
	api.ServeError = func(rw http.ResponseWriter, r *http.Request, err error) {
		if r != nil && (restsearch.IsSearchRoute(r.URL.Path) || restsearch.IsAggregateRoute(r.URL.Path)) {
			restsearch.ServeError(rw, r, err)
			return
		}
		defaultServeError(rw, r, err)
	}

	api.SearchSearchNearTextHandler = searchops.SearchNearTextHandlerFunc(
		func(params searchops.SearchNearTextParams, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.NearText(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return searchNearTextErrResponder(apiErr)
			}
			return searchops.NewSearchNearTextOK().WithPayload(payload)
		})

	api.SearchSearchBm25Handler = searchops.SearchBm25HandlerFunc(
		func(params searchops.SearchBm25Params, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.Bm25(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return searchBm25ErrResponder(apiErr)
			}
			return searchops.NewSearchBm25OK().WithPayload(payload)
		})

	api.SearchSearchHybridHandler = searchops.SearchHybridHandlerFunc(
		func(params searchops.SearchHybridParams, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.Hybrid(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return searchHybridErrResponder(apiErr)
			}
			return searchops.NewSearchHybridOK().WithPayload(payload)
		})

	api.SearchSearchNearObjectHandler = searchops.SearchNearObjectHandlerFunc(
		func(params searchops.SearchNearObjectParams, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.NearObject(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return searchNearObjectErrResponder(apiErr)
			}
			return searchops.NewSearchNearObjectOK().WithPayload(payload)
		})

	api.AggregateAggregateHandler = aggregateops.AggregateHandlerFunc(
		func(params aggregateops.AggregateParams, principal *models.Principal) middleware.Responder {
			payload, apiErr := h.Aggregate(params.HTTPRequest.Context(), principal, params.Collection, params.Body)
			if apiErr != nil {
				return aggregateErrResponder(apiErr)
			}
			return aggregateops.NewAggregateOK().WithPayload(payload)
		})
}

// searchErrPayload renders a search APIError as the standard REST error body.
func searchErrPayload(apiErr *restsearch.APIError) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{{Message: apiErr.Error()}},
	}
}

// searchNearTextErrResponder translates a search APIError into the generated
// responder for its status, keeping the standard REST error shape.
func searchNearTextErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := searchErrPayload(apiErr)

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
		// statuses the handler never produces itself; the declared 401/503
		// are answered above it (security layer, op-mode middleware)
		return middleware.Error(apiErr.Status, payload)
	}
}

// searchHybridErrResponder translates a search APIError into the generated
// responder for its status, keeping the standard REST error shape.
func searchHybridErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := searchErrPayload(apiErr)

	switch apiErr.Status {
	case http.StatusBadRequest:
		return searchops.NewSearchHybridBadRequest().WithPayload(payload)
	case http.StatusForbidden:
		return searchops.NewSearchHybridForbidden().WithPayload(payload)
	case http.StatusNotFound:
		return searchops.NewSearchHybridNotFound().WithPayload(payload)
	case http.StatusUnprocessableEntity:
		return searchops.NewSearchHybridUnprocessableEntity().WithPayload(payload)
	case http.StatusTooManyRequests:
		return searchops.NewSearchHybridTooManyRequests().WithPayload(payload)
	case http.StatusBadGateway:
		return searchops.NewSearchHybridBadGateway().WithPayload(payload)
	case http.StatusInternalServerError:
		return searchops.NewSearchHybridInternalServerError().WithPayload(payload)
	default:
		// statuses the handler never produces itself; the declared 401/503
		// are answered above it (security layer, op-mode middleware)
		return middleware.Error(apiErr.Status, payload)
	}
}

// searchNearObjectErrResponder translates a search APIError into the
// generated responder for its status, keeping the standard REST error shape.
// near-object declares no 502 — the source object's stored vector anchors
// the search, so no embedding provider is ever called.
func searchNearObjectErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := searchErrPayload(apiErr)

	switch apiErr.Status {
	case http.StatusBadRequest:
		return searchops.NewSearchNearObjectBadRequest().WithPayload(payload)
	case http.StatusForbidden:
		return searchops.NewSearchNearObjectForbidden().WithPayload(payload)
	case http.StatusNotFound:
		return searchops.NewSearchNearObjectNotFound().WithPayload(payload)
	case http.StatusUnprocessableEntity:
		return searchops.NewSearchNearObjectUnprocessableEntity().WithPayload(payload)
	case http.StatusTooManyRequests:
		return searchops.NewSearchNearObjectTooManyRequests().WithPayload(payload)
	case http.StatusInternalServerError:
		return searchops.NewSearchNearObjectInternalServerError().WithPayload(payload)
	default:
		// statuses the handler never produces itself; the declared 401/503
		// are answered above it (security layer, op-mode middleware)
		return middleware.Error(apiErr.Status, payload)
	}
}

// aggregateErrResponder translates an aggregate APIError into the generated
// responder for its status, keeping the standard REST error shape. aggregate
// declares neither 502 (nothing is ever vectorized) nor 429 (the traverser's
// aggregate path is not rate limited).
func aggregateErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := searchErrPayload(apiErr)

	switch apiErr.Status {
	case http.StatusBadRequest:
		return aggregateops.NewAggregateBadRequest().WithPayload(payload)
	case http.StatusForbidden:
		return aggregateops.NewAggregateForbidden().WithPayload(payload)
	case http.StatusNotFound:
		return aggregateops.NewAggregateNotFound().WithPayload(payload)
	case http.StatusUnprocessableEntity:
		return aggregateops.NewAggregateUnprocessableEntity().WithPayload(payload)
	case http.StatusInternalServerError:
		return aggregateops.NewAggregateInternalServerError().WithPayload(payload)
	default:
		// statuses without a declared response
		return middleware.Error(apiErr.Status, payload)
	}
}

// searchBm25ErrResponder translates a search APIError into the generated
// responder for its status, keeping the standard REST error shape. bm25
// declares no 502 — a keyword search never calls an embedding provider.
func searchBm25ErrResponder(apiErr *restsearch.APIError) middleware.Responder {
	payload := searchErrPayload(apiErr)

	switch apiErr.Status {
	case http.StatusBadRequest:
		return searchops.NewSearchBm25BadRequest().WithPayload(payload)
	case http.StatusForbidden:
		return searchops.NewSearchBm25Forbidden().WithPayload(payload)
	case http.StatusNotFound:
		return searchops.NewSearchBm25NotFound().WithPayload(payload)
	case http.StatusUnprocessableEntity:
		return searchops.NewSearchBm25UnprocessableEntity().WithPayload(payload)
	case http.StatusTooManyRequests:
		return searchops.NewSearchBm25TooManyRequests().WithPayload(payload)
	case http.StatusInternalServerError:
		return searchops.NewSearchBm25InternalServerError().WithPayload(payload)
	default:
		// statuses the handler never produces itself; the declared 401/503
		// are answered above it (security layer, op-mode middleware)
		return middleware.Error(apiErr.Status, payload)
	}
}
