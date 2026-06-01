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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/restrictions"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// restQueryHandler serves the pure-REST search/aggregate endpoints that mirror
// the gRPC Search/Aggregate RPCs:
//
//	POST /v1/{collection}/query      → Search
//	POST /v1/{collection}/aggregate  → Aggregate
//
// The JSON request body is the protojson encoding of the gRPC SearchRequest /
// AggregateRequest message, and the response is the protojson encoding of the
// corresponding reply. An empty body is treated as an empty request (i.e. a
// default search/aggregate over the collection). The collection name is taken
// from the path and always overrides any value carried in the body.
//
// These endpoints are the GraphQL replacement. Unlike GraphQL they are
// namespace-compatible, because they delegate to the same gRPC pipeline
// (SearchWithPrincipal/AggregateWithPrincipal) which resolves
// namespace-qualified collections and strips namespace details from errors.
//
// They are registered as a custom route in the global middleware chain (see
// makeAddRESTQueryHandlers, wired from makeSetupGlobalMiddleware) rather than as
// go-swagger operations: the body is opaque protojson and does not belong in the
// OpenAPI model surface, and this mirrors how module routes (makeAddModuleHandlers)
// are attached. As a result authentication is performed here rather than by the
// swagger security middleware, matching the gRPC handler's own auth flow.
type restQueryHandler struct {
	querier              state.GRPCQuerier
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	disabled             bool
	maxBodyBytes         int64
	logger               logrus.FieldLogger
}

type restQueryKind int

const (
	restQueryKindSearch restQueryKind = iota
	restQueryKindAggregate
)

// makeAddRESTQueryHandlers builds the middleware that intercepts the REST
// query/aggregate routes and falls through to next for everything else.
func makeAddRESTQueryHandlers(appState *state.State) func(http.Handler) http.Handler {
	h := &restQueryHandler{
		querier: appState.GRPCQuerier,
		authComposer: composer.New(
			appState.ServerConfig.Config.Authentication,
			appState.APIKey, appState.OIDC),
		allowAnonymousAccess: appState.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		disabled:             appState.ServerConfig.Config.DisableRESTQuery,
		maxBodyBytes:         int64(appState.ServerConfig.Config.GRPC.MaxMsgSize),
		logger:               appState.Logger,
	}
	return h.middleware
}

func (h *restQueryHandler) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection, kind, ok := matchRESTQueryPath(r.URL.Path)
		if !ok || r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		h.serve(w, r, collection, kind)
	})
}

// matchRESTQueryPath returns the (url-decoded) collection and the query kind for
// paths of the exact shape /v1/{collection}/query or /v1/{collection}/aggregate.
// ok is false for anything else so the caller falls through to the next handler.
// No existing REST route has the {x}/query or {x}/aggregate shape, so this never
// shadows another endpoint.
func matchRESTQueryPath(path string) (collection string, kind restQueryKind, ok bool) {
	const prefix = "/v1/"
	if !strings.HasPrefix(path, prefix) {
		return "", 0, false
	}
	rest := strings.TrimPrefix(path, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return "", 0, false
	}
	col, verb := rest[:slash], rest[slash+1:]
	// Require exactly two segments: a non-empty collection and a verb with no
	// further path elements.
	if col == "" || verb == "" || strings.ContainsRune(verb, '/') {
		return "", 0, false
	}
	switch verb {
	case "query":
		kind = restQueryKindSearch
	case "aggregate":
		kind = restQueryKindAggregate
	default:
		return "", 0, false
	}
	decoded, err := url.PathUnescape(col)
	if err != nil {
		decoded = col
	}
	return decoded, kind, true
}

func (h *restQueryHandler) serve(w http.ResponseWriter, r *http.Request, collection string, kind restQueryKind) {
	if h.disabled {
		h.writeError(w, http.StatusUnprocessableEntity, nil, fmt.Errorf("rest query api is disabled"))
		return
	}
	if h.querier == nil {
		// Defensive: the querier is set when the gRPC server is created, which
		// always happens during startup. If it is somehow unset, fail loudly
		// rather than panic.
		h.writeError(w, http.StatusInternalServerError, nil, fmt.Errorf("query pipeline is not available"))
		return
	}

	principal, err := h.principalFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, nil, err)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBodyBytes))
	if err != nil {
		h.writeError(w, http.StatusRequestEntityTooLarge, principal,
			fmt.Errorf("read request body: %w", err))
		return
	}

	// Peel off the optional REST `where` filter before the strict protojson
	// parse (it is not part of the protobuf request).
	reqBody, where, err := splitWhereFilter(body)
	if err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, principal, err)
		return
	}

	switch kind {
	case restQueryKindSearch:
		req := &pbv1.SearchRequest{}
		if err := unmarshalRequestBody(reqBody, req); err != nil {
			h.writeError(w, http.StatusUnprocessableEntity, principal,
				fmt.Errorf("parse request body: %w", err))
			return
		}
		req.Collection = collection
		if where != nil && req.Filters != nil {
			h.writeError(w, http.StatusUnprocessableEntity, principal, errWhereAndFilters)
			return
		}
		reply, err := h.querier.SearchWithPrincipal(r.Context(), principal, req, where)
		if err != nil {
			h.writeError(w, httpStatusForQueryError(err), principal, err)
			return
		}
		h.writeProto(w, principal, reply)
	case restQueryKindAggregate:
		req := &pbv1.AggregateRequest{}
		if err := unmarshalRequestBody(reqBody, req); err != nil {
			h.writeError(w, http.StatusUnprocessableEntity, principal,
				fmt.Errorf("parse request body: %w", err))
			return
		}
		req.Collection = collection
		if where != nil && req.Filters != nil {
			h.writeError(w, http.StatusUnprocessableEntity, principal, errWhereAndFilters)
			return
		}
		reply, err := h.querier.AggregateWithPrincipal(r.Context(), principal, req, where)
		if err != nil {
			h.writeError(w, httpStatusForQueryError(err), principal, err)
			return
		}
		h.writeProto(w, principal, reply)
	}
}

var errWhereAndFilters = errors.New("set either `where` or `filters` in the body, not both")

// unmarshalRequestBody decodes a protojson request body, treating an empty or
// whitespace-only body as an empty (zero-value) request.
func unmarshalRequestBody(body []byte, msg proto.Message) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	return protojson.Unmarshal(body, msg)
}

// splitWhereFilter peels an optional top-level `where` filter (in the REST
// WhereFilter syntax) off the JSON body. It returns the remaining JSON — to be
// parsed as the protobuf request — and the parsed WhereFilter (nil if absent).
// The `where` field is a REST-only convenience that is not part of the protobuf
// request, so it must be removed before the strict protojson parse; the gRPC
// pipeline resolves it server-side via filterext.Parse and it overrides the
// protobuf `filters` field.
func splitWhereFilter(body []byte) (rest []byte, where *models.WhereFilter, err error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return body, nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		// Not a JSON object; leave the body untouched and let the protojson
		// parse surface the error.
		return body, nil, nil
	}
	raw, ok := top["where"]
	if !ok {
		return body, nil, nil
	}
	delete(top, "where")
	var wf models.WhereFilter
	if err := json.Unmarshal(raw, &wf); err != nil {
		return nil, nil, fmt.Errorf("parse `where` filter: %w", err)
	}
	rest, err = json.Marshal(top)
	if err != nil {
		return nil, nil, err
	}
	return rest, &wf, nil
}

// principalFromRequest authenticates the request exactly like the gRPC auth
// handler (adapters/handlers/grpc/v1/auth): a Bearer token is validated via the
// shared composer; a missing/invalid token falls back to anonymous access when
// it is enabled, otherwise the composer rejects it.
func (h *restQueryHandler) principalFromRequest(r *http.Request) (*models.Principal, error) {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		if h.allowAnonymousAccess {
			return nil, nil
		}
		return h.authComposer("", nil)
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	return h.authComposer(token, nil)
}

// httpStatusForQueryError maps an error from the gRPC search/aggregate pipeline
// to an HTTP status. It mirrors the gRPC code mapping in
// adapters/handlers/grpc/server.go (translateTypedError): typed auth, usage-limit
// and restriction errors get precise statuses. Any other error is treated as a
// client/query error (422), consistent with how the GraphQL endpoint surfaces
// query-resolution failures.
func httpStatusForQueryError(err error) int {
	switch {
	case errors.As(err, &authzerrors.Unauthenticated{}):
		return http.StatusUnauthorized
	case errors.As(err, &authzerrors.Forbidden{}):
		return http.StatusForbidden
	}
	if _, ok := usagelimits.AsLimitExceeded(err); ok {
		return http.StatusTooManyRequests
	}
	if _, ok := restrictions.AsViolation(err); ok {
		return http.StatusUnprocessableEntity
	}
	return http.StatusUnprocessableEntity
}

func (h *restQueryHandler) writeProto(w http.ResponseWriter, principal *models.Principal, msg proto.Message) {
	out, err := protojson.Marshal(msg)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, principal,
			fmt.Errorf("marshal response: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(out); err != nil {
		h.logger.WithField("action", "rest_query_write_response").Warnf("write response: %v", err)
	}
}

func (h *restQueryHandler) writeError(w http.ResponseWriter, status int, principal *models.Principal, err error) {
	payload := errPayloadFromSingleErr(principal, err)
	out, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, writeErr := w.Write(out); writeErr != nil {
		h.logger.WithField("action", "rest_query_write_error").Warnf("write error response: %v", writeErr)
	}
}
