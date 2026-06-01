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
// the gRPC Search/Aggregate RPCs and the client-library query methods:
//
//	POST /v1/{collection}/query/{method}  → Search  (fetch|bm25|hybrid|near-vector|near-text|...)
//	POST /v1/{collection}/aggregate       → Aggregate
//
// The JSON request body is the protojson encoding of the gRPC SearchRequest /
// AggregateRequest message, and the response is the protojson encoding of the
// corresponding reply. Every /query/{method} route parses into the same
// SearchRequest and runs the same pipeline; the path only adds a per-endpoint
// assertion (validateSearchForKind) that the body carries exactly the matching
// search method. An empty body is treated as an empty request. The collection
// name is taken from the path and always overrides any value carried in the body.
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
	kindAggregate   restQueryKind = iota
	kindQueryFetch                // /query/fetch — no search method
	kindQueryBM25                 // /query/bm25
	kindQueryHybrid               // /query/hybrid
	kindQueryNearVector
	kindQueryNearText
	kindQueryNearObject
	kindQueryNearImage
	kindQueryNearAudio
	kindQueryNearVideo
	kindQueryNearDepth
	kindQueryNearThermal
	kindQueryNearImu
)

// querySubKinds maps the /v1/{collection}/query/{sub} suffix to its kind. These
// mirror the client-library query methods.
var querySubKinds = map[string]restQueryKind{
	"fetch":        kindQueryFetch,
	"bm25":         kindQueryBM25,
	"hybrid":       kindQueryHybrid,
	"near-vector":  kindQueryNearVector,
	"near-text":    kindQueryNearText,
	"near-object":  kindQueryNearObject,
	"near-image":   kindQueryNearImage,
	"near-audio":   kindQueryNearAudio,
	"near-video":   kindQueryNearVideo,
	"near-depth":   kindQueryNearDepth,
	"near-thermal": kindQueryNearThermal,
	"near-imu":     kindQueryNearImu,
}

// kindExpectedSearchField is the SearchRequest proto-JSON field a specialized
// query endpoint requires. Universal (any) and fetch (none) are handled
// separately in validateSearchForKind.
var kindExpectedSearchField = map[restQueryKind]string{
	kindQueryBM25:        "bm25Search",
	kindQueryHybrid:      "hybridSearch",
	kindQueryNearVector:  "nearVector",
	kindQueryNearText:    "nearText",
	kindQueryNearObject:  "nearObject",
	kindQueryNearImage:   "nearImage",
	kindQueryNearAudio:   "nearAudio",
	kindQueryNearVideo:   "nearVideo",
	kindQueryNearDepth:   "nearDepth",
	kindQueryNearThermal: "nearThermal",
	kindQueryNearImu:     "nearImu",
}

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

// matchRESTQueryPath returns the (url-decoded) collection and the endpoint kind
// for the REST query/aggregate routes:
//
//	/v1/{collection}/query              universal search
//	/v1/{collection}/query/{method}     fetch | bm25 | hybrid | near-vector | near-text | ...
//	/v1/{collection}/aggregate          aggregation
//
// ok is false for anything else so the caller falls through to the next handler.
// No existing REST route has these shapes, so this never shadows another endpoint.
func matchRESTQueryPath(path string) (collection string, kind restQueryKind, ok bool) {
	const prefix = "/v1/"
	if !strings.HasPrefix(path, prefix) {
		return "", 0, false
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) < 2 || parts[0] == "" {
		return "", 0, false
	}
	col := parts[0]
	if decoded, err := url.PathUnescape(col); err == nil {
		col = decoded
	}
	switch {
	case len(parts) == 2 && parts[1] == "aggregate":
		return col, kindAggregate, true
	case len(parts) == 3 && parts[1] == "query":
		if k, found := querySubKinds[parts[2]]; found {
			return col, k, true
		}
	}
	return "", 0, false
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

	// Adapt REST-friendly conveniences (`where` filter, consistency-level
	// shorthand) before the strict protojson parse.
	reqBody, where, err := preprocessQueryBody(body)
	if err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, principal, err)
		return
	}

	if kind == kindAggregate {
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
		return
	}

	// Query family: universal /query and the per-method /query/{method} routes.
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
	if err := validateSearchForKind(req, kind); err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, principal, err)
		return
	}
	reply, err := h.querier.SearchWithPrincipal(r.Context(), principal, req, where)
	if err != nil {
		h.writeError(w, httpStatusForQueryError(err), principal, err)
		return
	}
	h.writeProto(w, principal, reply)
}

var errWhereAndFilters = errors.New("set either `where` or `filters` in the body, not both")

// searchMethodsSet returns the proto-JSON names of the mutually-exclusive search
// methods present on the request.
func searchMethodsSet(req *pbv1.SearchRequest) []string {
	var set []string
	add := func(present bool, name string) {
		if present {
			set = append(set, name)
		}
	}
	add(req.HybridSearch != nil, "hybridSearch")
	add(req.Bm25Search != nil, "bm25Search")
	add(req.NearVector != nil, "nearVector")
	add(req.NearText != nil, "nearText")
	add(req.NearObject != nil, "nearObject")
	add(req.NearImage != nil, "nearImage")
	add(req.NearAudio != nil, "nearAudio")
	add(req.NearVideo != nil, "nearVideo")
	add(req.NearDepth != nil, "nearDepth")
	add(req.NearThermal != nil, "nearThermal")
	add(req.NearImu != nil, "nearImu")
	return set
}

// validateSearchForKind enforces that the request body matches the specialized
// query endpoint it was sent to. The universal /query accepts anything;
// /query/fetch must carry no search method; every other endpoint must carry
// exactly its own search method and no other. This keeps the body identical to
// the gRPC SearchRequest (an agent can still construct any of these from the
// proto) while giving each endpoint clear validation.
func validateSearchForKind(req *pbv1.SearchRequest, kind restQueryKind) error {
	set := searchMethodsSet(req)
	if kind == kindQueryFetch {
		if len(set) > 0 {
			return fmt.Errorf("this endpoint takes no search method, but %q was set; use the matching /query/<method> endpoint", set[0])
		}
		return nil
	}
	want := kindExpectedSearchField[kind]
	switch {
	case len(set) == 1 && set[0] == want:
		return nil
	case len(set) == 0:
		return fmt.Errorf("this endpoint requires the %q search field to be set", want)
	default:
		return fmt.Errorf("this endpoint accepts only the %q search field, got %v", want, set)
	}
}

// unmarshalRequestBody decodes a protojson request body, treating an empty or
// whitespace-only body as an empty (zero-value) request.
func unmarshalRequestBody(body []byte, msg proto.Message) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	return protojson.Unmarshal(body, msg)
}

// consistencyLevelAliases maps the short REST consistency-level names to the
// canonical protobuf enum names, so callers can write the familiar
// "ONE"/"QUORUM"/"ALL" instead of "CONSISTENCY_LEVEL_ONE" etc.
var consistencyLevelAliases = map[string]string{
	"ONE":    "CONSISTENCY_LEVEL_ONE",
	"QUORUM": "CONSISTENCY_LEVEL_QUORUM",
	"ALL":    "CONSISTENCY_LEVEL_ALL",
}

// preprocessQueryBody adapts a couple of REST-friendly conveniences in the JSON
// body into the canonical protobuf JSON before the strict protojson parse:
//
//   - a top-level `where` filter (REST WhereFilter syntax) is peeled off and
//     returned separately. It is not part of the protobuf request; the pipeline
//     resolves it server-side via filterext.Parse and it overrides the protobuf
//     `filters` field.
//   - a `consistencyLevel` given in the short form ("ONE"/"QUORUM"/"ALL",
//     case-insensitive) is rewritten to the protobuf enum name so protojson can
//     decode it.
//
// It returns the (possibly rewritten) body to hand to protojson and the parsed
// WhereFilter (nil if absent). A non-object body is returned untouched so the
// protojson parse surfaces the error.
func preprocessQueryBody(body []byte) (rest []byte, where *models.WhereFilter, err error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return body, nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return body, nil, nil
	}

	changed := false

	if raw, ok := top["where"]; ok {
		var wf models.WhereFilter
		if err := json.Unmarshal(raw, &wf); err != nil {
			return nil, nil, fmt.Errorf("parse `where` filter: %w", err)
		}
		where = &wf
		delete(top, "where")
		changed = true
	}

	if raw, ok := top["consistencyLevel"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			if full, ok := consistencyLevelAliases[strings.ToUpper(s)]; ok && full != s {
				if enc, err := json.Marshal(full); err == nil {
					top["consistencyLevel"] = enc
					changed = true
				}
			}
		}
	}

	if !changed {
		return body, where, nil
	}
	rest, err = json.Marshal(top)
	if err != nil {
		return nil, nil, err
	}
	return rest, where, nil
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
