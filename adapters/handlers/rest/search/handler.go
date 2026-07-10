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

package search

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// IsSearchRoute reports whether path is under the static REST search
// namespace, /v1/search/{collection}/{search-type}. Every such route is
// semantically a read; the operational-mode middleware uses this to classify
// search requests as reads even though POST is an HTTP write method.
func IsSearchRoute(path string) bool {
	parts := strings.Split(path, "/")
	// ["", "v1", "search", {collection}, {search-type}]
	return len(parts) == 5 && parts[0] == "" && parts[1] == "v1" &&
		parts[2] == "search" && parts[3] != "" && parts[4] != ""
}

// classSearcher is the subset of traverser.Traverser used by the handler.
type classSearcher interface {
	GetClass(ctx context.Context, principal *models.Principal,
		params dto.GetParams) ([]interface{}, error)
}

// schemaReader is the subset of schema.Manager used by the handler.
type schemaReader interface {
	ReadOnlyClass(name string) *models.Class
	ResolveAlias(alias string) string
}

// HandlerConfig wires the handler's dependencies.
type HandlerConfig struct {
	Traverser         classSearcher
	SchemaReader      schemaReader
	Authorizer        authorization.Authorizer
	NamespacesEnabled bool
	DefaultLimit      int64
	Disabled          *runtime.DynamicValue[bool]
	Logger            logrus.FieldLogger
}

// Handler implements the search endpoints. The caller is authenticated in
// the swagger security layer; the handler receives the resulting principal.
type Handler struct {
	traverser         classSearcher
	schemaReader      schemaReader
	authorizer        authorization.Authorizer
	namespacesEnabled bool
	defaultLimit      int64
	disabled          *runtime.DynamicValue[bool]
	logger            logrus.FieldLogger
}

func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{
		traverser:         cfg.Traverser,
		schemaReader:      cfg.SchemaReader,
		authorizer:        cfg.Authorizer,
		namespacesEnabled: cfg.NamespacesEnabled,
		defaultLimit:      cfg.DefaultLimit,
		disabled:          cfg.Disabled,
		logger:            cfg.Logger,
	}
}

// APIError couples an error with the HTTP status it maps to. The rest
// package translates it into the matching generated responder.
type APIError struct {
	Status int
	Err    error
}

func newAPIError(status int, format string, args ...interface{}) *APIError {
	return &APIError{Status: status, Err: fmt.Errorf(format, args...)}
}

func (e *APIError) Error() string {
	return e.Err.Error()
}

// buildParamsFunc turns the resolved collection into the dto.GetParams for a
// specific search type. It is called after authorization, with the
// authorized class and a getClass that authorizes (and caches) any further
// collections a filter or reference selection touches.
type buildParamsFunc func(class *models.Class, className string,
	getClass func(string) (*models.Class, error)) (dto.GetParams, *APIError)

// execute is the search-type-agnostic orchestrator shared by every REST
// search endpoint. It carries the fixed request flow — disabled check,
// reserved-field rejection before any schema access, namespace/alias
// resolution, authorization before schema access, params build, traverser
// execution, reply build — and delegates only the search-type-specific
// dto.GetParams construction to buildParams. The reserved-before-authz and
// authz-before-schema ordering is load-bearing (a caller must not learn
// whether a collection exists before passing authorization).
func (h *Handler) execute(ctx context.Context, principal *models.Principal,
	collection, tenant string, common *models.SearchCommon, buildParams buildParamsFunc,
) (*models.SearchResponse, *APIError) {
	before := time.Now()

	// error messages must never leak cross-namespace schema
	strip := func(apiErr *APIError) *APIError {
		return &APIError{Status: apiErr.Status, Err: namespacing.StripErrForPrincipal(principal, apiErr.Err)}
	}

	if h.disabled.Get() {
		return nil, newAPIError(http.StatusUnprocessableEntity, "rest search api is disabled")
	}

	// reserved fields are rejected before any schema access, so an
	// unauthorized caller cannot probe the collection
	if apiErr := checkReservedFields(common); apiErr != nil {
		return nil, strip(apiErr)
	}

	resolved, _, err := namespacing.Resolve(principal, h.schemaReader, h.namespacesEnabled, collection)
	if err != nil {
		return nil, strip(&APIError{Status: http.StatusBadRequest, Err: err})
	}

	ctx = restCtx.AddPrincipalToContext(ctx, principal)

	getClass := h.classGetterWithAuthz(ctx, principal, tenant)
	class, err := getClass(resolved)
	if err != nil {
		return nil, strip(statusFromError(err))
	}

	params, apiErr := buildParams(class, resolved, getClass)
	if apiErr != nil {
		return nil, strip(apiErr)
	}

	res, err := h.traverser.GetClass(ctx, principal, params)
	if err != nil {
		return nil, strip(statusFromError(err))
	}

	reply, err := buildResponse(res, params, time.Since(before))
	if err != nil {
		return nil, strip(&APIError{Status: http.StatusInternalServerError, Err: err})
	}

	return reply, nil
}

// NearText executes a near-text search over collection, supplying execute
// with the near-text params builder. It returns the 200 payload or an
// APIError carrying the HTTP status.
func (h *Handler) NearText(ctx context.Context, principal *models.Principal,
	collection string, body *models.SearchNearTextRequest,
) (*models.SearchResponse, *APIError) {
	if body == nil {
		// defensive: swagger's required-body validation normally catches this
		return nil, newAPIError(http.StatusBadRequest, "request body is required")
	}

	return h.execute(ctx, principal, collection, body.Tenant, &body.SearchCommon,
		func(class *models.Class, className string,
			getClass func(string) (*models.Class, error),
		) (dto.GetParams, *APIError) {
			return h.buildNearTextParams(class, className, body, getClass, principal)
		})
}

// classGetterWithAuthz returns a class getter that authorizes READ on the
// collection's (or tenant's) data before reading its schema. READ on data is
// sufficient for querying: the schema exposes nothing a data reader cannot
// already obtain.
func (h *Handler) classGetterWithAuthz(ctx context.Context, principal *models.Principal, tenant string) func(string) (*models.Class, error) {
	authorizedCollections := map[string]*models.Class{}

	return func(name string) (*models.Class, error) {
		classTenantName := name + "#" + tenant
		class, ok := authorizedCollections[classTenantName]
		if !ok {
			resources := authorization.CollectionsData(name)
			if tenant != "" {
				resources = authorization.ShardsData(name, tenant)
			}
			if err := h.authorizer.Authorize(ctx, principal, authorization.READ, resources...); err != nil {
				return nil, err
			}
			class = h.schemaReader.ReadOnlyClass(name)
			authorizedCollections[classTenantName] = class
		}
		if class == nil {
			return nil, fmt.Errorf("%w %s in schema", errCollectionNotFound, name)
		}
		return class, nil
	}
}

// errCollectionNotFound marks a collection missing from the schema. It is
// produced by classGetterWithAuthz via %w, so the message stays "could not
// find collection <name> in schema" while statusFromError matches by
// sentinel instead of substring.
var errCollectionNotFound = errors.New("could not find collection")

// errClassNotFoundMarker is the one remaining substring fallback: "could not
// find class %s in schema" has many producers across the db layer (reachable
// here e.g. when a collection is deleted mid-request) and none carries a
// sentinel yet. Unlike the typed cases, a wording change upstream would
// silently degrade this mapping to 500 — acceptable for a race-only path.
// TODO: add an ErrClassNotFound sentinel to entities/errors, attach it at
// the upstream producers, then drop this marker (tracked separately).
const errClassNotFoundMarker = "could not find class"

// statusFromError maps errors surfaced by authorization, schema access and
// the traverser onto the REST search status codes. All expected cases are
// matched by type (errors.Is/errors.As); the producers attach the typed
// errors from entities/errors and the wrap chain preserves them (see
// usecases/traverser/explorer.go, near_params_vector.go — wraps there must
// stay %w/Wrapf, never %v/%s, or these matches silently degrade to 500).
//
// ORDERING INVARIANT: a missing-vectorizer configuration error surfaces
// through the vectorization call path wrapped inside an
// ErrQueryVectorization, so the ErrNoVectorizerModule case (422, config
// problem) MUST stay above the ErrQueryVectorization case (502, provider
// outage).
func statusFromError(err error) *APIError {
	var forbidden autherrs.Forbidden
	if errors.As(err, &forbidden) {
		return &APIError{Status: http.StatusForbidden, Err: err}
	}
	// typed rate-limit error (entities/errors.NewErrRateLimit) — matched by
	// type so its "429 Too many requests" wording is irrelevant here.
	var rateLimit enterrors.ErrRateLimit
	if errors.As(err, &rateLimit) {
		return &APIError{Status: http.StatusTooManyRequests, Err: err}
	}

	switch {
	case errors.Is(err, enterrors.ErrTenantNotFound):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case errors.Is(err, enterrors.ErrTenantNotActive):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &objects.ErrMultiTenancy{}):
		// remaining multi-tenancy validation failures (the tenant sentinels
		// above are checked first): tenant on a non-MT collection, or a
		// missing tenant on an MT collection
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.Is(err, errCollectionNotFound):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case errors.As(err, &enterrors.ErrNoVectorizerModule{}):
		// near-text on a collection without a vectorizer: valid request,
		// unrunnable configuration. MUST stay above ErrQueryVectorization.
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &enterrors.ErrCertaintyIncompatible{}):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &inverted.MissingIndexError{}):
		// a filter targets a property whose inverted index is disabled:
		// valid request, unrunnable collection configuration
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &enterrors.ErrQueryVectorization{}):
		// the embedding provider failed, the search cannot run
		return &APIError{Status: http.StatusBadGateway, Err: err}
	}

	msg := err.Error()
	switch {
	case strings.Contains(msg, errClassNotFoundMarker):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case strings.Contains(msg, "invalid 'where' filter"):
		// this wrap is ours (parseWhere), not upstream-fragile
		return &APIError{Status: http.StatusBadRequest, Err: err}
	default:
		return &APIError{Status: http.StatusInternalServerError, Err: err}
	}
}
