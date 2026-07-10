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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
			return nil, fmt.Errorf("could not find collection %s in schema", name)
		}
		return class, nil
	}
}

// Substrings of upstream error messages that statusFromError matches, kept
// as named constants (single source of truth) because these upstream errors
// are plain fmt.Errorf strings with no typed sentinel to match on. A wording
// change upstream must fail a statusFromError test (see handler_test.go),
// not silently mis-map in production. TODO: promote these to typed
// sentinels in their core packages so the string matching can be dropped
// (tracked separately).
const (
	// usecases/modules/modules.go VectorFromSearchParam — near-text on a
	// collection whose (target) vector has no vectorizer module.
	errNoVectorizerMarker = "Make sure a vectorizer module is configured"
	// usecases/traverser/explorer.go + near_params_vector.go — the embedding
	// provider failed to produce a query vector.
	errVectorizeParamsMarker = "vectorize params"
	errVectorizeSearchMarker = "vectorize search vector"
	// entities/schema/configvalidation/config_validation.go — certainty
	// requested on a non-cosine vector index.
	errCertaintyMarker = "can't compute and return certainty"
	// adapters/repos/db/multitenancy/validator.go — tenant usage does not
	// match the collection's multi-tenancy configuration.
	errMTDisabledWithTenantMarker   = "multi-tenancy disabled, but request was with tenant"
	errMTEnabledWithoutTenantMarker = "multi-tenancy enabled, but request was without tenant"
	// Collection not present in the schema. Two markers because the wording
	// differs by source: this handler's classGetterWithAuthz says
	// "collection" (our user-facing rename), while the upstream traverser
	// still says "class" — both must map to 404.
	errCollectionNotFoundMarker = "could not find collection"
	errClassNotFoundMarker      = "could not find class"
	// entities/errors tenant sentinels' messages. Kept as a fallback to the
	// typed errors.Is checks below because the search path wraps tenant
	// errors through objects.NewErrMultiTenancy and full sentinel coverage
	// on the reaching error is not verified without a live run; the live
	// smoke exercises the unknown-tenant path. Follow-up: confirm sentinel
	// coverage end-to-end, then drop these fallbacks.
	errTenantNotFoundMarker  = "tenant not found"
	errTenantNotActiveMarker = "tenant not active"
)

// statusFromError maps errors surfaced by authorization, schema access and
// the traverser onto the REST search status codes. Typed/sentinel errors are
// matched with errors.As/errors.Is; the remaining cases match the named
// substring constants above (their upstream producers have no sentinel yet).
//
// ORDERING INVARIANT: the substring switch matches message fragments and
// several markers can co-occur in one message, so case order is load-bearing.
// In particular the no-vectorizer error from usecases/modules ("could not
// vectorize input ... Make sure a vectorizer module is configured for this
// class") arrives wrapped in the explorer's "vectorize params:" prefix — the
// errNoVectorizerMarker (422) case MUST stay above the errVectorizeParams
// (502) case, or a config problem would be misreported as a provider outage.
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

	msg := err.Error()
	switch {
	case errors.Is(err, enterrors.ErrTenantNotFound) || strings.Contains(msg, errTenantNotFoundMarker):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case errors.Is(err, enterrors.ErrTenantNotActive) || strings.Contains(msg, errTenantNotActiveMarker):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case strings.Contains(msg, errMTDisabledWithTenantMarker) ||
		strings.Contains(msg, errMTEnabledWithoutTenantMarker):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case strings.Contains(msg, errCollectionNotFoundMarker) ||
		strings.Contains(msg, errClassNotFoundMarker):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case strings.Contains(msg, errNoVectorizerMarker):
		// near-text on a collection without a vectorizer: valid request,
		// unrunnable configuration
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case strings.Contains(msg, errCertaintyMarker):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case strings.Contains(msg, errVectorizeParamsMarker) ||
		strings.Contains(msg, errVectorizeSearchMarker):
		// the embedding provider failed, the search cannot run
		return &APIError{Status: http.StatusBadGateway, Err: err}
	case strings.Contains(msg, "invalid 'where' filter"):
		// this wrap is ours (parseWhere), not upstream-fragile
		return &APIError{Status: http.StatusBadRequest, Err: err}
	default:
		return &APIError{Status: http.StatusInternalServerError, Err: err}
	}
}
