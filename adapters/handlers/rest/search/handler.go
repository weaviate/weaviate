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
		params dto.GetParams) ([]any, error)
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
	MaximumResults    int64
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
	maximumResults    int64
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
		maximumResults:    cfg.MaximumResults,
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

func newAPIError(status int, format string, args ...any) *APIError {
	return &APIError{Status: status, Err: fmt.Errorf(format, args...)}
}

func (e *APIError) Error() string {
	return e.Err.Error()
}

// classGetterFunc authorizes access to a collection and returns its class,
// erroring when the caller is not authorized or the collection is unknown.
type classGetterFunc func(string) (*models.Class, error)

// buildParamsFunc turns the resolved collection into the dto.GetParams for a
// specific search type. It is called after authorization, with the
// authorized class and a getClass that authorizes (and caches) any further
// collections a filter or reference selection touches.
type buildParamsFunc func(class *models.Class, className string,
	getClass classGetterFunc) (dto.GetParams, *APIError)

// execute is the orchestrator shared by every REST search endpoint; only
// the search-type-specific dto.GetParams construction is delegated to
// buildParams. The authz-before-schema ordering is load-bearing: a caller
// must not learn whether a collection exists before passing authorization.
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
		var forbidden autherrs.Forbidden
		if resolved != collection && errors.As(err, &forbidden) {
			// deny on the name the caller sent — a 403 must not disclose
			// the alias target
			deniedPrincipal := principal
			if deniedPrincipal == nil {
				deniedPrincipal = &models.Principal{Username: "anonymous"}
			}
			err = autherrs.NewForbidden(deniedPrincipal, authorization.READ, dataResources(collection, tenant)...)
		}
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
	paramsBuilder := func(class *models.Class, className string, getClass classGetterFunc) (dto.GetParams, *APIError) {
		return h.buildNearTextParams(class, className, body, getClass, principal)
	}
	return h.execute(ctx, principal, collection, body.Tenant, &body.SearchCommon, paramsBuilder)
}

// dataResources is the authorization resource set for a collection's (or
// tenant's) data.
func dataResources(collection, tenant string) []string {
	if tenant != "" {
		return authorization.ShardsData(collection, tenant)
	}
	return authorization.CollectionsData(collection)
}

// classGetterWithAuthz returns a class getter that authorizes READ on the
// collection's (or tenant's) data before reading its schema. READ on data is
// sufficient for querying: the schema exposes nothing a data reader cannot
// already obtain.
func (h *Handler) classGetterWithAuthz(ctx context.Context, principal *models.Principal, tenant string) classGetterFunc {
	authorizedCollections := map[string]*models.Class{}

	return func(name string) (*models.Class, error) {
		classTenantName := name + "#" + tenant
		class, ok := authorizedCollections[classTenantName]
		if !ok {
			if err := h.authorizer.Authorize(ctx, principal, authorization.READ, dataResources(name, tenant)...); err != nil {
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

// errCollectionNotFound marks a collection missing from the schema.
var errCollectionNotFound = errors.New("could not find collection")

// errClassNotFoundMarker is a string fallback for the upstream "could not
// find class %s in schema" errors, which carry no sentinel yet (many
// producers; reachable when a collection is deleted mid-request).
// TODO: add an ErrClassNotFound sentinel upstream, then drop this.
const errClassNotFoundMarker = "could not find class"

// statusFromError maps traverser/authz/schema errors onto HTTP statuses via
// errors.Is/As. This relies on the wrap chain staying %w/Wrapf (never
// %v/%s), or the typed matches silently degrade to 500.
//
// ORDERING: ErrNoVectorizerModule (422) must precede ErrQueryVectorization
// (502) — the former arrives wrapped inside the latter.
func statusFromError(err error) *APIError {
	var forbidden autherrs.Forbidden
	if errors.As(err, &forbidden) {
		return &APIError{Status: http.StatusForbidden, Err: err}
	}
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
		// tenant-vs-collection mismatch (tenant sentinels checked above)
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.Is(err, errCollectionNotFound):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case errors.As(err, &enterrors.ErrNoVectorizerModule{}):
		// must stay above ErrQueryVectorization (see func doc)
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &enterrors.ErrCertaintyIncompatible{}):
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &inverted.MissingIndexError{}):
		// filter on a property whose inverted index is disabled
		return &APIError{Status: http.StatusUnprocessableEntity, Err: err}
	case errors.As(err, &enterrors.ErrQueryVectorization{}):
		// embedding provider failure
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
