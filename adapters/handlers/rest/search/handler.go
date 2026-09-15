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
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	dbinverted "github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// IsSearchRoute reports whether urlPath is under the static REST search
// namespace, /v1/search/{collection}/{search-type}. Every such route is
// semantically a read; the operational-mode middleware uses this to classify
// search requests as reads even though POST is an HTTP write method.
//
// path.Clean matches the router's normalization, so trailing/doubled/dot
// slashes are classified the same way the router routes them.
func IsSearchRoute(urlPath string) bool {
	parts := strings.Split(path.Clean(urlPath), "/")
	// ["", "v1", "search", {collection}, {search-type}]
	return len(parts) == 5 && parts[0] == "" && parts[1] == "v1" &&
		parts[2] == "search" && parts[3] != "" && parts[4] != ""
}

// classSearcher is the subset of traverser.Traverser used by the handler.
type classSearcher interface {
	GetClass(ctx context.Context, principal *models.Principal,
		params dto.GetParams) ([]any, error)
	Aggregate(ctx context.Context, principal *models.Principal,
		params *aggregation.Params) (any, error)
}

// HandlerConfig wires the handler's dependencies.
type HandlerConfig struct {
	Traverser         classSearcher
	SchemaReader      schema.SchemaReader
	Authorizer        authorization.Authorizer
	NamespacesEnabled bool
	DefaultLimit      int64
	MaximumResults    int64
	// CrossRefDepthLimit is QUERY_CROSS_REFERENCE_DEPTH_LIMIT; the handler
	// rejects deeper returnReferences nesting than the traverser would.
	CrossRefDepthLimit int
	Logger             logrus.FieldLogger
}

// Handler implements the search endpoints. The caller is authenticated in
// the swagger security layer; the handler receives the resulting principal.
type Handler struct {
	traverser          classSearcher
	schemaReader       schema.SchemaReader
	authorizer         authorization.Authorizer
	namespacesEnabled  bool
	defaultLimit       int64
	maximumResults     int64
	crossRefDepthLimit int
	logger             logrus.FieldLogger
}

func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{
		traverser:          cfg.Traverser,
		schemaReader:       cfg.SchemaReader,
		authorizer:         cfg.Authorizer,
		namespacesEnabled:  cfg.NamespacesEnabled,
		defaultLimit:       cfg.DefaultLimit,
		maximumResults:     cfg.MaximumResults,
		crossRefDepthLimit: cfg.CrossRefDepthLimit,
		logger:             cfg.Logger,
	}
}

// APIError couples an error with the HTTP status it maps to. The rest
// package translates it into the matching generated responder. Err is what
// the client sees; cause, when set, is the full error kept for the log.
type APIError struct {
	Status int
	Err    error
	cause  error
}

// Cause returns the full error behind a shortened client message, or Err. It
// is for the log and for matching documented errors; only Err has been
// stripped for the caller, so only Err may be shown to a client.
func (e *APIError) Cause() error {
	if e.cause != nil {
		return e.cause
	}
	return e.Err
}

// strippedForPrincipal is apiErr with the caller's namespace removed from the
// client message. The cause is carried over unchanged: it is never shown to
// the client, but the reply still matches its docs link against it.
func strippedForPrincipal(principal *models.Principal, apiErr *APIError) *APIError {
	return &APIError{
		Status: apiErr.Status,
		Err:    namespacing.StripErrForPrincipal(principal, apiErr.Err),
		cause:  apiErr.cause,
	}
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

// resolveAuthorizedClass runs the first steps shared by search and aggregate:
// alias/namespace resolution, then authorization BEFORE any schema access, so
// a denied caller cannot learn whether the collection exists. Errors come back
// unstripped; the caller applies its namespace strip.
func (h *Handler) resolveAuthorizedClass(ctx context.Context, principal *models.Principal,
	collection, tenant string,
) (context.Context, *models.Class, string, classGetterFunc, *APIError) {
	resolved, aliasUsed, err := namespacing.Resolve(principal, h.schemaReader, h.namespacesEnabled, collection)
	if err != nil {
		return ctx, nil, "", nil, &APIError{Status: http.StatusBadRequest, Err: err}
	}

	ctx = restCtx.AddPrincipalToContext(ctx, principal)

	getClass := h.classGetterWithAuthz(ctx, principal, tenant)
	class, err := getClass(resolved)
	if err != nil {
		var forbidden autherrs.Forbidden
		if errors.As(err, &forbidden) {
			// 403 before the existence check, with the alias target hidden
			return ctx, nil, "", nil, h.hideAliasTarget(ctx, principal, collection, resolved, tenant, aliasUsed != "", err)
		}
		return ctx, nil, "", nil, statusFromError(err)
	}

	return ctx, class, resolved, getClass, nil
}

// execute is the orchestrator shared by every REST search endpoint; only
// the search-type-specific dto.GetParams construction is delegated to
// buildParams. The authz-before-schema ordering is load-bearing: a caller
// must not learn whether a collection exists before passing authorization.
func (h *Handler) execute(ctx context.Context, principal *models.Principal, op string,
	collection, tenant string, common *models.SearchCommon, buildParams buildParamsFunc,
) (*models.SearchResponse, *APIError) {
	before := time.Now()

	// error messages must never leak cross-namespace schema
	strip := func(apiErr *APIError) *APIError {
		h.logAPIError(op, collection, apiErr)
		return strippedForPrincipal(principal, apiErr)
	}

	// reserved fields are rejected before any schema access, so an
	// unauthorized caller cannot probe the collection
	if apiErr := checkReservedFields(common); apiErr != nil {
		return nil, strip(apiErr)
	}

	ctx, class, resolved, getClass, apiErr := h.resolveAuthorizedClass(ctx, principal, collection, tenant)
	if apiErr != nil {
		return nil, strip(apiErr)
	}

	params, apiErr := buildParams(class, resolved, getClass)
	if apiErr != nil {
		return nil, strip(apiErr)
	}

	res, err := h.traverser.GetClass(ctx, principal, params)
	if err != nil {
		return nil, strip(statusFromError(err))
	}

	reply, err := buildResponse(res, params, principal, time.Since(before))
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
	return h.execute(ctx, principal, "near-text", collection, body.Tenant, &body.SearchCommon, paramsBuilder)
}

// hideAliasTarget makes an alias denial indistinguishable from one on a plain
// collection of the caller-supplied name: it re-runs the authorizer on that
// name and, if that passes, rewords the target's denial onto the alias, so the
// 403 keeps the authorizer's shape and never names the target.
func (h *Handler) hideAliasTarget(ctx context.Context, principal *models.Principal,
	collection, target, tenant string, aliasUsed bool, err error,
) *APIError {
	var forbidden autherrs.Forbidden
	if !aliasUsed || !errors.As(err, &forbidden) {
		return statusFromError(err)
	}
	if reauth := h.authorizer.Authorize(ctx, principal, authorization.READ, dataResources(collection, tenant)...); reauth != nil {
		return statusFromError(reauth)
	}
	// authorized on the alias name but denied on its target: still deny,
	// wording the target's denial as one on the alias
	msg := regexp.MustCompile(`\b`+regexp.QuoteMeta(target)+`\b`).ReplaceAllLiteralString(err.Error(), collection)
	return &APIError{Status: http.StatusForbidden, Err: errors.New(msg), cause: err}
}

func (h *Handler) logAPIError(op, collection string, apiErr *APIError) {
	if h.logger == nil || apiErr == nil {
		return
	}
	entry := h.logger.WithFields(logrus.Fields{"action": "rest_search", "op": op, "collection": collection, "status": apiErr.Status})
	if apiErr.Status >= http.StatusInternalServerError {
		entry.Errorf("%s failed: %v", op, apiErr.Cause())
		return
	}
	entry.Debugf("%s rejected: %v", op, apiErr.Cause())
}

// Bm25 executes a keyword (BM25F) search over collection, supplying execute
// with the bm25 params builder. It returns the 200 payload or an APIError
// carrying the HTTP status.
func (h *Handler) Bm25(ctx context.Context, principal *models.Principal,
	collection string, body *models.SearchBm25Request,
) (*models.SearchResponse, *APIError) {
	paramsBuilder := func(class *models.Class, className string, getClass classGetterFunc) (dto.GetParams, *APIError) {
		return h.buildBm25Params(class, className, body, getClass, principal)
	}
	return h.execute(ctx, principal, "bm25", collection, body.Tenant, &body.SearchCommon, paramsBuilder)
}

// NearObject executes a similarity search over collection anchored at an
// existing object's stored vector, supplying execute with the near-object
// params builder. It returns the 200 payload or an APIError carrying the
// HTTP status.
func (h *Handler) NearObject(ctx context.Context, principal *models.Principal,
	collection string, body *models.SearchNearObjectRequest,
) (*models.SearchResponse, *APIError) {
	paramsBuilder := func(class *models.Class, className string, getClass classGetterFunc) (dto.GetParams, *APIError) {
		return h.buildNearObjectParams(class, className, body, getClass, principal)
	}
	return h.execute(ctx, principal, "near-object", collection, body.Tenant, &body.SearchCommon, paramsBuilder)
}

// Hybrid executes a hybrid (keyword + vector) search over collection,
// supplying execute with the hybrid params builder. It returns the 200
// payload or an APIError carrying the HTTP status.
func (h *Handler) Hybrid(ctx context.Context, principal *models.Principal,
	collection string, body *models.SearchHybridRequest,
) (*models.SearchResponse, *APIError) {
	paramsBuilder := func(class *models.Class, className string, getClass classGetterFunc) (dto.GetParams, *APIError) {
		return h.buildHybridParams(class, className, body, getClass, principal)
	}
	return h.execute(ctx, principal, "hybrid", collection, body.Tenant, &body.SearchCommon, paramsBuilder)
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
// ORDERING: ErrNoVectorizerModule (422), ErrSourceObjectNotFound (400),
// ErrSourceObjectNoVector (422) and ErrDirtyReadOfDeletedObject (400) must
// precede ErrQueryVectorization (500) — each arrives wrapped inside the
// latter.
func statusFromError(err error) *APIError {
	var forbidden autherrs.Forbidden
	if errors.As(err, &forbidden) {
		return &APIError{Status: http.StatusForbidden, Err: err}
	}
	var rateLimit enterrors.ErrRateLimit
	if errors.As(err, &rateLimit) {
		return &APIError{Status: http.StatusTooManyRequests, Err: err}
	}

	// typed errors answer with their own message: the wrap chain above them
	// names traverser stages and shard ids the caller cannot act on
	var (
		multiTenancy  objects.ErrMultiTenancy
		noVectorizer  enterrors.ErrNoVectorizerModule
		srcNotFound   enterrors.ErrSourceObjectNotFound
		srcNoVector   enterrors.ErrSourceObjectNoVector
		dirtyRead     objects.ErrDirtyReadOfDeletedObject
		certainty     enterrors.ErrCertaintyIncompatible
		missingIndex  inverted.MissingIndexError
		vectorization enterrors.ErrQueryVectorization
	)
	shortened := func(status int, e error) *APIError { return &APIError{Status: status, Err: e, cause: err} }
	shortenedChain := func(status int) *APIError {
		return &APIError{Status: status, Err: errors.New(stripEngineWrap(err.Error())), cause: err}
	}
	switch {
	case errors.Is(err, enterrors.ErrTenantNotFound):
		return shortenedChain(http.StatusNotFound)
	case errors.Is(err, enterrors.ErrTenantNotActive):
		return shortenedChain(http.StatusUnprocessableEntity)
	case errors.As(err, &multiTenancy):
		// tenant-vs-collection mismatch (tenant sentinels checked above)
		return shortened(http.StatusUnprocessableEntity, multiTenancy)
	case errors.Is(err, errCollectionNotFound):
		return &APIError{Status: http.StatusNotFound, Err: err}
	case errors.As(err, &noVectorizer):
		// must stay above ErrQueryVectorization (see func doc)
		return shortened(http.StatusUnprocessableEntity, noVectorizer)
	case errors.As(err, &srcNotFound):
		// near-object: the id names no object — a bad body value, like an
		// unknown targetVector (must stay above ErrQueryVectorization)
		return shortened(http.StatusBadRequest, srcNotFound)
	case errors.As(err, &srcNoVector):
		// near-object: the object exists but its stored vectors cannot
		// anchor this search (must stay above ErrQueryVectorization)
		return shortened(http.StatusUnprocessableEntity, srcNoVector)
	case errors.As(err, &dirtyRead):
		// near-object: the source object is mid-delete across replicas, which
		// every other read path treats as gone (usecases/objects head, merge)
		return shortened(http.StatusBadRequest, dirtyRead)
	case errors.As(err, &certainty):
		return shortened(http.StatusUnprocessableEntity, certainty)
	case errors.As(err, &missingIndex):
		// filter on a property whose inverted index is disabled
		return shortened(http.StatusUnprocessableEntity, missingIndex)
	case errors.Is(err, dbinverted.ErrOnlyStopwords):
		// a Like pattern or keyword query that tokenizes to nothing
		return shortened(http.StatusBadRequest, dbinverted.ErrOnlyStopwords)
	case errors.As(err, &vectorization):
		// embedding provider failure — 500, not 502: Weaviate is not acting as
		// a gateway. The provider's response can quote credentials, so it goes
		// to the log, not to the client.
		return &APIError{Status: http.StatusInternalServerError, Err: errVectorizationFailed, cause: err}
	}

	msg := err.Error()
	_, documented := enterrors.Documented(err)
	switch {
	case strings.Contains(msg, errClassNotFoundMarker):
		return shortenedChain(http.StatusNotFound)
	case strings.Contains(msg, "invalid 'where' filter"):
		// this wrap is ours (parseWhere), not upstream-fragile
		return &APIError{Status: http.StatusBadRequest, Err: err}
	case documented:
		// a documented failure explains itself and the reply appends its page,
		// so the caller keeps that message instead of the generic one
		return shortenedChain(http.StatusInternalServerError)
	default:
		return &APIError{Status: http.StatusInternalServerError, Err: errInternal, cause: err}
	}
}

// Client-facing messages for failures whose detail belongs in the log.
var (
	errVectorizationFailed = errors.New("vectorizing the query failed; the vectorizer module's response is in the server log")
	errInternal            = errors.New("internal server error; details are in the server log")
)

// engineWrapSegment matches the wrap segments the traverser and db prepend
// on the way up ("explorer: get class: vector search: object search at index
// x: local shard object search x_abc: ..."), which name internals the caller
// cannot act on.
var engineWrapSegment = regexp.MustCompile(`^(explorer|get class|list class|search|vector search|hybrid|keyword search|` +
	`object search at index \S+|local shard object search \S+|concurrentTargetVectorSearch|nearObject params|` +
	`determine shard|identify groups|scan|shard \S+|aggregate|filtered aggregate|unfiltered aggregate)$`)

// stripEngineWrap drops the leading engine wrap segments of an error message.
func stripEngineWrap(msg string) string {
	segments := strings.Split(msg, ": ")
	i := 0
	for i < len(segments)-1 && engineWrapSegment.MatchString(segments[i]) {
		i++
	}
	return strings.Join(segments[i:], ": ")
}
