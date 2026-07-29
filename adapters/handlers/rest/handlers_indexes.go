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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	reindexusecase "github.com/weaviate/weaviate/usecases/reindex"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// The reindex status merge, cancel lifecycle, conflict/cap gate and
// validators live in [reindexusecase]; this file is the HTTP shell.
//
// If you find yourself editing status merging, conflict checks,
// migration-type maps or validation here, that belongs in the usecases
// package.

func setupIndexesHandlers(api *operations.WeaviateAPI, appState *state.State, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) {
	h := &indexesHandlers{
		appState:            appState,
		metricRequestsTotal: newIndexesRequestsTotal(metrics, logger),
	}
	api.SchemaSchemaObjectsIndexesGetHandler = schema.SchemaObjectsIndexesGetHandlerFunc(h.getIndexes)
	api.SchemaSchemaObjectsIndexUpsertHandler = schema.SchemaObjectsIndexUpsertHandlerFunc(h.upsertIndex)
	api.SchemaSchemaObjectsIndexRebuildHandler = schema.SchemaObjectsIndexRebuildHandlerFunc(h.rebuildIndex)
	api.SchemaSchemaObjectsIndexCancelHandler = schema.SchemaObjectsIndexCancelHandlerFunc(h.cancelIndex)
}

// jsonResponder writes an arbitrary status + JSON body so the shared
// upsert/rebuild/cancel submit path can return any of its outcome codes
// without per-operation generated responders.
func jsonResponder(status int, payload interface{}) middleware.Responder {
	return middleware.ResponderFunc(func(w http.ResponseWriter, producer runtime.Producer) {
		w.WriteHeader(status)
		if payload != nil {
			if err := producer.Produce(w, payload); err != nil {
				panic(err)
			}
		}
	})
}

// authzResponder maps an authz error to 403 (forbidden) or 500, shared by
// the three reindex mutation handlers.
func authzResponder(principal *models.Principal, err error) middleware.Responder {
	if errors.As(err, &authzerrors.Forbidden{}) {
		return jsonResponder(http.StatusForbidden, errPayloadFromSingleErr(principal, err))
	}
	return jsonResponder(http.StatusInternalServerError, errPayloadFromSingleErr(principal, err))
}

// normalizeIndexTypeParam maps the {indexType} path value to its internal
// token, folding both API spellings `rangeFilters` and `rangeable` to the
// internal token `rangeable`. Returns ok=false for values outside the enum
// (defense in depth; swagger already rejects those with 422).
func normalizeIndexTypeParam(pathValue string) (internalToken string, ok bool) {
	switch pathValue {
	case "filterable":
		return "filterable", true
	case "searchable":
		return "searchable", true
	case "rangeFilters", "rangeable":
		return "rangeable", true
	}
	return "", false
}

// canonicalIndexType maps the internal token to the API spelling used in
// responses: "rangeable" surfaces as "rangeFilters".
func canonicalIndexType(internalToken string) string {
	return reindexusecase.CanonicalIndexType(internalToken)
}

type indexesHandlers struct {
	appState            *state.State
	metricRequestsTotal restApiRequestsTotal
}

// indexesRequestsTotal gives the indexes endpoints their own
// `query_type` so dashboards can split reindex submit/cancel/status
// traffic from generic schema mutations.
type indexesRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newIndexesRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &indexesRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "indexes", logger},
	}
}

// logError mirrors [schemaRequestsTotal.logError]: anything authz- or
// validation-shaped is a UserError; everything else is a server error
// so the unexpected-error log fires.
func (e *indexesRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &authzerrors.Forbidden{}):
		e.logUserError(className)
	case errors.Is(err, reindexusecase.ErrBadRequest),
		errors.Is(err, reindexusecase.ErrNotFound),
		errors.Is(err, reindexusecase.ErrConflict),
		errors.Is(err, reindexusecase.ErrServiceUnavailable):
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}

// submitLock returns the per-(collection, property) mutex for the
// check-and-submit critical section, allocating one on first use.
//
// The actual lock manager lives on appState (ReindexSubmitLocks) so
// it is SHARED with the DELETE-property-index REST handler. Without
// the sharing, a parallel PUT .../index/{indexType} (which submits a
// reindex task) and DELETE .../index/{indexType}
// (which drops the canonical bucket) race at the RAFT serializer and
// produce a torn bucket — see [state.ReindexSubmitLocks] godoc for the
// full failure shape.
//
// The map is keyed by collection-lowercased + property so case-folded
// collection lookups (matching the rest of the conflict logic) hit
// the same lock entry.
func (h *indexesHandlers) submitLock(collection, propertyName string) *sync.Mutex {
	return h.appState.ReindexSubmitLocks.SubmitLockFor(collection, propertyName)
}

// getIndexes implements GET /v1/schema/{className}/indexes.
func (h *indexesHandlers) getIndexes(params schema.SchemaObjectsIndexesGetParams, principal *models.Principal) middleware.Responder {
	// Resolve (alias-aware) before authz so authz and the lookup use the qualified name.
	collection, _, rErr := namespacing.Resolve(principal, h.appState.SchemaManager,
		h.appState.ServerConfig.Config.Namespaces.Enabled, params.ClassName)
	if rErr != nil {
		h.metricRequestsTotal.logError(params.ClassName, rErr)
		return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, rErr))
	}

	// Require READ on the collection's metadata: this endpoint exposes
	// per-property index state, which is collection-internal information.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.READ, authorization.CollectionsMetadata(collection)...); err != nil {
		h.metricRequestsTotal.logError(collection, err)
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	status, err := h.appState.ReindexService.CollectionStatus(
		params.HTTPRequest.Context(),
		collection,
		h.appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval,
	)
	if err != nil {
		h.metricRequestsTotal.logError(collection, err)
		if errors.Is(err, reindexusecase.ErrNotFound) {
			return schema.NewSchemaObjectsIndexesGetNotFound()
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errorResponse(principal, err.Error()))
	}

	props := make([]*models.PropertyIndexStatus, 0, len(status.Properties))
	for _, p := range status.Properties {
		// Strip the caller's namespace so status and submit responses agree.
		for _, idx := range p.Indexes {
			if idx.TaskID != "" {
				idx.TaskID = namespacing.StripOwnNamespace(principal, idx.TaskID)
			}
		}
		props = append(props, &models.PropertyIndexStatus{
			Name: p.Name,
			// Reference DataTypes carry the qualified target class.
			DataType:    namespacing.StripOwnNamespace(principal, p.DataType),
			Description: p.Description,
			Indexes:     p.Indexes,
		})
	}

	h.metricRequestsTotal.logOk(collection)
	return schema.NewSchemaObjectsIndexesGetOK().WithPayload(&models.IndexStatusResponse{
		Collection: namespacing.StripOwnNamespace(principal, collection),
		Properties: props,
	})
}

// principalUsername extracts the user-facing identifier from a principal
// for audit logging. Falls back to "anonymous" if the principal is nil.
func principalUsername(principal *models.Principal) string {
	if principal == nil {
		return "anonymous"
	}
	return principal.Username
}

// findCancelTargetTask returns the in-flight (STARTED/PREPARING/SWAPPING)
// reindex task matching (collection, propertyName, indexType), or nil.
// A non-STARTED match becomes a 202 NO_OP, not an error.
func findCancelTargetTask(tasks []*distributedtask.Task, collection, propertyName, indexType string) (*distributedtask.Task, dbreindex.ReindexTaskPayload) {
	return reindexusecase.FindCancelTargetTask(tasks, collection, propertyName, indexType)
}

// cancelReindexTask delegates to [reindexusecase.Service.Cancel] and maps
// its outcome to HTTP.
//
// Idempotent cancel: by the time this runs the caller's (collection,
// property) tuple has already been verified to exist by [cancelIndex] —
// a missing class or property would have produced a 404 there. So when no
// in-flight task matches the cancel target, or the FSM reports the target
// as no longer running, the service answers Status: NO_OP and this maps it
// to 202 rather than 404 or 500. "Make sure no reindex is running on this
// property" is the same idempotent intent whether or not a task happened
// to be in flight at request time.
//
// On success: 202 + Status: CANCELLED with the cancelled task's ID.
func (h *indexesHandlers) cancelReindexTask(ctx context.Context, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	result, err := h.appState.ReindexService.Cancel(ctx, collection, propertyName, indexType, principalUsername(principal))
	if err != nil {
		h.metricRequestsTotal.logError(collection, err)
		if errors.Is(err, reindexusecase.ErrServiceUnavailable) {
			return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal, err.Error()))
		}
		return jsonResponder(http.StatusInternalServerError, errorResponse(principal, err.Error()))
	}

	h.metricRequestsTotal.logOk(collection)
	if result.Status == reindexusecase.StatusNoOp {
		return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
			Status: reindexusecase.StatusNoOp,
		})
	}
	return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
		// The task ID embeds the qualified collection.
		TaskID: namespacing.StripOwnNamespace(principal, result.TaskID),
		Status: result.Status,
	})
}

func errorResponse(principal *models.Principal, msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: namespacing.StripErrorMessage(principal, msg)},
		},
	}
}

// maxConcurrentReindexPerCollection caps how many in-flight reindex tasks
// can target the same collection at once; the value and its rationale live
// in [reindexusecase.MaxConcurrentReindexPerCollection].
const maxConcurrentReindexPerCollection = reindexusecase.MaxConcurrentReindexPerCollection

// checkReindexAdmission checks conflict (409, or 503 if unverifiable) and the
// per-collection cap (429) against an already-fetched task snapshot. The
// fail-closed 503 for an unreachable task store lives in [listReindexTasks].
func (h *indexesHandlers) checkReindexAdmission(principal *models.Principal, collection string,
	migrationType dbreindex.ReindexMigrationType, properties []string,
	tasks []*distributedtask.Task,
) middleware.Responder {
	reason, checkErr := reindexusecase.CheckReindexConflict(collection, migrationType, properties, tasks)
	if checkErr != nil {
		// checkErr may name a task ID in a namespace the caller can't see, and
		// StripErrorMessage only strips the caller's own namespace — so log the
		// detail server-side and return a generic 503 instead of checkErr.Error().
		h.appState.Logger.WithField("collection", collection).
			Errorf("submit: cannot verify reindex conflict, failing closed: %v", checkErr)
		return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cannot verify reindex preconditions: an in-flight reindex task has an unparseable or incomplete payload; retry after an operator inspects the task store"))
	}
	if reason != "" {
		return jsonResponder(http.StatusConflict, errorResponse(principal, reason))
	}
	if inflight := reindexusecase.CountStartedTasksForCollection(collection, tasks); inflight >= maxConcurrentReindexPerCollection {
		return reindexCapExceededResponder(principal, collection, inflight, maxConcurrentReindexPerCollection)
	}
	return nil
}

// reindexCapExceededResponder returns 429 (not 503 — this is a
// per-collection concurrency limit, not cluster unavailability),
// shared by upsert and rebuild.
func reindexCapExceededResponder(principal *models.Principal, collection string, inflight, capLimit int) middleware.Responder {
	body := errorResponse(principal, fmt.Sprintf(
		"collection %q already has %d concurrent reindex tasks (max %d); wait for one to finish before submitting another",
		collection, inflight, capLimit))
	return middleware.ResponderFunc(func(w http.ResponseWriter, producer runtime.Producer) {
		w.WriteHeader(http.StatusTooManyRequests)
		if err := producer.Produce(w, body); err != nil {
			// Match the generated swagger responders' behaviour for body
			// write failures; the recovery middleware logs and returns 500.
			panic(err)
		}
	})
}
