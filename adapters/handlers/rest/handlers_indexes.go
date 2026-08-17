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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func setupIndexesHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := &indexesHandlers{appState: appState}
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
	if internalToken == "rangeable" {
		return models.IndexStatusTypeRangeFilters
	}
	return internalToken
}

type indexesHandlers struct {
	appState *state.State
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

// localTaskLister reads this node's own FSM task list (*cluster.Service
// satisfies it). Deliberately narrower than [distributedtask.TaskLister]:
// omitting the leader-routed ListDistributedTasks makes reaching for it here
// a compile error, not a typo.
type localTaskLister interface {
	LocalDistributedTasks() map[string][]*distributedtask.Task
}

// classReader reads a class from this node's schema. *schema.Manager satisfies it.
type classReader interface {
	ReadOnlyClass(name string) *models.Class
}

// indexStatusOperands reads the task list and the class from this node,
// tasks first, so the class can never be the older of the two operands being
// compared. Nil class: collection doesn't exist. Nil lister: no cluster
// service, response is schema-only.
func indexStatusOperands(collection string, tasks localTaskLister, schemaReader classReader) (*models.Class, []parsedReindexTask) {
	var byNamespace map[string][]*distributedtask.Task
	if tasks != nil {
		byNamespace = tasks.LocalDistributedTasks()
	}
	class := schemaReader.ReadOnlyClass(collection)
	if class == nil {
		return nil, nil
	}
	return class, parseReindexTasks(byNamespace[db.ReindexNamespace])
}

// getIndexes implements GET /v1/schema/{className}/indexes.
func (h *indexesHandlers) getIndexes(params schema.SchemaObjectsIndexesGetParams, principal *models.Principal) middleware.Responder {
	// Resolve (alias-aware) before authz so authz and the lookup use the qualified name.
	collection, _, rErr := namespacing.Resolve(principal, h.appState.SchemaManager,
		h.appState.ServerConfig.Config.Namespaces.Enabled, params.ClassName)
	if rErr != nil {
		return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, rErr))
	}

	// Require READ on the collection's metadata: this endpoint exposes
	// per-property index state, which is collection-internal information.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.READ, authorization.CollectionsMetadata(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	// ClusterService is a concrete pointer: assigning a nil one straight into
	// the interface produces a non-nil interface holding a nil pointer, and
	// the first method call panics.
	var tasks localTaskLister
	if h.appState.ClusterService != nil {
		tasks = h.appState.ClusterService
	}
	class, parsedTasks := indexStatusOperands(collection, tasks, h.appState.SchemaManager)
	if class == nil {
		return schema.NewSchemaObjectsIndexesGetNotFound()
	}

	// Precompute once so per-property resolution below is O(1); stamp/class-flag
	// fast paths still take precedence in SearchablePropertyIsBlockmaxParsed.
	finishedBlockmaxProps := make(map[string]struct{})
	for _, pt := range parsedTasks {
		if pt.task.Status != distributedtask.TaskStatusFinished {
			continue
		}
		if !strings.EqualFold(pt.payload.Collection, collection) {
			continue
		}
		if _, _, producesBlockmax, _ := db.ReindexBucketEffect(pt.payload.MigrationType); !producesBlockmax {
			continue
		}
		for _, p := range pt.payload.Properties {
			finishedBlockmaxProps[p] = struct{}{}
		}
	}

	// Build per-property index status.
	props := make([]*models.PropertyIndexStatus, 0, len(class.Properties))
	for _, prop := range class.Properties {
		pis := &models.PropertyIndexStatus{
			Name: prop.Name,
			// Reference DataTypes carry the qualified target class.
			DataType: namespacing.StripOwnNamespace(principal, dataTypeString(prop)),
		}
		pis.Description = prop.Description

		// One entry per applicable index type. carryTokenization mirrors
		// the historical behavior: filterable and searchable expose the
		// property's tokenization on the flag-on entry; rangeable does not.
		// Rangeable only applies to numeric/date properties.
		isNumeric := isNumericProperty(prop)
		entries := []struct {
			indexType         string
			flagOn            bool
			applicable        bool
			carryTokenization bool
		}{
			{"filterable", prop.IndexFilterable == nil || *prop.IndexFilterable, true, true},
			{"searchable", prop.IndexSearchable == nil || *prop.IndexSearchable, true, true},
			{"rangeable", prop.IndexRangeFilters != nil && *prop.IndexRangeFilters, isNumeric, false},
		}

		var indexes []*models.IndexStatus
		for _, e := range entries {
			if !e.applicable {
				continue
			}
			idx := &models.IndexStatus{Type: canonicalIndexType(e.indexType), Status: "ready"}
			if e.flagOn && e.carryTokenization {
				idx.Tokenization = prop.Tokenization
			}
			// Only searchable indexes have a BM25 algorithm; surface the
			// property's TRUE wand/blockmax state (not just the class-wide
			// flag, which flips only once every searchable property has
			// migrated). Filterable / rangeable have no equivalent today.
			if e.indexType == "searchable" && e.flagOn {
				idx.Algorithm = models.IndexStatusAlgorithmWand
				if db.SearchablePropertyIsBlockmaxParsed(class, prop.Name, finishedBlockmaxProps) {
					idx.Algorithm = models.IndexStatusAlgorithmBlockmax
				}
			}
			mergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, h.appState.Logger)
			// Strip the caller's namespace so status and submit responses agree.
			if idx.TaskID != "" {
				idx.TaskID = namespacing.StripOwnNamespace(principal, idx.TaskID)
			}
			// Flag on → always emit. Flag off → emit only when a reindex
			// task carries actionable signal (in-flight or terminal
			// failure/cancellation).
			if e.flagOn || isSyntheticStatus(idx.Status) {
				indexes = append(indexes, idx)
			}
		}

		pis.Indexes = indexes
		props = append(props, pis)
	}

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

// reindexTaskCanceller is the slice of the cluster service the cancel path
// needs. *cluster.Service satisfies it (both methods hang off the embedded
// *Raft).
type reindexTaskCanceller interface {
	distributedtask.TaskLister
	CancelDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// findCancelTarget returns the in-flight reindex task for (collection,
// propertyName, indexType), or nil when none matches. A cancellable match
// wins, so several matches cannot cost the operator a cancel.
func findCancelTarget(tasks []*distributedtask.Task, collection, propertyName, indexType string, logger logrus.FieldLogger) (*distributedtask.Task, db.ReindexTaskPayload) {
	var (
		refusable        *distributedtask.Task
		refusablePayload db.ReindexTaskPayload
	)
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			// Undecodable in-flight task: it may be the very one the
			// operator is trying to cancel, and they get a NO_OP instead.
			logger.WithField("task_id", task.ID).
				Warnf("cancel: skipping in-flight reindex task with an undecodable payload: %v", err)
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}
		// Empty Properties means "all properties" for every blocking guard;
		// disagreeing here would leave the operator with no cancel target.
		if !db.ReindexPropsOverlap(payload.Properties, []string{propertyName}) {
			continue
		}
		if matches, _ := migrationTypeTargetsIndex(payload.MigrationType, indexType); !matches {
			continue
		}
		if task.Status.IsCancellable() {
			return task, payload
		}
		if refusable == nil {
			refusable, refusablePayload = task, payload
		}
	}
	return refusable, refusablePayload
}

// cancelPreflight answers a cancel that owes no RAFT apply: there is
// nothing to cancel, or DTM would refuse it. It reads the predicate the
// FSM guard applies, so the two answers cannot drift.
func (h *indexesHandlers) cancelPreflight(target *distributedtask.Task, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	switch {
	case target == nil:
		return h.cancelNoOpResponder(collection, propertyName, indexType, principal)
	case !target.Status.IsCancellable():
		return h.cancelRefusedResponder(target, collection, propertyName, indexType, principal)
	}
	return nil
}

// cancelApplyFailureResponder maps an FSM rejection to the pre-flight's
// status code for the same condition — status can race between read and
// apply, and a bare 500 would leak the sentinel's internal marker into the
// response body.
func (h *indexesHandlers) cancelApplyFailureResponder(err error, target *distributedtask.Task, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	switch {
	case errors.Is(err, distributedtask.ErrTaskNotRunning):
		return h.cancelRacedResponder(target, collection, propertyName, indexType, principal)
	case errors.Is(err, distributedtask.ErrTaskDoesNotExist):
		return h.cancelNoOpResponder(collection, propertyName, indexType, principal)
	}
	return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
		fmt.Sprintf("cancelling task: %v", err)))
}

// cancelNoOpResponder answers a cancel that has nothing to cancel.
func (h *indexesHandlers) cancelNoOpResponder(collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event": "reindex_task_cancel_noop",
		"collection":  collection,
		"property":    propertyName,
		"index_type":  indexType,
		"principal":   principalUsername(principal),
	}).Info("cancel: no in-flight task to cancel; returning NO_OP")
	return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
		Status: reindexCancelStatusNoOp,
	})
}

// cancelRefusedResponder answers a cancel DTM will not accept. "Wait for
// a terminal state" is honest advice in a coordination phase and a dead
// end for a status only other nodes can terminate, hence two bodies.
func (h *indexesHandlers) cancelRefusedResponder(target *distributedtask.Task, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event": "reindex_task_cancel_refused",
		"collection":  collection,
		"property":    propertyName,
		"index_type":  indexType,
		"taskID":      target.ID,
		"status":      target.Status.String(),
		"principal":   principalUsername(principal),
	}).Info("cancel: task is past the point where cancelling is safe; refusing")
	return jsonResponder(http.StatusConflict, errorResponse(principal,
		fmt.Sprintf("reindex task %q on %s.%s is in status %s: %s",
			target.ID, collection, propertyName, target.Status,
			cancelRefusalReason(target.Status))))
}

// cancelRacedResponder answers a cancel whose target stopped accepting
// one between the list read and the apply. The status held here is stale
// by construction, so rendering it would name a phase the task has left.
// This body names no status and sends the operator back to the read.
func (h *indexesHandlers) cancelRacedResponder(target *distributedtask.Task, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_cancel_raced",
		"collection":     collection,
		"property":       propertyName,
		"index_type":     indexType,
		"taskID":         target.ID,
		"status_at_read": target.Status.String(),
		"principal":      principalUsername(principal),
	}).Info("cancel: task left the cancellable state between the read and the apply; refusing")
	return jsonResponder(http.StatusConflict, errorResponse(principal,
		fmt.Sprintf("reindex task %q on %s.%s changed status between this request's task read and "+
			"the cancel, and is no longer cancellable. Nothing was cancelled. It has either entered "+
			"a cluster-wide coordination phase, where nodes may already have written merged state or "+
			"renamed bucket directories, reached a status this node's build does not recognize, or "+
			"already reached a terminal state. Re-read GET /v1/schema/%s/indexes to see where it landed",
			target.ID, collection, propertyName, collection)))
}

// cancelRefusalReason explains a 409 from the cancel verb. Its
// coordination-phase wording has to hold for PREPARING too, where no node
// has swapped yet, so it cannot name the swap as under way.
func cancelRefusalReason(status distributedtask.TaskStatus) string {
	if !status.IsRecognized() {
		return "this build cannot classify that status, so it cannot tell whether stopping the " +
			"task is safe and refuses the cancel on every node — the task has to reach a terminal " +
			"state on the nodes that do recognize it"
	}
	return "nodes may already have written merged state or renamed bucket directories, so " +
		"stopping it now would leave the cluster serving migrated buckets under the " +
		"pre-migration schema — wait for it to reach a terminal state"
}

// cancelReindexTask finds the in-flight reindex task targeting
// cancelReindexTask finds the in-flight reindex task targeting
// (collection, propertyName, indexType) and asks DTM to cancel it.
//
// Idempotent cancel: by the time this runs the caller's (collection,
// property) tuple has already been verified to exist by [cancelIndex] —
// a missing class or property would have produced a 404 there. So when
// no task matches the cancel target we return 202 + Status:
// NO_OP rather than 404. That mirrors how callers think about cancel:
// "make sure no reindex is running on this property" is the same
// idempotent intent whether or not a task happened to be in flight at
// request time. The previous 404 conflated "the cancel target is
// unknown" with "there is nothing to cancel" — callers couldn't
// disambiguate without parsing the response body, and scripts that
// expect "this task is cancelled now" had to special-case 404 as a
// success.
//
// On success: 202 + Status: CANCELLED with the cancelled task's ID. The
// DTM scheduler picks up the CANCELLED state on its next tick and
// terminates the local handle; the task's ctx (the provider's per-task
// ctx via runningHandles) is then cancelled, and the worker goroutine
// returns.
//
// svc is appState.ClusterService in production; the interface lets the
// idempotent-cancel error mapping be tested against a real Manager.
func (h *indexesHandlers) cancelReindexTask(ctx context.Context, svc reindexTaskCanceller, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	tasks, err := svc.ListDistributedTasks(ctx)
	if err != nil {
		return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
			fmt.Sprintf("listing tasks: %v", err)))
	}

	target, targetPayload := findCancelTarget(
		tasks[db.ReindexNamespace], collection, propertyName, indexType, h.appState.Logger)

	if resp := h.cancelPreflight(target, collection, propertyName, indexType, principal); resp != nil {
		return resp
	}

	if err := svc.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		return h.cancelApplyFailureResponder(err, target, collection, propertyName, indexType, principal)
	}

	// Drain the local reindex goroutine BEFORE cleaning partial on-disk
	// state. Without this, the cleanup races against the worker which is
	// still writing to the __reindex / __ingest buckets — ShutdownBucket
	// would tear those buckets out from under the writer and corrupt the
	// store. CancelDistributedTask above cancels the per-task ctx, so the
	// worker should be exiting; the wait simply blocks until it does.
	//
	// Bounded wait: a stuck goroutine must not turn the cancel HTTP
	// request into an open-ended hang. The same timeout (10s) is used by
	// the DTM scheduler for analogous waits. If we time out, we still
	// return 202 — the next submit's defense-in-depth cleanup will pick
	// up the work.
	if h.appState.ReindexProvider != nil {
		h.appState.Logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Info("cancel: starting drain+cleanup for cancelled reindex task")
		drainCtx, drainCancel := context.WithTimeout(ctx, reindexCancelDrainTimeout)
		drainErr := h.appState.ReindexProvider.WaitForLocalTaskDrain(drainCtx, target.TaskDescriptor)
		drainCancel()
		if drainErr != nil {
			h.appState.Logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"index_type": indexType,
			}).Errorf("cancel: timed out waiting for local reindex goroutine to drain (%v); skipping inline cleanup — next submit will retry", drainErr)
		} else {
			h.appState.Logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"index_type": indexType,
			}).Info("cancel: drain complete, running on-disk cleanup")
			// Goroutine has drained. Wipe the sidecars and migration
			// directories for every indexType this migration touches —
			// change-tokenization spawns both a searchable and a
			// filterable strategy under one task, so cleaning only the
			// URL's indexType leaves the sibling orphaned. Errors are
			// logged; submit-time pre-cleanup will retry.
			indexTypesToClean, known := indexTypesFromMigrationType(targetPayload.MigrationType)
			if !known || len(indexTypesToClean) == 0 {
				// Unknown migration type: fall back to the indexType
				// named in the URL.
				indexTypesToClean = []string{indexType}
			}
			// One cache for the whole loop; see the submit path for why.
			sweep := h.appState.DB.NewStalePartialReindexSweep()
			cleanupFailures, cleanupDropped := sweepStaleReindexState(indexTypesToClean, func(it string) error {
				return sweep(ctx, collection, propertyName, it)
			})
			// The log fields name every strategy this migration touches, not just
			// the URL's index type; each failure line adds its own index_type.
			logCancelCleanupOutcome(h.appState.Logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"strategies": indexTypesToClean,
			}), cleanupFailures, cleanupDropped)
		}
	} else {
		h.appState.Logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Warn("cancel: appState.ReindexProvider is nil; skipping drain+cleanup")
	}

	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_cancelled",
		"taskID":         target.ID,
		"collection":     collection,
		"property":       propertyName,
		"index_type":     indexType,
		"migration_type": targetPayload.MigrationType,
		"principal":      principalUsername(principal),
	}).Info("reindex provider: cancelled task")

	return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
		// The task ID embeds the qualified collection.
		TaskID: namespacing.StripOwnNamespace(principal, target.ID),
		Status: "CANCELLED",
	})
}

// reindexCancelStatusNoOp is the IndexUpdateResponse.Status value the
// cancel handler emits when there is nothing to cancel. Lets scripts
// treat "cancel was a no-op" and "cancel cancelled an in-flight task"
// as a single success path rather than the previous "200 vs 404"
// disambiguation — see the cancelReindexTask godoc.
const reindexCancelStatusNoOp = "NO_OP"

// reindexCancelDrainTimeout caps how long the cancel handler waits for
// the local reindex goroutine to exit before falling back to "let the
// next submit clean up". 10s matches the DTM scheduler's analogous
// waits and is comfortably above the per-iteration cycle (which checks
// ctx.Err() every checkProcessingEveryNoObjects=1000 objects, with a
// processingDuration cap of 600s but a per-iteration cap that's much
// shorter in practice — empirically <1s on test corpora).
const reindexCancelDrainTimeout = 10 * time.Second

// staleSweepFailure pairs a sweep error with its index type and with what the
// sweep left behind, so a handler can name the index type as a structured
// field and pick its wording from the outcome rather than from the error text.
type staleSweepFailure struct {
	indexType string
	outcome   db.CleanupSweepOutcome
	err       error
}

func (f staleSweepFailure) Error() string {
	return fmt.Sprintf("indexType=%q: %v", f.indexType, f.err)
}

// sweepStaleReindexState runs sweep once per index type and splits the
// results by [db.ClassifyCleanupSweep]. A dropped collection is not a
// failure (nothing left to short-circuit on) but also not a completed
// cleanup, so it comes back as its own count rather than folded into either.
func sweepStaleReindexState(
	indexTypes []string, sweep func(indexType string) error,
) (failures []staleSweepFailure, dropped int) {
	for _, indexType := range indexTypes {
		outcome, failure := db.ClassifyCleanupSweep(sweep(indexType))
		switch {
		case outcome == db.CleanupSweepDropped:
			dropped++
		case failure != nil:
			failures = append(failures, staleSweepFailure{
				indexType: indexType, outcome: outcome, err: failure,
			})
		}
	}
	return failures, dropped
}

// The two handlers that sweep, passed to [db.CleanupSweepSummary] so an
// operator can tell which one ran, and used to word what the outcome means for
// the caller (see [sweepConsequence]).
const (
	sweepPhaseSubmit = "submit"
	sweepPhaseCancel = "cancel"
)

// sweepConsequence is what this caller does next about what the sweep left,
// which is all the handlers add to the shared summary. The phases differ on an
// incomplete walk: cancel is done once it has swept, so the state waits for a
// later submit, while the submit that logs this dispatches its task anyway and
// the task can resume against the state the sweep could not verify.
func sweepConsequence(phase string, outcome db.CleanupSweepOutcome) string {
	switch {
	case outcome == db.CleanupSweepFailed:
		return "a later task may short-circuit on the stale state and report a false success — " +
			"operator inspection recommended"
	case phase == sweepPhaseSubmit:
		return "this submit proceeds anyway, so the task it dispatches may resume against them"
	default:
		return "the next submit sweeps them again"
	}
}

// logStaleSweepFailures emits one operator-facing line per sweep that did not
// finish. What it left behind and how loudly to say so both come from
// [db.CleanupSweepSummary], so the handlers cannot rank the same outcome
// differently from the sweep itself.
func logStaleSweepFailures(entry *logrus.Entry, phase string, failures []staleSweepFailure) {
	for _, failure := range failures {
		msg, level := db.CleanupSweepSummary(phase, failure.outcome)
		entry.WithField("index_type", failure.indexType).
			Logf(level, "%s: %v; %s", msg, failure, sweepConsequence(phase, failure.outcome))
	}
}

// logCancelCleanupOutcome reports what the cancel handler's on-disk cleanup
// did. Only a run whose sweeps all reached every shard is a finished sweep: a
// run a collection delete suppressed swept nothing past the delete, and
// reporting it as finished would claim work that did not happen.
func logCancelCleanupOutcome(entry *logrus.Entry, failures []staleSweepFailure, dropped int) {
	if len(failures) > 0 {
		logStaleSweepFailures(entry, sweepPhaseCancel, failures)
		return
	}
	outcome := db.CleanupSweepClean
	if dropped > 0 {
		outcome = db.CleanupSweepDropped
	}
	msg, level := db.CleanupSweepSummary(sweepPhaseCancel, outcome)
	entry.Log(level, msg)
}

// indexTypesFromMigrationType returns the canonical inverted-index types
// ("filterable", "searchable", "rangeable") that a migration type targets,
// for use by submit-time pre-cleanup. Returns (nil, false) only for unknown
// migration types — every known type returns at least one indexType.
//
// Most migration types target exactly one index. change-tokenization (both
// indexes) targets TWO — it spawns one ShardReindexTaskGeneric per index
// (searchable + filterable) via createReindexTasks, and each leaves its own
// .migrations/<prefix>_<prop>/ sentinel directory on disk. Pre-submit
// cleanup must wipe BOTH dirs; cleaning only one of them was the root cause
// of the Sev 1 data-loss bug fixed alongside this change (see Journey 7 in
// change_tok_delete_journeys_test.go): a prior filterable-only retokenize
// left .migrations/filterable_retokenize_<prop>/tidied.mig on disk, the
// next change-tokenization-both submit did not clean it, and its
// FilterableRetokenize sub-task short-circuited on OnAfterLsmInit's
// IsTidied check while OnMigrationComplete still flipped the schema's
// Tokenization. Schema and on-disk state then disagreed.
//
// Callers run the sweep from db.DB.NewStalePartialReindexSweep once per
// indexType returned. Safe when no stale state exists — missing
// directories and unloaded buckets are silently skipped.
func indexTypesFromMigrationType(mt db.ReindexMigrationType) ([]string, bool) {
	switch mt {
	case db.ReindexTypeEnableSearchable, db.ReindexTypeChangeAlgorithm, db.ReindexTypeRebuildSearchable:
		return []string{"searchable"}, true
	case db.ReindexTypeEnableFilterable, db.ReindexTypeRepairFilterable:
		return []string{"filterable"}, true
	case db.ReindexTypeEnableRangeable, db.ReindexTypeRepairRangeable:
		return []string{"rangeable"}, true
	case db.ReindexTypeChangeTokenization:
		// change-tokenization-both runs ONE task per inverted index
		// (searchable + filterable). Each leaves its own per-property
		// migration dir on disk. Pre-cleanup must wipe both, otherwise a
		// stale tidied.mig from a previous single-index retokenize on the
		// same prop short-circuits the sub-task and produces a schema /
		// bucket state mismatch (Sev 1 silent data loss).
		return []string{"searchable", "filterable"}, true
	case db.ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}, true
	}
	return nil, false
}

// migrationTypeTargetsIndex returns:
//
//   - matches: true if the migration type writes to the named index bucket.
//   - isKnown: true if the migration type is one this function knows about.
//
// A new ReindexType added to the codebase without being mapped here would
// return (false, false). Callers that need to log/alert on that case can
// check the second return; cancel-path callers can ignore it because a
// (false, false) result still means "this task is not a cancel target".
func migrationTypeTargetsIndex(mt db.ReindexMigrationType, indexType string) (matches, isKnown bool) {
	switch mt {
	case db.ReindexTypeEnableSearchable, db.ReindexTypeChangeAlgorithm, db.ReindexTypeRebuildSearchable:
		return indexType == "searchable", true
	case db.ReindexTypeEnableFilterable, db.ReindexTypeRepairFilterable:
		return indexType == "filterable", true
	case db.ReindexTypeEnableRangeable, db.ReindexTypeRepairRangeable:
		return indexType == "rangeable", true
	case db.ReindexTypeChangeTokenization:
		// touches both searchable and filterable buckets
		return indexType == "searchable" || indexType == "filterable", true
	case db.ReindexTypeChangeTokenizationFilterable:
		return indexType == "filterable", true
	}
	return false, false
}

// decodeErrorPolicy controls how firstActiveReindexTask treats a task whose
// payload cannot be decoded.
type decodeErrorPolicy int

const (
	// decodeSkip ignores an undecodable task (the submit-time conflict gate
	// flags it, so the match sites treat it as "no task").
	decodeSkip decodeErrorPolicy = iota
	// decodeUndecodableIsHit counts an undecodable task as a match — the
	// unverifiable scan fails closed rather than derive a trustworthy NO_OP.
	decodeUndecodableIsHit
)

// firstActiveReindexTask returns the first active task whose decoded payload
// satisfies match. On a decode error, policy decides: decodeSkip continues,
// decodeUndecodableIsHit returns that task as a match. Factors the shared
// IsActive + unmarshal loop out of the five per-index lookup helpers.
func firstActiveReindexTask(
	tasks []*distributedtask.Task,
	policy decodeErrorPolicy,
	match func(db.ReindexTaskPayload) bool,
) (*distributedtask.Task, db.ReindexTaskPayload, bool) {
	for _, t := range tasks {
		if !t.Status.IsActive() {
			continue
		}
		var p db.ReindexTaskPayload
		if err := json.Unmarshal(t.Payload, &p); err != nil {
			if policy == decodeUndecodableIsHit {
				return t, db.ReindexTaskPayload{}, true
			}
			continue
		}
		if match(p) {
			return t, p, true
		}
	}
	return nil, db.ReindexTaskPayload{}, false
}

// parsedReindexTask pairs a distributed task with its already-unmarshalled
// reindex payload. The handler builds a slice of these once per request so
// mergeReindexStatus doesn't re-unmarshal task.Payload N times where N is the
// number of properties in the collection.
type parsedReindexTask struct {
	task    *distributedtask.Task
	payload db.ReindexTaskPayload
}

// parseReindexTasks unmarshals every reindex task's payload once. Tasks
// with unparseable payloads are skipped — those are flagged elsewhere by
// checkReindexConflict at submit time; for the read-side merge they're
// the same as no task.
//
// FINISHED tasks are kept: [indexesHandlers.getIndexes] crosses them against
// [db.ReindexBucketEffect] to build finishedBlockmaxProps, which is how a
// searchable index reports algorithm=blockmax for a property whose migration
// completed before the class-wide flag flipped. Dropping FINISHED tasks here
// silently downgrades that report to wand.
func parseReindexTasks(tasks []*distributedtask.Task) []parsedReindexTask {
	parsed := make([]parsedReindexTask, 0, len(tasks))
	for _, task := range tasks {
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		parsed = append(parsed, parsedReindexTask{task: task, payload: payload})
	}
	return parsed
}

// mergeReindexStatus checks if there's an active or recently-terminated
// reindex task that targets the given property+indexType and updates the
// IndexStatus accordingly.
//
// Status values produced (in addition to the caller-supplied default
// "ready"):
//
//   - "pending":    STARTED task, no unit progress yet.
//   - "indexing":   STARTED task with some progress, or a PREPARING /
//     SWAPPING task, whose units are done but whose
//     cross-replica barrier or per-node swap is still running.
//   - "failed":     latest matching task ended in FAILED.
//   - "cancelled":  latest matching task ended in CANCELLED.
//
// `flagOn` is the caller's view of the corresponding schema flag. No status
// branches on it — the caller owns the emit gate — but it is passed so
// FINISHED-with-the-flag-off is observable in the log.
//
// Property matching is uniform across all migration types: every branch
// requires payload.Properties to be non-empty and to contain propName.
// The REST handler always populates Properties with exactly one entry;
// rejecting an empty list consistently guards against direct cluster
// payload authoring fanning out a synthetic "indexing" entry to every
// property in the collection (a hazard that would otherwise be specific
// to the repair-* migration types if they accepted an empty list as
// "match all").
//
// db.ReindexPropsOverlap reads that same empty list as "all properties";
// the disagreement is deliberate, not drift. A guard picks the answer
// that refuses, a status report picks the answer it can substantiate.
// Reconciling this side onto the guard's rule would publish a synthetic
// "indexing" entry across every property of the collection on behalf of
// a task createReindexTasks refuses to run at all. Skipping is the
// cheaper wrong answer: the entry stays "ready" when the flag is on and
// is dropped from the response when it is off — the same two outcomes
// this endpoint already produces when it cannot read the task list, so
// "ready" here means "no evidence of a reindex", never "idle".
//
// The logger is used to flag unknown migration types: a future ReindexType
// added without updating this switch would otherwise silently report "ready"
// for an in-flight task. Passing a nil logger is allowed (test callers may
// rely on this); the entry is still skipped, just without a log line.
func mergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, flagOn bool, parsedTasks []parsedReindexTask, logger logrus.FieldLogger) {
	// Two tasks for the same (collection, prop, indexType) may coexist —
	// e.g. a freshly retried STARTED enable-filterable plus the original
	// FAILED attempt that the operator just retried (terminal tasks
	// deliberately do NOT block fresh submits; see checkReindexConflict).
	// Pick the most useful one to surface rather than first-in-map-order:
	//   STARTED > FAILED ≈ CANCELLED ≈ FINISHED   (in-flight beats terminal)
	//   newer StartedAt > older StartedAt          (within the same priority)
	// FINISHED tasks stay in the loop for the tiebreak below: a completed
	// migration must outrank an older FAILED attempt on the same property,
	// or the entry reports "failed" after the retry succeeded. FINISHED
	// itself surfaces nothing (see its case).
	var best *distributedtask.Task
	var bestPayload db.ReindexTaskPayload
	for _, pt := range parsedTasks {
		task := pt.task
		payload := pt.payload

		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		// Require a non-empty Properties list. The REST handler always
		// populates this with one entry; an empty list only happens via
		// direct cluster payload authoring and is treated as "match
		// nothing" so we never silently fan out a synthetic entry to
		// every property in the collection — which is why this line
		// disagrees with db.ReindexPropsOverlap on purpose.
		if !slices.Contains(payload.Properties, propName) {
			continue
		}

		targets, known := migrationTypeTargetsIndex(payload.MigrationType, indexType)
		if !known && logger != nil {
			// A new ReindexType was added without being mapped to a bucket,
			// which would silently report "ready" for an in-flight task. Log
			// loudly so this surfaces in CI/staging before it hits prod.
			// targets is false here too, so we fall through and leave the
			// synthetic entry alone.
			logger.WithFields(logrus.Fields{
				"migration_type": payload.MigrationType,
				"task_id":        task.ID,
				"collection":     collection,
			}).Errorf("reindex status: unknown migration type %q; index status may be stale", payload.MigrationType)
		}
		if !targets {
			continue
		}

		if best == nil || taskStatusPriority(task) > taskStatusPriority(best) ||
			(taskStatusPriority(task) == taskStatusPriority(best) && task.StartedAt.After(best.StartedAt)) {
			best = task
			bestPayload = payload
		}
	}

	if best == nil {
		return
	}

	// Decide the status first; only THEN apply per-migration-type side
	// effects (Tokenization / TargetTokenization / TargetAlgorithm). Setting
	// those fields ahead of the status decision was the source of the
	// "post-FINISHED targetAlgorithm bleed" bug: for a RepairSearchable task
	// that has FINISHED with the schema flag already flipped (UsingBlockMaxWAND
	// == true), the status switch correctly leaves the entry as the base
	// "ready", but the unconditional TargetAlgorithm assignment above had
	// already poisoned the response with an in-flight signal that no longer
	// applies. The post-rebuild contract (verified by
	// TestSingleNode_ReindexSuite/MapToBlockmax) is: once the schema flag
	// has caught up, the synthetic "targetAlgorithm" / "targetTokenization"
	// fields must be empty.
	//
	// The rule is: a side-effect field is surfaced only when the status
	// switch below changes idx.Status away from "ready" (i.e., we are
	// actually painting an in-flight or SWAPPING-window signal). When the
	// status stays "ready", we keep idx in its base state.
	surfaceSyntheticFields := false

	switch best.Status {
	case distributedtask.TaskStatusFailed:
		idx.Status = "failed"
		idx.Progress = aggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusCancelled:
		idx.Status = "cancelled"
		idx.Progress = aggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusStarted:
		progress := aggregateProgress(best)
		idx.Progress = progress
		// Any non-PENDING unit means work has started somewhere; flip the
		// pill to "indexing" without waiting for the first throttled
		// progress checkpoint (which can lag by tens of seconds on a large
		// shard while per-shard setup drains).
		if progress > 0 || anyUnitWorking(best) {
			idx.Status = "indexing"
		} else {
			idx.Status = "pending"
		}
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusPreparing, distributedtask.TaskStatusSwapping:
		// Units done; cross-replica PREP barrier or per-node swap still in
		// flight. Surface as "indexing at 100%" until FINISHED + flagOn.
		idx.Status = "indexing"
		idx.Progress = 1.0
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusFinished:
		// Nothing to surface: a FINISHED task never produces a synthetic
		// entry, so the schema flag alone decides whether one is emitted.
		// FINISHED with the flag off is normal, not a skew — an index
		// DELETEd after its migration completed leaves exactly that, and
		// the task record outlives the DELETE by
		// DefaultDistributedTasksCompletedTaskTTL (5 days).
		if !flagOn && logger != nil {
			// Two producers, and this site cannot tell them apart: (1) the
			// index was DELETEd after its migration completed — normal, and
			// visible until the task record ages out; (2) this node's task
			// list ran ahead of its schema, which reading both locally and
			// tasks-first is supposed to prevent. Debug, not error: (1) is
			// routine and would otherwise log on every poll for five days.
			logger.WithFields(logrus.Fields{
				"task_id": best.ID, "collection": collection,
				"property": propName, "index_type": indexType,
			}).Debug("reindex status: FINISHED task with the schema flag off")
		}
	}

	if !best.Status.IsRecognized() {
		// "indexing" rather than "pending" or "ready": per-unit progress
		// does not prove that no shard has started.
		idx.Status = "indexing"
		idx.Progress = aggregateProgress(best)
		surfaceSyntheticFields = true
	}

	// Only paint the in-flight side-effect fields when the switch surfaced an
	// in-flight signal. A "ready" entry (now only from a FINISHED task) is
	// already fully described by the schema-derived fields above.
	if !surfaceSyntheticFields {
		return
	}

	// Surface the driving task's ID on every task-driven entry; absent on a
	// plain "ready" entry. A coupled searchable+filterable migration is one
	// task, so both entries carry the same taskId.
	idx.TaskID = best.ID

	switch bestPayload.MigrationType {
	case db.ReindexTypeEnableSearchable:
		if bestPayload.TargetTokenization != "" {
			idx.Tokenization = bestPayload.TargetTokenization
		}
	case db.ReindexTypeChangeTokenization,
		db.ReindexTypeChangeTokenizationFilterable:
		if bestPayload.TargetTokenization != "" {
			idx.TargetTokenization = bestPayload.TargetTokenization
		}
	case db.ReindexTypeChangeAlgorithm:
		// change-algorithm migrates WAND → BlockMax. The targetAlgorithm
		// lets the UI render the in-flight switch the same way it renders
		// targetTokenization for change-tokenization.
		idx.TargetAlgorithm = models.IndexStatusTargetAlgorithmBlockmax
	case db.ReindexTypeRebuildSearchable,
		db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableFilterable, db.ReindexTypeEnableRangeable,
		db.ReindexTypeRepairRangeable:
		// No tokenization or algorithm side effects for these types.
	}
}

// taskStatusPriority ranks the most user-relevant task when several match
// the same (collection, prop, indexType): in-flight beats terminal, and
// FINISHED ranks with FAILED/CANCELLED so a completed migration still wins
// the StartedAt tiebreak over an older FAILED attempt.
func taskStatusPriority(task *distributedtask.Task) int {
	if task.Status.IsActive() {
		return 2
	}
	return 1
}

// aggregateProgress averages Unit.Progress across all units in the task.
// Returns 0 when there are no units.
func aggregateProgress(task *distributedtask.Task) float32 {
	if len(task.Units) == 0 {
		return 0
	}
	var total float32
	for _, u := range task.Units {
		total += u.Progress
	}
	return total / float32(len(task.Units))
}

// anyUnitWorking returns true if at least one unit has transitioned out
// of PENDING — i.e. some shard is actively iterating, has finished, or
// failed.
func anyUnitWorking(task *distributedtask.Task) bool {
	for _, u := range task.Units {
		if u.Status != distributedtask.UnitStatusPending {
			return true
		}
	}
	return false
}

func dataTypeString(prop *models.Property) string {
	if len(prop.DataType) > 0 {
		return prop.DataType[0]
	}
	return ""
}

func shortRandomSuffix() string {
	b := make([]byte, 2) // 4 hex chars
	if _, err := rand.Read(b); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b)
}

// isSyntheticStatus reports whether the IndexStatus.Status value was
// emitted by mergeReindexStatus (i.e. driven by a reindex task) and so
// should be surfaced even when the property's schema flag for that index
// type is off. The default "ready" remains invisible when the flag is
// off, since it carries no actionable signal.
func isSyntheticStatus(s string) bool {
	switch s {
	case models.IndexStatusStatusIndexing,
		models.IndexStatusStatusPending,
		models.IndexStatusStatusFailed,
		models.IndexStatusStatusCancelled:
		return true
	}
	return false
}

func errorResponse(principal *models.Principal, msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: namespacing.StripErrorMessage(principal, msg)},
		},
	}
}

// normalizeSearchableAlgorithm canonicalises algorithm to "wand"/"blockmax",
// accepting aliases like "block-max"/"bmw" (case-insensitive). Returns ""
// for anything else, so the dispatcher's allowlist treats a new algorithm
// as a missing case, not silent acceptance.
func normalizeSearchableAlgorithm(s string) string {
	// Strip surrounding whitespace before any other transform — a body
	// like {"algorithm":" blockmax "} should not be rejected on a stray
	// space.
	trimmed := strings.TrimSpace(s)
	lower := strings.ToLower(trimmed)
	// Strip ASCII separators that callers sometimes inject (e.g.
	// "block-max", "block_max"). Done after lowercasing so the set is
	// minimal.
	stripped := strings.ReplaceAll(strings.ReplaceAll(lower, "-", ""), "_", "")
	switch stripped {
	case "blockmax", "blockmaxwand", "bmw":
		return models.IndexStatusAlgorithmBlockmax
	case "wand":
		return models.IndexStatusAlgorithmWand
	}
	return ""
}

// maxConcurrentReindexPerCollection caps how many STARTED reindex tasks
// can target the same collection at once. Each task creates ingest +
// backup buckets on every replica; without a cap, a script that runs
// PUT .../index/{indexType} per property would fan out N tasks for an
// N-property collection and overwhelm both LSM compaction and disk.
//
// The value is sized to comfortably accommodate realistic batch property
// changes (e.g. retokenizing every text property on a ~20-property
// collection in one go) while still preventing pathological unbounded
// fan-out from a script that loops over hundreds of properties. The
// original value of 4 was too restrictive: it rejected legitimate batch
// migrations against modest-sized collections and broke the
// reindex_concurrent acceptance test which exercises 15 simultaneous
// non-conflicting submits.
const maxConcurrentReindexPerCollection = 32

// checkReindexAdmission checks conflict (409, or 503 if unverifiable) and the
// per-collection cap (429) against an already-fetched task snapshot. The
// fail-closed 503 for an unreachable task store lives in [listReindexTasks].
func (h *indexesHandlers) checkReindexAdmission(principal *models.Principal, collection string,
	migrationType db.ReindexMigrationType, properties []string,
	tasks []*distributedtask.Task,
) middleware.Responder {
	reason, checkErr := checkReindexConflict(collection, migrationType, properties, tasks)
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
	if inflight := countInFlightTasksForCollection(collection, tasks); inflight >= maxConcurrentReindexPerCollection {
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

// countInFlightTasksForCollection counts in-flight reindex tasks for a
// collection. PREPARING and SWAPPING count: they still hold tracker dirs.
func countInFlightTasksForCollection(collection string, tasks []*distributedtask.Task) int {
	n := 0
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if strings.EqualFold(payload.Collection, collection) {
			n++
		}
	}
	return n
}

// checkReindexConflict checks if a new reindex task would conflict with any
// running tasks. Returns (reason, nil) when no conflict, ("reason", nil)
// when a conflict is detected, or ("", err) when a running task has a
// payload we cannot decode — in which case we cannot prove non-conflict
// and the caller must reject the submit.
//
// A conflict is two tasks touching the same bucket type on the same property
// (empty Properties matches any). Classification lives once in db
// ([db.TypesConflictReason] over [db.ReindexBucketEffect]), shared with the
// FSM apply-time check so the two paths can't drift. Unparseable payloads
// fail closed rather than skip silently.
func checkReindexConflict(collection string, newType db.ReindexMigrationType,
	newProps []string, tasks []*distributedtask.Task,
) (string, error) {
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}

		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; cannot verify conflict; "+
					"retry after operator inspects the task: %w", task.ID, err)
		}
		// Parsed but empty (`{}`, or missing Collection/MigrationType) is the
		// same unprovable-non-conflict state as unparseable, so it's refused
		// too — likely an older binary's shape whose fields zeroed on Unmarshal.
		if payload.Collection == "" || payload.MigrationType == "" {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an empty Collection or MigrationType "+
					"(payload may have been written by an older binary); cannot verify conflict; "+
					"retry after operator inspects the task", task.ID)
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		if conflict := db.TypesConflictReason(newType, newProps, payload.MigrationType, payload.Properties); conflict != "" {
			return fmt.Sprintf("reindex task %q conflicts: %s", task.ID, conflict), nil
		}
	}
	return "", nil
}

// The conflict predicate + bucket-touch helpers + property-overlap
// helper used by the pre-flight check above all live in the db
// package now ([db.TypesConflictReason], [db.TouchesSearchable],
// [db.TouchesFilterable], [db.ReindexPropsOverlap]) — they're shared
// with the FSM-deterministic conflict check at apply time so the two
// paths can't drift on what counts as a conflict.
