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
// token, resolving the `rangeable` alias to `rangeFilters`. Returns ok=false
// for values outside the enum (defense in depth; swagger already rejects
// those with 422).
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

	class := h.appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return schema.NewSchemaObjectsIndexesGetNotFound()
	}

	// Fetch active reindex tasks.
	var activeTasks map[string][]*distributedtask.Task
	if h.appState.ClusterService != nil {
		var err error
		activeTasks, err = h.appState.ClusterService.ListDistributedTasks(context.Background())
		if err != nil {
			activeTasks = nil // degrade gracefully
		}
	}

	// Pre-parse the reindex task payloads once per request so the per-property
	// merge below doesn't re-unmarshal each task N times.
	parsedTasks := parseReindexTasks(activeTasks[db.ReindexNamespace])

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

	// finalizeWindow bounds the "FINISHED but flag-off → indexing@100%"
	// override in mergeReindexStatus. The legitimate window is at most
	// one DTM scheduler tick (the gap between task FINISHED and the
	// scheduler calling OnGroupCompleted) plus the per-shard swap
	// duration (typically <1s). We use 2× the tick interval as a
	// generous coverage. The clamp at finalizeWindowMin/Max keeps the
	// window reasonable in both pathological sub-second tick configs
	// (clamp up to 3s) and production 60s+ tick configs (clamp down to
	// 10s) — a longer-lived bleed in production was the user-visible
	// face of https://github.com/weaviate/weaviate/issues/10675, and capping the override here
	// keeps the worst-case stale "indexing(1)" pill bounded.
	finalizeWindow := 2 * h.appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval
	if finalizeWindow < finalizeWindowMin {
		finalizeWindow = finalizeWindowMin
	}
	if finalizeWindow > finalizeWindowMax {
		finalizeWindow = finalizeWindowMax
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
			mergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, finalizeWindow, h.appState.Logger)
			// Suppress a stale "indexing@100%" phantom left after DELETE;
			// idx.TaskID is still pre-strip here, matching parsedTasks.
			if !e.flagOn && idx.Status == models.IndexStatusStatusIndexing &&
				h.isPostDeleteFinalizeBleed(collection, prop.Name, canonicalIndexType(e.indexType), idx.TaskID, parsedTasks) {
				continue
			}
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

// findCancelTargetTask returns the in-flight reindex task (and payload)
// targeting (collection, propertyName, indexType), or nil if none. "In
// flight" matches IsActive (STARTED/PREPARING/SWAPPING) — the same predicate
// the conflict gate uses. Selecting a PREPARING/SWAPPING task here does not
// imply the FSM will cancel it: CancelTask only aborts STARTED tasks, so a
// target past STARTED (units done, mid-swap) resolves to an idempotent 202
// NO_OP in cancelReindexTask, not an error.
func findCancelTargetTask(tasks []*distributedtask.Task, collection, propertyName, indexType string) (*distributedtask.Task, db.ReindexTaskPayload) {
	task, payload, _ := firstActiveReindexTask(tasks, decodeSkip, func(p db.ReindexTaskPayload) bool {
		if !strings.EqualFold(p.Collection, collection) || !slices.Contains(p.Properties, propertyName) {
			return false
		}
		matches, _ := migrationTypeTargetsIndex(p.MigrationType, indexType)
		return matches
	})
	return task, payload
}

// reindexTaskCanceller is the slice of the cluster service the cancel path
// needs. *cluster.Service satisfies it (both methods hang off the embedded
// *Raft).
type reindexTaskCanceller interface {
	distributedtask.TaskLister
	CancelDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// cancelReindexTask finds the in-flight reindex task targeting
// (collection, propertyName, indexType) and asks DTM to cancel it.
//
// Idempotent cancel: by the time this runs the caller's (collection,
// property) tuple has already been verified to exist by [cancelIndex] —
// a missing class or property would have produced a 404 there. So when
// no STARTED task matches the cancel target we return 202 + Status:
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
// svc is the cluster service (production passes appState.ClusterService).
// Abstracting the two calls behind reindexTaskCanceller lets the
// idempotent-cancel error mapping be exercised against a real
// distributedtask.Manager without standing up a RAFT stack.
func (h *indexesHandlers) cancelReindexTask(ctx context.Context, svc reindexTaskCanceller, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	tasks, err := svc.ListDistributedTasks(ctx)
	if err != nil {
		return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
			fmt.Sprintf("listing tasks: %v", err)))
	}

	target, targetPayload := findCancelTargetTask(tasks[db.ReindexNamespace], collection, propertyName, indexType)

	if target == nil {
		// Idempotent cancel: caller's (collection, property) is known to
		// exist (cancelIndex verified before dispatch). No task to cancel
		// means the request is a no-op — surface that explicitly via
		// Status: NO_OP at 202 rather than overloading 404 with two
		// distinct semantics (caller-error vs already-done).
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

	if err := svc.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		// The FSM only cancels STARTED tasks. A target that reached
		// PREPARING/SWAPPING (units done, mid-swap) — or completed in the
		// race between the list above and this apply — is rejected with
		// ErrTaskNotRunning. There is nothing left to cancel, so this is an
		// idempotent NO_OP (202), not an infra failure (500).
		if errors.Is(err, distributedtask.ErrTaskNotRunning) {
			h.appState.Logger.WithFields(logrus.Fields{
				"audit_event": "reindex_task_cancel_noop",
				"collection":  collection,
				"property":    propertyName,
				"index_type":  indexType,
				"taskID":      target.ID,
				"principal":   principalUsername(principal),
			}).Info("cancel: task no longer running (past STARTED or completed mid-request); returning NO_OP")
			return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
				Status: reindexCancelStatusNoOp,
			})
		}
		return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
			fmt.Sprintf("cancelling task: %v", err)))
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
			var cleanupErrs []error
			for _, it := range indexTypesToClean {
				if err := h.appState.DB.CleanStalePartialReindexState(ctx, collection, propertyName, it); err != nil {
					cleanupErrs = append(cleanupErrs, fmt.Errorf("indexType=%q: %w", it, err))
				}
			}
			if len(cleanupErrs) > 0 {
				h.appState.Logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
					"strategies": indexTypesToClean,
				}).Errorf("cancel: cleaning partial reindex state on disk for %d strategies failed: %v; next submit's defense-in-depth cleanup will retry", len(cleanupErrs), cleanupErrs)
			} else {
				h.appState.Logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
				}).Info("cancel: on-disk cleanup complete")
			}
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

// finalizeWindowMin / finalizeWindowMax bound the "FINISHED but
// flag-off → indexing@100%" override in [mergeReindexStatus]. The
// window is normally computed as 2× the DTM scheduler tick interval,
// but is clamped at both ends:
//
//   - finalizeWindowMin (3s) protects against pathological sub-second
//     tick configs where 2× would shrink the legitimate window faster
//     than realistic swap-phase jitter. 3s comfortably covers the
//     in-test 1s tick + swap + jitter.
//
//   - finalizeWindowMax (10s) caps how long a stale FINISHED task can
//     bleed an "indexing(1)" pill after a DELETE — production tick is
//     60s, so a naive 2× would let the bleed live for 2 minutes,
//     which was the user-visible face of https://github.com/weaviate/weaviate/issues/10675.
//
// Outside the window, flagOn==false cannot legitimately mean "swap
// pending" — either the swap failed silently (logged as "swap
// INCOMPLETE" elsewhere) or the swap completed and DELETE flipped the
// flag back to false (the frontend repro on 2026-05-14 in
// https://github.com/weaviate/weaviate/issues/10675 — "indexing(1) bleed"). In both cases
// surfacing the override would be a status lie. The trade-off in
// production: between task FINISHED and the schema flag flip, a
// caller polling the GET endpoint will see "indexing@100%" for up to
// 10s, then briefly see an empty searchable entry, then see "ready"
// once the flag flips. The brief empty entry is the original UX gap
// that the override was added to bridge (fd4bfab7cb); we accept it
// here as the lesser evil compared to the unbounded bleed.
const (
	finalizeWindowMin = 3 * time.Second
	finalizeWindowMax = 10 * time.Second
)

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
// Callers iterate the returned slice and run CleanStalePartialReindexState
// once per indexType. Safe to call when no stale state exists: missing
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
	// decodeSkip: an undecodable in-flight task is ignored (the read /
	// idempotency match sites — the submit-time conflict gate flags it
	// instead, so treating it as "no task" here is safe).
	decodeSkip decodeErrorPolicy = iota
	// decodeUndecodableIsHit: an undecodable in-flight task counts as a match
	// (fail closed — the caller can't verify it, so it must not derive a
	// trustworthy NO_OP). Used by the unverifiable-task scan.
	decodeUndecodableIsHit
)

// firstActiveReindexTask returns the first active (IsActive) reindex task whose
// decoded payload satisfies match, plus its payload. On a decode error, policy
// decides: decodeSkip continues; decodeUndecodableIsHit returns that task as a
// match with a zero payload. It factors the shared IsActive + unmarshal loop
// out of the five per-index task-lookup helpers so each keeps only its match
// predicate and decode policy.
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
// FINISHED tasks are kept in the slice (they were dropped here historically,
// but mergeReindexStatus now uses them to surface a brief "indexing@100%"
// SWAPPING-window entry while OnGroupCompleted's swap propagates to the
// schema — without that, the GET response goes empty for a few ms between
// FINISHED and the schema flip, which renders as "None" in the UI).
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
//   - "indexing":   STARTED task with some progress, OR a FINISHED task
//     whose swap hasn't propagated to the schema flag yet
//     (the brief OnGroupCompleted finalize window). The
//     `flagOn` parameter distinguishes the two: when the
//     schema flag is already on, a stale FINISHED task is
//     ignored — the base "ready" wins.
//   - "failed":     latest matching task ended in FAILED.
//   - "cancelled":  latest matching task ended in CANCELLED.
//
// `flagOn` is the caller's view of whether the corresponding schema flag
// (IndexFilterable / IndexSearchable / IndexRangeFilters, depending on
// indexType) is currently true. It lets this function decide whether a
// FINISHED task is "still finalizing" (flag-off) or "fully done"
// (flag-on, so the base "ready" entry takes over).
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
// The logger is used to flag unknown migration types: a future ReindexType
// added without updating this switch would otherwise silently report "ready"
// for an in-flight task. Passing a nil logger is allowed (test callers may
// rely on this); the entry is still skipped, just without a log line.
// finalizeWindow caps the "FINISHED-but-flag-off → indexing@100%"
// override (see the TaskStatusFinished branch below). Callers pass in
// 2× the DTM scheduler tick interval (clamped to finalizeWindowMin);
// the test harness passes a wider value because the test container
// always uses 1s ticks. Pass 0 to disable the override entirely (rare;
// kept for tests that want to assert the post-DELETE bleed never
// surfaces regardless of FinishedAt freshness).
func mergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, flagOn bool, parsedTasks []parsedReindexTask, finalizeWindow time.Duration, logger logrus.FieldLogger) {
	// Two tasks for the same (collection, prop, indexType) may coexist —
	// e.g. a freshly retried STARTED enable-filterable plus the original
	// FAILED attempt that the operator just retried (terminal tasks
	// deliberately do NOT block fresh submits; see checkReindexConflict).
	// Pick the most useful one to surface rather than first-in-map-order:
	//   STARTED > FAILED ≈ CANCELLED ≈ FINISHED   (in-flight beats terminal)
	//   newer StartedAt > older StartedAt          (within the same priority)
	// FINISHED tasks are KEPT: TaskStatusFinished below paints the brief
	// "indexing@100%" finalize window until the schema flag flips.
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
		// every property in the collection.
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
		// The DTM declares a task FINISHED once every unit is terminal, but
		// for semantic migrations (enable-*, change-tokenization) the actual
		// schema flag flip happens later, inside OnGroupCompleted's swap
		// phase. Without a synthetic entry, that window — from "task
		// FINISHED" to "schema flag flipped on this node" — would leave the
		// GET response with no synthetic entry at all and no base "ready"
		// entry (because the flag is still off), so the UI would see an
		// empty `indexes` array and render "None".
		// Treat it as "indexing@100%" until the schema catches up; once
		// flagOn flips true, the base case "ready" override takes precedence
		// and this branch is effectively ignored.
		//
		// Bound the window by task.FinishedAt: outside it, flagOn==false
		// cannot mean "swap pending" — the swap window is at most one
		// scheduler tick plus per-shard swap time, comfortably under
		// reindexFinalizeWindow. If flagOn is still false past this
		// window, the only realistic causes are:
		//   - the swap completed (flag flipped true) and a subsequent
		//     DELETE flipped it back to false (the frontend repro on
		//     2026-05-14 #10675 — "indexing(1) bleed");
		//   - the swap failed silently (logged loudly by
		//     OnGroupCompleted's "swap INCOMPLETE" branch).
		// In neither case do we want a synthetic "indexing@100%" entry —
		// the first case is a stale-task false signal, the second is an
		// error condition the swap-incomplete logs already surface.
		if !flagOn && finalizeWindow > 0 && time.Since(best.FinishedAt) < finalizeWindow {
			idx.Status = "indexing"
			idx.Progress = 1.0
			surfaceSyntheticFields = true
		}
	}

	// Only paint the per-migration-type "in-flight" side-effect fields when
	// the status switch actually surfaced an in-flight or finalizing signal.
	// If the entry stayed "ready" (FINISHED + flag-on, or FINISHED outside
	// the finalize window), the migration has either completed and propagated
	// to the schema (the schema-derived fields above are authoritative) or
	// the task is stale and shouldn't pollute the response.
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

// taskStatusPriority returns a priority for picking the most user-relevant
// task when more than one task matches a (collection, prop, indexType).
// In-flight beats terminal: a user who has just retried a previously
// failed migration wants to see the new attempt's progress, not the old
// failure. FINISHED ranks alongside FAILED / CANCELLED so a recently-
// completed FINISHED task wins the StartedAt tiebreak over an older
// FAILED on the same property (and mergeReindexStatus uses it to keep
// the synthetic "indexing@100%" entry visible until the schema flip
// propagates — see the FINISHED case there).
func taskStatusPriority(task *distributedtask.Task) int {
	switch task.Status {
	case distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping:
		// PREPARING and SWAPPING rank alongside STARTED: from the user's
		// perspective the task is still running (PREP barrier or swap
		// pending; schema flip has not yet committed). Surface their
		// synthetic "indexing@100%" entry instead of an older FAILED
		// attempt's terminal entry.
		return 2
	case distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
		distributedtask.TaskStatusFinished:
		return 1
	default:
		return 0
	}
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

// isPostDeleteFinalizeBleed reports whether a synthetic "indexing@100%"
// entry is a phantom: its driving task (taskID) FINISHED but the index was
// DELETEd afterward. A STARTED task always outranks a FINISHED one, so a
// live re-enable is never suppressed.
func (h *indexesHandlers) isPostDeleteFinalizeBleed(collection, property, indexType, taskID string, parsedTasks []parsedReindexTask) bool {
	if taskID == "" || h.appState == nil || h.appState.ReindexDeleteMarkers == nil {
		return false
	}
	var finishedAt time.Time
	found := false
	for _, pt := range parsedTasks {
		if pt.task.ID != taskID {
			continue
		}
		if pt.task.Status != distributedtask.TaskStatusFinished {
			// A live (STARTED/PREPARING/SWAPPING) task drove this entry —
			// not the finalize-window override. Never suppress.
			return false
		}
		finishedAt = pt.task.FinishedAt
		found = true
		break
	}
	if !found {
		return false
	}
	deletedAt := h.appState.ReindexDeleteMarkers.LastDeleted(collection, property, indexType)
	return !deletedAt.IsZero() && deletedAt.After(finishedAt)
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
	if inflight := countStartedTasksForCollection(collection, tasks); inflight >= maxConcurrentReindexPerCollection {
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

// countStartedTasksForCollection counts in-flight reindex tasks for a
// collection. Counts every non-terminal status (STARTED/PREPARING/SWAPPING
// via IsActive) because PREPARING/SWAPPING still hold tracker dirs and
// reindex buckets.
func countStartedTasksForCollection(collection string, tasks []*distributedtask.Task) int {
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
// Two tasks conflict if they touch the same index bucket type for the same
// property. Every migration type is property-scoped: the property the task
// targets is the one named in payload.Properties. An empty Properties list
// is reserved for a future whole-collection rebuild and is treated as
// matching any property for conflict purposes.
//
// The bucket types each migration touches on its targeted property:
//   - change-algorithm:     searchable bucket (Map/WAND → Blockmax)
//   - rebuild-searchable:   searchable bucket (rebuild in place)
//   - repair-filterable:    filterable bucket
//   - enable-searchable:    searchable bucket (from scratch)
//   - enable-filterable:    filterable bucket (from scratch)
//   - change-tokenization:  searchable + filterable buckets
//   - enable-rangeable:     rangeable bucket — no cross-type conflicts
//
// Unparseable payloads (e.g. payload schema change across versions, RAFT
// replay of a task from an older binary) are treated as a hard error
// rather than silently skipped: silent-skip would let a real bucket-level
// conflict slip through and allow a second task to race against the
// in-flight one.
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
		// Successfully parsed but informationally empty: a `{}` payload, or
		// one missing Collection / MigrationType. This is the same epistemic
		// state as unparseable — we cannot prove non-conflict — so we
		// refuse for the same reason. Most realistic cause: an older binary
		// wrote a payload shape we no longer recognize and the missing fields
		// dropped to their zero values during Unmarshal.
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
