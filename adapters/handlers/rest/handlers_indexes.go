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
	"slices"
	"strings"
	"sync"
	"time"

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
	api.SchemaSchemaObjectsIndexesUpdateHandler = schema.SchemaObjectsIndexesUpdateHandlerFunc(h.updateIndex)
}

type indexesHandlers struct {
	appState *state.State
}

// submitLock returns the per-(collection, property) mutex for the
// check-and-submit critical section, allocating one on first use.
//
// The actual lock manager lives on appState (ReindexSubmitLocks) so
// it is SHARED with the DELETE-property-index REST handler. Without
// the sharing, a parallel PUT /indexes/{prop} (which submits a
// reindex task) and DELETE /properties/{prop}/index/{indexName}
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
	collection := params.ClassName

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

	// BM25 algorithm currently backing searchable indexes for this class.
	// The schema-level UsingBlockMaxWAND flag flips only after every
	// searchable bucket on every shard has been migrated to blockmax (see
	// MapToBlockmaxStrategy.OnMigrationComplete). While a per-property
	// repair-searchable is in flight the flag is still false; the
	// targetAlgorithm field (set by mergeReindexStatus) carries the
	// "incoming" signal in that case.
	searchableAlgorithm := models.IndexStatusAlgorithmWand
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
		searchableAlgorithm = models.IndexStatusAlgorithmBlockmax
	}

	// Build per-property index status.
	props := make([]*models.PropertyIndexStatus, 0, len(class.Properties))
	for _, prop := range class.Properties {
		pis := &models.PropertyIndexStatus{
			Name:     prop.Name,
			DataType: dataTypeString(prop),
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
			idx := &models.IndexStatus{Type: e.indexType, Status: "ready"}
			if e.flagOn && e.carryTokenization {
				idx.Tokenization = prop.Tokenization
			}
			// Only searchable indexes have a BM25 algorithm; surface the
			// class-level wand/blockmax state so the UI can render it
			// honestly. Filterable / rangeable have no equivalent today.
			if e.indexType == "searchable" && e.flagOn {
				idx.Algorithm = searchableAlgorithm
			}
			mergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, finalizeWindow, h.appState.Logger)
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
		Collection: collection,
		Properties: props,
	})
}

// updateIndex implements PUT /v1/schema/{className}/indexes/{propertyName}.
//
// Concurrent non-conflicting reindex tasks are allowed. Two tasks conflict if
// they would touch the same bucket for the same property. The conflict check
// rejects same-type same-property tasks, plus cross-type conflicts (e.g.
// repair-searchable blocks change-tokenization on any property since
// repair-searchable touches all searchable buckets).
func (h *indexesHandlers) updateIndex(params schema.SchemaObjectsIndexesUpdateParams, principal *models.Principal) middleware.Responder {
	collection := params.ClassName
	propertyName := params.PropertyName

	// Require UPDATE on the collection itself: submitting a reindex task is a
	// privileged, cluster-wide, destructive operation (rebuilds buckets on
	// every replica, flips schema flags). The read-only authzed sibling above
	// uses CollectionsMetadata; here we need the stronger Collections verb.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.UPDATE, authorization.Collections(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesUpdateForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	// Acquire the per-(collection, property) submit lock EARLY — before
	// reading the class or running any validation — so a parallel DELETE
	// on /properties/{prop}/index/{name} cannot mutate the schema (drop
	// the canonical bucket) between this handler's class read and its
	// task-add RAFT call.
	//
	// The previous lock position (just before AddDistributedTask, after
	// validation) was insufficient: a parallel DELETE could win the lock,
	// flip IndexSearchable=false + drop the searchable bucket, release;
	// meanwhile PUT was already past its `class := ReadOnlyClass(...)` +
	// `validateTokenizationChange(targetProp)` snapshot which still
	// observed IndexSearchable=true, so validation passed and PUT
	// proceeded to submit a change-tok task against a no-longer-existing
	// bucket — FilterableRetokenize/SearchableRetokenize then failed
	// at the swap step. The
	// TestParallelConflictMatrix/change_tokenization_both__delete_searchable_parallel
	// case in test/acceptance/reindex_concurrent pins this scenario.
	//
	// Now: PUT holds the lock across class read + validation + RAFT
	// task-add. A concurrent DELETE waits; when it acquires, the task
	// is in-flight in RAFT and the apply-time MutationGuard
	// rejects the DELETE deterministically. If DELETE wins instead,
	// PUT's class read sees IndexSearchable=false and
	// validateTokenizationChange rejects with 400.
	propLock := h.submitLock(collection, propertyName)
	propLock.Lock()
	defer propLock.Unlock()

	class := h.appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return schema.NewSchemaObjectsIndexesUpdateNotFound()
	}

	// Find the property.
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == propertyName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return schema.NewSchemaObjectsIndexesUpdateNotFound()
	}

	body := params.Body
	if body == nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, "request body required"))
	}

	// Reject ambiguous bodies (multiple groups set, conflicting verbs within
	// a group, or zero verbs) before the switch silently picks one arm.
	if err := validateBodyExclusivity(body); err != nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
	}

	// Cancel is fundamentally different from the other actions: it does not
	// submit a new task, it asks DTM to abort one. Handle it up front so the
	// switch below stays focused on submit-shaped intents.
	if cancelIndexType, cancelling := requestedCancel(body); cancelling {
		return h.cancelReindexTask(params.HTTPRequest.Context(), collection, propertyName, cancelIndexType, principal)
	}

	// Determine which migration type to submit based on the diff.
	var (
		migrationType  db.ReindexMigrationType
		properties     []string
		targetTok      string
		bucketStrategy string
	)

	switch {
	// enable-searchable must be matched BEFORE change-tokenization: an
	// enable request carries tokenization in the same body, but a property
	// that has no searchable index yet cannot have its tokenization
	// "changed" — validateTokenizationChange would fail looking for a
	// non-existent searchable bucket.
	case body.Searchable != nil && body.Searchable.Enabled:
		migrationType = db.ReindexTypeEnableSearchable
		properties = []string{propertyName}
		targetTok = body.Searchable.Tokenization
		if err := validateEnableSearchableProperty(targetProp, targetTok); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Tokenization != "":
		// Change tokenization on a property whose searchable index already
		// exists. If Enabled was also set it would have matched the case
		// above.
		migrationType = db.ReindexTypeChangeTokenization
		properties = []string{propertyName}
		targetTok = body.Searchable.Tokenization

		// Reject early when the property has no searchable index. Otherwise
		// the downstream validator surfaces a "searchable bucket not
		// found" error that doesn't tell the caller what to do — they
		// just see a 400 and the dialog hangs. Filterable-only properties
		// should use {filterable: {tokenization: X}} instead.
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("property %q has no searchable index; use {\"filterable\":{\"tokenization\":...}} to retokenize the filterable bucket, or {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add a searchable index", propertyName)))
		}

		var err error
		bucketStrategy, err = validateTokenizationChange(h.appState, class, collection, propertyName, targetTok)
		if err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Filterable != nil && body.Filterable.Tokenization != "":
		// Change tokenization on a property whose filterable index exists.
		// Differs from {searchable:{tokenization:X}}: this variant
		// retokenizes ONLY the filterable bucket, never the searchable.
		// The right shape for filterable-only text/text[] properties, and
		// also valid when the property has both indexes and the caller
		// wants to retokenize only the filterable side (rare but
		// well-defined: filterable uses Equal semantics, retokenizing it
		// independently of searchable is meaningful).
		migrationType = db.ReindexTypeChangeTokenizationFilterable
		properties = []string{propertyName}
		targetTok = body.Filterable.Tokenization

		if err := validateFilterableTokenizationChange(targetProp, targetTok); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Rebuild:
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("property %q does not have a searchable index", propertyName)))
		}
		// rebuild preserves the current BM25 algorithm and tokenization.
		// WAND searchable indexes cannot be rebuilt — the only supported
		// next step for them is migration to BlockMax via
		// {"searchable":{"algorithm":"blockmax"}}.
		if class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.UsingBlockMaxWAND {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				"cannot rebuild a WAND searchable index — WAND is deprecated; use {\"searchable\":{\"algorithm\":\"blockmax\"}} to migrate first"))
		}
		migrationType = db.ReindexTypeRebuildSearchable
		properties = []string{propertyName}

	case body.Searchable != nil && body.Searchable.Algorithm != "":
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("property %q does not have a searchable index", propertyName)))
		}
		// Case-insensitive match — swagger generated EnumCase validator
		// is permissive here, so accept "Blockmax" / "BLOCKMAX" too.
		if !strings.EqualFold(body.Searchable.Algorithm, models.IndexStatusAlgorithmBlockmax) {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("unsupported algorithm %q; only %q is accepted (WAND is deprecated)",
					body.Searchable.Algorithm, models.IndexStatusAlgorithmBlockmax)))
		}
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				"searchable index is already on blockmax"))
		}
		migrationType = db.ReindexTypeChangeAlgorithm
		properties = []string{propertyName}

	case body.Filterable != nil && body.Filterable.Enabled:
		migrationType = db.ReindexTypeEnableFilterable
		properties = []string{propertyName}
		if err := validateEnableFilterableProperty(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Filterable != nil && body.Filterable.Rebuild:
		migrationType = db.ReindexTypeRepairFilterable
		properties = []string{propertyName}
		if targetProp.IndexFilterable != nil && !*targetProp.IndexFilterable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("property %q does not have a filterable index", propertyName)))
		}
		if err := validateRebuildFilterableDataType(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Rangeable != nil && body.Rangeable.Enabled:
		migrationType = db.ReindexTypeEnableRangeable
		properties = []string{propertyName}
		if err := validateRangeableProperties(class, properties); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Rangeable != nil && body.Rangeable.Rebuild:
		migrationType = db.ReindexTypeRepairRangeable
		properties = []string{propertyName}
		if err := validateRebuildRangeableProperty(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	default:
		// The verb list must enumerate EVERY dispatch case above. A missing
		// verb here ships as a confusing 400 ("you sent a valid body shape
		// but the error says it's invalid") and was the symptom flagged on
		// weaviate/0-weaviate-issues#227 (Gap 7). Order: per index-group,
		// then alphabetical within group.
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
			"no actionable change detected; set one of: "+
				"searchable.algorithm, searchable.cancel, searchable.enabled, searchable.rebuild, searchable.tokenization, "+
				"filterable.cancel, filterable.enabled, filterable.rebuild, filterable.tokenization, "+
				"rangeable.cancel, rangeable.enabled, rangeable.rebuild"))
	}

	// --- Multi-tenancy handling ---
	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
	tenants := params.Tenants
	semantic := db.IsSemanticMigration(migrationType)

	// Validate MT + tenants combination.
	if !isMT && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse(principal, "tenants parameter is only valid for multi-tenant collections"))
	}
	if semantic && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse(principal, "tenants parameter cannot be used with semantic migrations (change-tokenization); all tenants must be targeted"))
	}

	// For MT collections with specific tenants, validate they exist and are not OFFLOADED/FROZEN.
	if isMT && len(tenants) > 0 {
		if err := validateTenants(h.appState.DB, params.HTTPRequest.Context(), collection, tenants); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}
	}

	// Build unit maps from shard placement. Use ShardReplicaOwnership (not
	// ShardOwnership) to create one unit per shard per replica node. Each
	// replica has its own local copy of the data that must be reindexed.
	ctx := params.HTTPRequest.Context()
	var shardOwnership map[string][]string
	var err error
	if isMT {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnershipForMT(ctx, collection, tenants)
	} else {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnership(ctx, collection)
	}
	if err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
			errorResponse(principal, fmt.Sprintf("getting shard ownership: %v", err)))
	}
	if len(shardOwnership) == 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, "collection has no shards"))
	}

	unitIDs, unitToShard, unitToNode := buildUnitMaps(shardOwnership)

	// Capture the property's tokenization at submit-time. OnTaskCompleted
	// will check this in the schema-flip mutator so a post-restart
	// FSM-replay of an older task can't override a newer task's already-
	// applied schema flip. See the OriginalTokenization godoc on
	// ReindexTaskPayload for the full rationale.
	var originalTok string
	if migrationType == db.ReindexTypeChangeTokenization ||
		migrationType == db.ReindexTypeChangeTokenizationFilterable ||
		migrationType == db.ReindexTypeEnableSearchable {
		originalTok = targetProp.Tokenization
	}

	payload := db.ReindexTaskPayload{
		MigrationType:        migrationType,
		Collection:           collection,
		Properties:           properties,
		TargetTokenization:   targetTok,
		OriginalTokenization: originalTok,
		BucketStrategy:       bucketStrategy,
		Tenants:              tenants,
		UnitToNode:           unitToNode,
		UnitToShard:          unitToShard,
	}

	// Build a human-readable task ID with a random suffix for uniqueness.
	// Format: "Collection:migration-type:property:ab3f" (or without property for whole-collection ops).
	suffix := shortRandomSuffix()
	taskID := fmt.Sprintf("%s:%s:%s", collection, migrationType, suffix)
	if len(properties) > 0 {
		taskID = fmt.Sprintf("%s:%s:%s:%s", collection, migrationType, properties[0], suffix)
	}

	// Note: propLock for (collection, propertyName) was acquired at
	// the top of this handler — before the class read and validation —
	// so the conflict-check + AddDistributedTask + DELETE-property-
	// index races are all serialized through the same lock entry. See
	// the early-acquisition comment up top + [state.ReindexSubmitLocks]
	// godoc for the multi-node caveat.

	// Check for conflicting active tasks. Any two reindex migrations on
	// the same (collection, property) tuple conflict; see typesConflict's
	// godoc for the on-disk state race that motivated the rule.
	if h.appState.ClusterService != nil {
		tasks, err := h.appState.ClusterService.ListDistributedTasks(ctx)
		if err == nil {
			reason, checkErr := checkReindexConflict(collection, migrationType, properties, tasks[db.ReindexNamespace])
			if checkErr != nil {
				// An in-flight task has an unparseable payload — we cannot
				// prove the new submit doesn't conflict with it, so refuse
				// rather than race. Return 503 so the caller knows to retry
				// after an operator inspects the in-flight task.
				return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal, checkErr.Error()))
			}
			if reason != "" {
				return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(errorResponse(principal, reason))
			}
			// Per-collection cap on concurrent STARTED reindex tasks. Without
			// this a caller scripting `for p in $(properties); do PUT
			// .../indexes/$p; done` against an N-property collection submits N
			// independent RAFT tasks, each fanning out ingest+backup buckets
			// on every replica. The LSM compaction layer and disk would not
			// survive that. Reject with 429 once the cap is reached.
			if inflight := countStartedTasksForCollection(collection, tasks[db.ReindexNamespace]); inflight >= maxConcurrentReindexPerCollection {
				return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal, fmt.Sprintf(
					"collection %q already has %d concurrent reindex tasks (max %d); wait for one to finish before submitting another",
					collection, inflight, maxConcurrentReindexPerCollection)))
			}
		}
	}

	// Defense in depth against the CANCEL→retry silent failure (same Sev 1
	// family as DELETE→re-enable, fixed in 6b7dc23768): if a previous
	// cancelled run left stale .migrations/<dir>/started.mig +
	// __reindex/__ingest sidecars on disk, the new task would resume
	// against them — finish in <1s with a 50-entry no-op — flip the
	// schema flag, and report success against an empty bucket.
	//
	// The cancel handler already runs this cleanup synchronously, but
	// only after waiting for the local goroutine to drain. The wait can
	// time out (or be skipped entirely if the node crashed mid-cancel),
	// in which case the on-disk state survives. Running it again here,
	// AFTER checkReindexConflict has confirmed no STARTED task targets
	// this (collection, prop, index) tuple, closes that gap.
	//
	// Safe to call even when no stale state exists: missing buckets and
	// missing directories are silently skipped by the per-shard helper.
	indexTypesForCleanup, indexTypeKnown := indexTypesFromMigrationType(migrationType)
	if indexTypeKnown {
		// Loop over every index type this migration touches. For
		// single-index migrations the slice has one entry; for
		// change-tokenization-both (which writes searchable AND filterable
		// sub-task dirs) it has two. Cleaning BOTH is critical — see the
		// indexTypesFromMigrationType godoc for the Sev 1 data-loss bug
		// that motivated the multi-index sweep.
		for _, indexTypeForCleanup := range indexTypesForCleanup {
			if err := h.appState.DB.CleanStalePartialReindexState(ctx, collection, propertyName, indexTypeForCleanup); err != nil {
				h.appState.Logger.WithFields(logrus.Fields{
					"collection":     collection,
					"property":       propertyName,
					"migration_type": migrationType,
					"index_type":     indexTypeForCleanup,
				}).Errorf("submit: pre-submit cleanup of stale partial reindex state failed: %v; the new task may short-circuit on the stale state and report a false success — operator inspection recommended", err)
			}
		}
	}

	// Semantic migrations opt into the two-phase RAFT PREP barrier;
	// MT semantic migrations also group by tenant for per-tenant barriers.
	if isMT && semantic {
		unitSpecs := buildUnitSpecs(shardOwnership)
		if err := h.appState.ClusterService.AddDistributedTaskWithGroupsBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitSpecs, semantic,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(principal, fmt.Sprintf("submitting task: %v", err)))
		}
	} else {
		if err := h.appState.ClusterService.AddDistributedTaskWithBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitIDs, semantic,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(principal, fmt.Sprintf("submitting task: %v", err)))
		}
	}

	// Operational audit line: reindex is a privileged cluster-wide operation
	// (rebuilds buckets on every replica, flips schema flags). Log the who,
	// what, and which task ID at submit time so ops can grep for it later.
	// RBAC audit logging upstream covers the authorize/deny decision; this
	// log covers the successful submission.
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_submitted",
		"taskID":         taskID,
		"collection":     collection,
		"property":       propertyName,
		"migration_type": migrationType,
		"principal":      principalUsername(principal),
	}).Info("reindex provider: submitted task")

	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		TaskID: taskID,
		Status: "STARTED",
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

// requestedCancel returns (indexType, true) if the body asks to cancel an
// in-flight reindex on this property, where indexType is one of
// "filterable", "searchable", or "rangeable". Returns ("", false)
// otherwise. validateBodyExclusivity has already guaranteed at most one
// cancel field is set across the body.
func requestedCancel(body *models.IndexUpdateRequest) (string, bool) {
	switch {
	case body.Searchable != nil && body.Searchable.Cancel:
		return "searchable", true
	case body.Filterable != nil && body.Filterable.Cancel:
		return "filterable", true
	case body.Rangeable != nil && body.Rangeable.Cancel:
		return "rangeable", true
	}
	return "", false
}

// cancelReindexTask finds the STARTED reindex task targeting
// (collection, propertyName, indexType) and asks DTM to cancel it.
// Returns 404 if no matching task exists, 202 with the cancelled
// task ID on success. The DTM scheduler picks up the CANCELLED state on
// its next tick and terminates the local handle; the task's ctx (the
// provider's per-task ctx via runningHandles) is then cancelled, and
// the worker goroutine returns.
func (h *indexesHandlers) cancelReindexTask(ctx context.Context, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	if h.appState.ClusterService == nil {
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal,
			"cluster service unavailable; cannot cancel reindex task"))
	}

	tasks, err := h.appState.ClusterService.ListDistributedTasks(ctx)
	if err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errorResponse(principal,
			fmt.Sprintf("listing tasks: %v", err)))
	}

	// Find the STARTED task that targets this (collection, prop, indexType).
	var target *distributedtask.Task
	var targetPayload db.ReindexTaskPayload
	for _, task := range tasks[db.ReindexNamespace] {
		if task.Status != distributedtask.TaskStatusStarted {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}
		if !slices.Contains(payload.Properties, propertyName) {
			continue
		}
		if matches, _ := migrationTypeTargetsIndex(payload.MigrationType, indexType); !matches {
			continue
		}
		target = task
		targetPayload = payload
		break
	}

	if target == nil {
		// 0-weaviate-issues#215 B5: bare 404 with no body is
		// indistinguishable from "endpoint not found" for operators
		// chasing a stuck-state cancel. Return a structured error
		// body identifying the (collection, property, indexType)
		// tuple the operator was trying to cancel and the actionable
		// remedy.
		return schema.NewSchemaObjectsIndexesUpdateNotFound().WithPayload(errorResponse(principal, fmt.Sprintf(
			"no in-flight reindex task to cancel for (collection=%q, property=%q, indexType=%q): the task may have already finished, been cancelled, or never been started; use GET /v1/schema/%s/indexes to inspect the current state",
			collection, propertyName, indexType, collection)))
	}

	if err := h.appState.ClusterService.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errorResponse(principal,
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
			// Goroutine has drained. Safe to wipe the sidecars and the
			// migration directory so the next submit starts from a clean
			// slate. Errors here are logged but don't fail the cancel —
			// the user already received 202 conceptually, and the defense
			// in depth at submit time will re-run cleanup.
			//
			// 0-weaviate-issues#215 B8: walk EVERY indexType the
			// migration touches, not just the indexType named in the
			// request URL. change-tokenization spawns both a
			// searchable and a filterable strategy under a single
			// DTM task; a cancel that cleans only `indexType` leaves
			// the sibling's `payload.mig` orphan (and, if the sibling's
			// iteration had progressed past markStarted, its sidecar
			// bucket too).
			indexTypesToClean, known := indexTypesFromMigrationType(targetPayload.MigrationType)
			if !known || len(indexTypesToClean) == 0 {
				// Defensive: a payload whose migration type we don't
				// recognise (e.g. an unknown future strategy) still
				// gets at least the indexType the user named cleaned,
				// so the cancel path stays in lockstep with submit-time
				// pre-cleanup (which also degrades to "nothing to do"
				// on unknown types).
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
		"audit_event": "reindex_task_cancelled",
		"taskID":      target.ID,
		"collection":  collection,
		"property":    propertyName,
		"index_type":  indexType,
		"principal":   principalUsername(principal),
	}).Info("reindex provider: cancelled task")

	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		TaskID: target.ID,
		Status: "CANCELLED",
	})
}

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

// normaliseSearchableAlgorithm canonicalises an explicit
// searchable.algorithm verb value into the single supported target
// (BlockMaxWAND). Accepted aliases are case-insensitive:
// "BlockMaxWAND", "blockmax", "BMW", "block_max_wand". Any other
// value (including "WAND", "wand") returns "" — the BlockMax→WAND
// reverse direction is intentionally not supported at this time
// because the underlying repair-searchable migration only writes
// blockmax-format segments. Callers map "" to a 400 with a clear
// message; see 0-weaviate-issues#215 B7.
func normaliseSearchableAlgorithm(value string) string {
	switch strings.ToLower(strings.ReplaceAll(value, "_", "")) {
	case "blockmaxwand", "blockmax", "bmw":
		return "BlockMaxWAND"
	}
	return ""
}

// parsedReindexTask pairs a distributed task with its already-unmarshalled
// reindex payload. The handler builds a slice of these once per request
// so mergeReindexStatus doesn't re-unmarshal task.Payload N times where
// N is the number of properties in the collection.
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
	//   STARTED  > FAILED ≈ CANCELLED       (in-flight beats terminal)
	//   newer StartedAt > older StartedAt   (within the same priority)
	// FINISHED was already skipped by parseReindexTasks (the schema flag
	// flips and the regular "ready" entry takes over).
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
		// repair-searchable migrates WAND → BlockMax. The targetAlgorithm
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

func errorResponse(principal *models.Principal, msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: namespacing.StripErrorMessage(principal, msg)},
		},
	}
}

// maxConcurrentReindexPerCollection caps how many STARTED reindex tasks
// can target the same collection at once. Each task creates ingest +
// backup buckets on every replica; without a cap, a script that runs
// PUT /indexes/<prop> per property would fan out N tasks for an
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
//   - repair-searchable:    searchable bucket
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
