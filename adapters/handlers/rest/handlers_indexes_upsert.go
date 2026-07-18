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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// upsertPlan is the outcome of diffing an upsert/rebuild request against
// current state: noop (respond 200 NO_OP), a conflict (respond 409), or a
// migrationType to submit. Validation failures are returned as errors (400),
// not encoded here.
type upsertPlan struct {
	noop           bool
	conflict       string
	migrationType  db.ReindexMigrationType
	targetTok      string
	bucketStrategy string
}

// upsertIndex implements PUT .../index/{indexType}: diffs the body against
// current state to create or migrate the index, or no-ops (200) if it
// already matches.
func (h *indexesHandlers) upsertIndex(params schema.SchemaObjectsIndexUpsertParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	indexType, ok := normalizeIndexTypeParam(params.IndexName)
	if !ok {
		// Defense in depth: the swagger enum already rejects out-of-set
		// values with 422 before the handler runs.
		return jsonResponder(http.StatusUnprocessableEntity, errorResponse(principal,
			fmt.Sprintf("invalid index type %q", params.IndexName)))
	}

	collection, resp := h.qualifyAndAuthorize(ctx, principal, params.ClassName)
	if resp != nil {
		return resp
	}

	// Lock EARLY (before class read + validation + RAFT submit) so a parallel
	// DELETE can't drop the bucket mid-snapshot — see submitLock godoc.
	propLock := h.submitLock(collection, params.PropertyName)
	propLock.Lock()
	defer propLock.Unlock()

	class, prop, resp := h.readClassProperty(principal, collection, params.PropertyName)
	if resp != nil {
		return resp
	}

	body := params.Body
	if body == nil {
		body = &models.IndexUpsertRequest{}
	}

	// Fetch the RAFT reindex task list once, under the submit lock, so the
	// blockmax derivation, active-task check, and conflict/cap gate all see
	// the same snapshot.
	reindexTasks, resp := h.listReindexTasks(ctx, principal)
	if resp != nil {
		return resp
	}

	plan, err := h.resolveUpsertPlan(class, collection, prop, indexType, body, reindexTasks)
	if err != nil {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
	}
	if plan.conflict != "" {
		return jsonResponder(http.StatusConflict, errorResponse(principal, plan.conflict))
	}
	if plan.noop {
		// A NO_OP still carries the tenants contract (RFC §1.5/§1.10): a
		// mis-scoped tenants param must be rejected, not silently swallowed
		// behind the 200. semantic-ness derives from indexType here — the
		// noop plan has no migrationType, and on PUT only rangeFilters is
		// format-only.
		isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
		if resp := h.validateTenantScope(ctx, principal, collection, isMT, indexType != "rangeable", params.Tenants); resp != nil {
			return resp
		}
		return jsonResponder(http.StatusOK, &models.IndexUpdateResponse{Status: reindexNoOpStatus})
	}

	return h.submitReindexTask(ctx, principal, class, collection, params.PropertyName, plan, params.Tenants, reindexTasks)
}

// rebuildIndex implements POST .../index/{indexType}/rebuild — rebuild the
// index from stored objects with unchanged configuration (repair / format
// refresh).
func (h *indexesHandlers) rebuildIndex(params schema.SchemaObjectsIndexRebuildParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	indexType, ok := normalizeIndexTypeParam(params.IndexName)
	if !ok {
		return jsonResponder(http.StatusUnprocessableEntity, errorResponse(principal,
			fmt.Sprintf("invalid index type %q", params.IndexName)))
	}

	collection, resp := h.qualifyAndAuthorize(ctx, principal, params.ClassName)
	if resp != nil {
		return resp
	}

	propLock := h.submitLock(collection, params.PropertyName)
	propLock.Lock()
	defer propLock.Unlock()

	class, prop, resp := h.readClassProperty(principal, collection, params.PropertyName)
	if resp != nil {
		return resp
	}

	reindexTasks, resp := h.listReindexTasks(ctx, principal)
	if resp != nil {
		return resp
	}

	plan, err := h.resolveRebuildPlan(class, prop, indexType, reindexTasks)
	if err != nil {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
	}

	return h.submitReindexTask(ctx, principal, class, collection, params.PropertyName, plan, params.Tenants, reindexTasks)
}

// listReindexTasks fetches the RAFT reindex task list under the submit lock.
// Fails closed (503) rather than let callers derive blockmax truth,
// idempotency, or the conflict/cap gate from a partial view.
func (h *indexesHandlers) listReindexTasks(ctx context.Context, principal *models.Principal) ([]*distributedtask.Task, middleware.Responder) {
	if h.appState.ClusterService == nil {
		return nil, jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cluster service unavailable; cannot submit reindex task"))
	}
	tasks, err := h.appState.ClusterService.ListDistributedTasks(ctx)
	if err != nil {
		h.appState.Logger.Errorf("submit: failing closed — cannot list in-flight distributed tasks to verify reindex preconditions: %v; rejecting with 503 rather than deriving blockmax/idempotency from a partial view", err)
		return nil, jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			fmt.Sprintf("cannot verify reindex preconditions: listing in-flight tasks failed (%v); retry once the task store is reachable", err)))
	}
	return tasks[db.ReindexNamespace], nil
}

// cancelIndex implements POST .../index/{indexType}/cancel — cancel the
// in-flight reindex task on this property's index. Idempotent.
func (h *indexesHandlers) cancelIndex(params schema.SchemaObjectsIndexCancelParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	indexType, ok := normalizeIndexTypeParam(params.IndexName)
	if !ok {
		return jsonResponder(http.StatusUnprocessableEntity, errorResponse(principal,
			fmt.Sprintf("invalid index type %q", params.IndexName)))
	}

	collection, resp := h.qualifyAndAuthorize(ctx, principal, params.ClassName)
	if resp != nil {
		return resp
	}

	// Cancel drains the local worker and scrubs partial on-disk state, which
	// must not race a concurrent DELETE / submit on the same (collection,
	// property).
	propLock := h.submitLock(collection, params.PropertyName)
	propLock.Lock()
	defer propLock.Unlock()

	// 404 is reserved for unknown collection/property; "nothing to cancel"
	// is a 202 NO_OP.
	if _, _, resp := h.readClassProperty(principal, collection, params.PropertyName); resp != nil {
		return resp
	}

	return h.cancelReindexTask(ctx, collection, params.PropertyName, indexType, principal)
}

// qualifyAndAuthorize resolves and authorizes UPDATE on the collection
// before the submit lock is taken; it deliberately skips the class read so
// that snapshot happens under the lock (see submitLock godoc for the DELETE
// race this closes).
func (h *indexesHandlers) qualifyAndAuthorize(ctx context.Context, principal *models.Principal, className string) (string, middleware.Responder) {
	// Qualify (no alias resolution, like DeleteClassPropertyIndex).
	collection, qErr := namespacing.QualifyClass(principal, h.appState.ServerConfig.Config.Namespaces.Enabled, className)
	if qErr != nil {
		// An unresolvable qualified name is an unknown collection.
		return "", jsonResponder(http.StatusNotFound, errPayloadFromSingleErr(principal, qErr))
	}

	if err := h.appState.Authorizer.Authorize(ctx, principal,
		authorization.UPDATE, authorization.Collections(collection)...); err != nil {
		return "", authzResponder(principal, err)
	}
	return collection, nil
}

// readClassProperty returns a 404 responder for an unknown collection or
// property. Callers hold the submit lock, so the snapshot can't be torn by
// a racing DELETE.
func (h *indexesHandlers) readClassProperty(principal *models.Principal, collection, propertyName string) (*models.Class, *models.Property, middleware.Responder) {
	class := h.appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return nil, nil, jsonResponder(http.StatusNotFound, errorResponse(principal, fmt.Sprintf("collection %q not found", collection)))
	}
	prop := findProperty(class, propertyName)
	if prop == nil {
		return nil, nil, jsonResponder(http.StatusNotFound, errorResponse(principal,
			fmt.Sprintf("property %q not found on collection %q", propertyName, collection)))
	}
	return class, prop, nil
}

// findProperty returns the named property from the class, or nil.
func findProperty(class *models.Class, propertyName string) *models.Property {
	for _, p := range class.Properties {
		if p.Name == propertyName {
			return p
		}
	}
	return nil
}

// reindexNoOpStatus is the IndexUpdateResponse.Status value returned when a
// PUT finds the desired configuration already in place (declarative upsert).
const reindexNoOpStatus = "NO_OP"

// resolveUpsertPlan diffs the request against current state, returning a
// migration to submit, a NO_OP, a 409 conflict, or a 400 validation error.
// reindexTasks supplies blockmax truth and the active-task idempotency check.
func (h *indexesHandlers) resolveUpsertPlan(class *models.Class, collection string, prop *models.Property, indexType string, body *models.IndexUpsertRequest, reindexTasks []*distributedtask.Task) (upsertPlan, error) {
	tok := strings.TrimSpace(body.Tokenization)
	algorithm := strings.TrimSpace(body.Algorithm)

	switch indexType {
	case "searchable":
		return h.resolveSearchableUpsert(class, collection, prop, tok, algorithm, reindexTasks)
	case "filterable":
		return resolveFilterableUpsert(prop, tok, algorithm)
	case "rangeable":
		return resolveRangeableUpsert(class, prop, tok, algorithm)
	}
	// Unreachable: normalizeIndexTypeParam already validated the token.
	return upsertPlan{}, fmt.Errorf("unsupported index type %q", indexType)
}

// searchablePropertyIsBlockmax reports whether the property's searchable
// index is on blockmax, derived only from RAFT-consistent state (class flag
// + task list) so every node agrees regardless of shards held locally.
// See [db.SearchablePropertyBlockmaxFromRAFT].
func searchablePropertyIsBlockmax(class *models.Class, propName string, reindexTasks []*distributedtask.Task) bool {
	classFlag := class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND
	return db.SearchablePropertyBlockmaxFromRAFT(classFlag, class.Class, propName, reindexTasks)
}

// activeSearchableTaskFor returns the in-flight task (if any) converging this
// property's searchable index, and whether it already targets what this
// request asks for.
func activeSearchableTaskFor(collection, propName, tok, algorithm string, reindexTasks []*distributedtask.Task) (task *distributedtask.Task, matches bool) {
	for _, t := range reindexTasks {
		if !t.Status.IsActive() {
			continue
		}
		var p db.ReindexTaskPayload
		if err := json.Unmarshal(t.Payload, &p); err != nil {
			continue
		}
		if !strings.EqualFold(p.Collection, collection) || !slices.Contains(p.Properties, propName) {
			continue
		}
		if !db.TouchesSearchable(p.MigrationType) {
			continue
		}
		return t, requestMatchesActiveSearchable(p.MigrationType, p.TargetTokenization, tok, algorithm)
	}
	return nil, false
}

// requestMatchesActiveSearchable reports whether an in-flight migration
// already converges to the requested target: blockmax is the only algorithm
// target, so either searchable migration type satisfies it; a tokenization
// request needs a matching target tokenization.
func requestMatchesActiveSearchable(activeType db.ReindexMigrationType, activeTargetTok, tok, algorithm string) bool {
	switch {
	case algorithm != "":
		return activeType == db.ReindexTypeChangeAlgorithm || activeType == db.ReindexTypeRebuildSearchable
	case tok != "":
		return (activeType == db.ReindexTypeChangeTokenization || activeType == db.ReindexTypeEnableSearchable) &&
			activeTargetTok == tok
	default:
		return false
	}
}

// resolveSearchableUpsert handles PUT .../index/searchable. At most one of
// tokenization / algorithm may change per request.
func (h *indexesHandlers) resolveSearchableUpsert(class *models.Class, collection string, prop *models.Property, tok, algorithm string, reindexTasks []*distributedtask.Task) (upsertPlan, error) {
	if tok != "" && algorithm != "" {
		return upsertPlan{}, errors.New("at most one configuration change per request: set either tokenization or algorithm, not both — issue two requests")
	}

	// An in-flight task converging this index owns the outcome: a matching
	// request NO-OPs, a differing one 409s. Checked before the schema read
	// below to avoid a stale NO_OP mid-migration.
	if algorithm != "" || tok != "" {
		if task, matches := activeSearchableTaskFor(collection, prop.Name, tok, algorithm, reindexTasks); task != nil {
			if matches {
				return upsertPlan{noop: true}, nil
			}
			return upsertPlan{conflict: fmt.Sprintf(
				"reindex task %q is already migrating searchable property %q to a different target; "+
					"wait for it to finish or cancel it before requesting a different change",
				task.ID, prop.Name)}, nil
		}
	}

	exists := searchableIndexOn(prop)

	switch {
	case algorithm != "":
		// Algorithm change targets an existing searchable index only.
		if !exists {
			return upsertPlan{}, errors.New(db.NoSearchableIndexError(prop.Name))
		}
		switch normalizeSearchableAlgorithm(algorithm) {
		case models.IndexStatusAlgorithmBlockmax:
			// supported target
		case models.IndexStatusAlgorithmWand:
			return upsertPlan{}, fmt.Errorf("algorithm %q is deprecated; only %q is accepted as a target",
				models.IndexStatusAlgorithmWand, models.IndexStatusAlgorithmBlockmax)
		default:
			return upsertPlan{}, fmt.Errorf("unsupported algorithm %q; only %q is accepted (WAND is deprecated)",
				algorithm, models.IndexStatusAlgorithmBlockmax)
		}
		// Already on blockmax → identical config → NO_OP. Use per-property
		// truth: the class-wide UsingBlockMaxWAND flag only flips once EVERY
		// searchable property has migrated, so a property that already
		// migrated must NO_OP even while the class flip is deferred behind
		// its siblings (RFC: repeat blockmax PUT → 200 NO_OP).
		if searchablePropertyIsBlockmax(class, prop.Name, reindexTasks) {
			return upsertPlan{noop: true}, nil
		}
		return upsertPlan{migrationType: db.ReindexTypeChangeAlgorithm}, nil

	case tok != "":
		if !exists {
			// Create the searchable index with the requested tokenization.
			if err := validateEnableSearchableProperty(prop, tok); err != nil {
				return upsertPlan{}, err
			}
			return upsertPlan{migrationType: db.ReindexTypeEnableSearchable, targetTok: tok}, nil
		}
		if prop.Tokenization == tok {
			return upsertPlan{noop: true}, nil
		}
		// Coupled tokenization migration (rewrites searchable + filterable).
		bucketStrategy, err := validateTokenizationChange(class, prop.Name, tok, reindexTasks)
		if err != nil {
			return upsertPlan{}, err
		}
		return upsertPlan{migrationType: db.ReindexTypeChangeTokenization, targetTok: tok, bucketStrategy: bucketStrategy}, nil

	default:
		// Empty body: ensure the index exists with its current config.
		if !exists {
			// validateEnableSearchableProperty("") always errors — "not a text
			// type" for ineligible props, "tokenization required" for text
			// ones — so creating a searchable index always needs a body.
			return upsertPlan{}, validateEnableSearchableProperty(prop, "")
		}
		return upsertPlan{noop: true}, nil
	}
}

// resolveFilterableUpsert handles PUT .../index/filterable.
func resolveFilterableUpsert(prop *models.Property, tok, algorithm string) (upsertPlan, error) {
	if algorithm != "" {
		return upsertPlan{}, errors.New("the algorithm field is only valid for a searchable index")
	}
	exists := filterableIndexOn(prop)

	if !exists {
		// Create. The filterable index is always built with the property's
		// current tokenization; a divergent tokenization is rejected.
		if err := validateEnableFilterableProperty(prop); err != nil {
			return upsertPlan{}, err
		}
		if tok != "" && tok != prop.Tokenization {
			return upsertPlan{}, fmt.Errorf("cannot create a filterable index for property %q with tokenization %q: it must match the property's current tokenization %q — retokenize after creation to change it",
				prop.Name, tok, prop.Tokenization)
		}
		return upsertPlan{migrationType: db.ReindexTypeEnableFilterable}, nil
	}

	// Exists: no tokenization change requested (or identical) → NO_OP.
	if tok == "" || tok == prop.Tokenization {
		return upsertPlan{noop: true}, nil
	}
	// Filterable-only retokenize (does not touch the searchable bucket).
	if err := validateFilterableTokenizationChange(prop, tok); err != nil {
		return upsertPlan{}, err
	}
	return upsertPlan{migrationType: db.ReindexTypeChangeTokenizationFilterable, targetTok: tok}, nil
}

// resolveRangeableUpsert handles PUT .../index/rangeFilters (internal token
// "rangeable"). The rangeFilters index takes no configuration fields.
func resolveRangeableUpsert(class *models.Class, prop *models.Property, tok, algorithm string) (upsertPlan, error) {
	if tok != "" || algorithm != "" {
		return upsertPlan{}, errors.New("the rangeFilters index takes no configuration fields; send an empty body {}")
	}
	if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
		return upsertPlan{noop: true}, nil
	}
	if err := validateRangeableProperties(class, []string{prop.Name}); err != nil {
		return upsertPlan{}, err
	}
	return upsertPlan{migrationType: db.ReindexTypeEnableRangeable}, nil
}

// resolveRebuildPlan validates the rebuild preconditions for the internal
// index-type token and returns the repair/rebuild migration to submit.
func (h *indexesHandlers) resolveRebuildPlan(class *models.Class, prop *models.Property, indexType string, reindexTasks []*distributedtask.Task) (upsertPlan, error) {
	switch indexType {
	case "searchable":
		if !searchableIndexOn(prop) {
			return upsertPlan{}, errors.New(db.NoSearchableIndexError(prop.Name))
		}
		// A WAND (map-strategy) searchable index cannot be rebuilt — migrate
		// to blockmax first. Per-property truth: a property whose bucket is
		// already blockmax must be rebuildable even while the class-wide flag
		// is still deferred behind sibling properties.
		if !searchablePropertyIsBlockmax(class, prop.Name, reindexTasks) {
			return upsertPlan{}, errors.New("cannot rebuild a WAND searchable index — WAND is deprecated; PUT {\"algorithm\":\"blockmax\"} to migrate first")
		}
		return upsertPlan{migrationType: db.ReindexTypeRebuildSearchable}, nil

	case "filterable":
		if !filterableIndexOn(prop) {
			return upsertPlan{}, fmt.Errorf("property %q does not have a filterable index", prop.Name)
		}
		if err := validateRebuildFilterableDataType(prop); err != nil {
			return upsertPlan{}, err
		}
		return upsertPlan{migrationType: db.ReindexTypeRepairFilterable}, nil

	case "rangeable":
		if err := validateRebuildRangeableProperty(prop); err != nil {
			return upsertPlan{}, err
		}
		return upsertPlan{migrationType: db.ReindexTypeRepairRangeable}, nil
	}
	// Unreachable: normalizeIndexTypeParam already validated the token.
	return upsertPlan{}, fmt.Errorf("unsupported index type %q", indexType)
}

// validateTenantScope enforces the RFC §1.5/§1.10 tenants contract: the query
// param is honored only on a multi-tenant collection, only for a format-only
// (non-semantic) operation, and only for existing, active tenants. Returns a
// 400 responder on violation, nil when the (possibly empty) scope is
// acceptable. Shared by the submit path and the NO_OP fast-path so a
// mis-scoped request is rejected there too, never silently accepted with 200.
func (h *indexesHandlers) validateTenantScope(ctx context.Context, principal *models.Principal, collection string, isMT, semantic bool, tenants []string) middleware.Responder {
	if len(tenants) == 0 {
		return nil
	}
	if !isMT {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal,
			"tenants parameter is only valid for multi-tenant collections"))
	}
	if semantic {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal,
			"tenants parameter cannot be used with semantic migrations; all tenants must be targeted"))
	}
	if err := validateTenants(h.appState.DB, ctx, collection, tenants); err != nil {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
	}
	return nil
}

// submitReindexTask is the shared submit path for upsert and rebuild:
// validates tenant scope, checks conflicts/cap, and submits the distributed
// task. Returns 202 STARTED or the mapped error, reusing the caller's RAFT
// snapshot (reindexTasks) so check-and-submit sees one consistent view.
func (h *indexesHandlers) submitReindexTask(ctx context.Context, principal *models.Principal, class *models.Class, collection, propertyName string, plan upsertPlan, tenants []string, reindexTasks []*distributedtask.Task) middleware.Responder {
	if h.appState.ClusterService == nil {
		return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cluster service unavailable; cannot submit reindex task"))
	}

	migrationType := plan.migrationType
	properties := []string{propertyName}
	semantic := db.IsSemanticMigration(migrationType)
	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled

	if resp := h.validateTenantScope(ctx, principal, collection, isMT, semantic, tenants); resp != nil {
		return resp
	}

	// One unit per shard per replica: each replica has its own local copy.
	var shardOwnership map[string][]string
	var err error
	if isMT {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnershipForMT(ctx, collection, tenants)
	} else {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnership(ctx, collection)
	}
	if err != nil {
		return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
			fmt.Sprintf("getting shard ownership: %v", err)))
	}
	if len(shardOwnership) == 0 {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, "collection has no shards"))
	}

	unitIDs, unitToShard, unitToNode := buildUnitMaps(shardOwnership)

	// Capture the property's submit-time tokenization so a post-restart
	// FSM replay of an older task can't override a newer task's schema flip
	// (see OriginalTokenization godoc on ReindexTaskPayload).
	var originalTok string
	if migrationType == db.ReindexTypeChangeTokenization ||
		migrationType == db.ReindexTypeChangeTokenizationFilterable ||
		migrationType == db.ReindexTypeEnableSearchable {
		if p := findProperty(class, propertyName); p != nil {
			originalTok = p.Tokenization
		}
	}

	payload := db.ReindexTaskPayload{
		MigrationType:        migrationType,
		Collection:           collection,
		Properties:           properties,
		TargetTokenization:   plan.targetTok,
		OriginalTokenization: originalTok,
		BucketStrategy:       plan.bucketStrategy,
		Tenants:              tenants,
		UnitToNode:           unitToNode,
		UnitToShard:          unitToShard,
	}

	// Human-readable task ID with a random suffix for uniqueness.
	// Format: "Collection:migration-type:property:ab3f".
	taskID := fmt.Sprintf("%s:%s:%s:%s", collection, migrationType, properties[0], shortRandomSuffix())

	// Conflict + concurrency-cap checks against the caller's RAFT snapshot,
	// serialized by the caller's submit lock against parallel DELETE and
	// submits. listErr is nil: the list was already fetched (fail-closed)
	// in listReindexTasks.
	if resp := h.checkReindexAdmission(principal, collection, migrationType, properties,
		reindexTasks, nil); resp != nil {
		return resp
	}

	// Defense in depth against CANCEL→retry silently resuming stale partial
	// state and reporting false success. Must clean BOTH dirs for a coupled
	// change-tokenization — see indexTypesFromMigrationType godoc.
	if indexTypesForCleanup, known := indexTypesFromMigrationType(migrationType); known {
		for _, it := range indexTypesForCleanup {
			if err := h.appState.DB.CleanStalePartialReindexState(ctx, collection, propertyName, it); err != nil {
				h.appState.Logger.WithFields(logrus.Fields{
					"collection":     collection,
					"property":       propertyName,
					"migration_type": migrationType,
					"index_type":     it,
				}).Errorf("submit: pre-submit cleanup of stale partial reindex state failed: %v; the new task may short-circuit on the stale state and report a false success — operator inspection recommended", err)
			}
		}
	}

	// Semantic migrations opt into the two-phase RAFT PREP barrier; MT
	// semantic migrations also group by tenant for per-tenant barriers.
	if isMT && semantic {
		unitSpecs := buildUnitSpecs(shardOwnership)
		if err := h.appState.ClusterService.AddDistributedTaskWithGroupsBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitSpecs, semantic,
		); err != nil {
			return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
				fmt.Sprintf("submitting task: %v", err)))
		}
	} else {
		if err := h.appState.ClusterService.AddDistributedTaskWithBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitIDs, semantic,
		); err != nil {
			return jsonResponder(http.StatusInternalServerError, errorResponse(principal,
				fmt.Sprintf("submitting task: %v", err)))
		}
	}

	// Operational audit line for a privileged cluster-wide operation.
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_submitted",
		"taskID":         taskID,
		"collection":     collection,
		"property":       propertyName,
		"migration_type": migrationType,
		"principal":      principalUsername(principal),
	}).Info("reindex provider: submitted task")

	return jsonResponder(http.StatusAccepted, &models.IndexUpdateResponse{
		// The task ID embeds the qualified collection.
		TaskID: namespacing.StripOwnNamespace(principal, taskID),
		Status: "STARTED",
	})
}
