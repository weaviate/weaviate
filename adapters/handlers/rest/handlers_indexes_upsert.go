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
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// upsertPlan is the outcome of diffing an upsert/rebuild request against
// current state: noop (respond 200 NO_OP) or migrationType (submit it).
// Validation failures are returned as errors, not encoded here.
type upsertPlan struct {
	noop           bool
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

	plan, err := h.resolveUpsertPlan(class, collection, prop, indexType, body)
	if err != nil {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
	}
	if plan.noop {
		return jsonResponder(http.StatusOK, &models.IndexUpdateResponse{Status: reindexNoOpStatus})
	}

	return h.submitReindexTask(ctx, principal, class, collection, params.PropertyName, plan, params.Tenants)
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

	plan, err := resolveRebuildPlan(class, prop, indexType)
	if err != nil {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
	}

	return h.submitReindexTask(ctx, principal, class, collection, params.PropertyName, plan, params.Tenants)
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

// resolveUpsertPlan diffs the body against current state and returns a
// migration to submit, a NO_OP, or a validation error (400).
func (h *indexesHandlers) resolveUpsertPlan(class *models.Class, collection string, prop *models.Property, indexType string, body *models.IndexUpsertRequest) (upsertPlan, error) {
	tok := strings.TrimSpace(body.Tokenization)
	algorithm := strings.TrimSpace(body.Algorithm)

	switch indexType {
	case "searchable":
		return h.resolveSearchableUpsert(class, collection, prop, tok, algorithm)
	case "filterable":
		return resolveFilterableUpsert(prop, tok, algorithm)
	case "rangeable":
		return resolveRangeableUpsert(class, prop, tok, algorithm)
	}
	// Unreachable: normalizeIndexTypeParam already validated the token.
	return upsertPlan{}, fmt.Errorf("unsupported index type %q", indexType)
}

// resolveSearchableUpsert handles PUT .../index/searchable. At most one of
// tokenization / algorithm may change per request.
func (h *indexesHandlers) resolveSearchableUpsert(class *models.Class, collection string, prop *models.Property, tok, algorithm string) (upsertPlan, error) {
	if tok != "" && algorithm != "" {
		return upsertPlan{}, errors.New("at most one configuration change per request: set either tokenization or algorithm, not both — issue two requests")
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
		// Already on blockmax → identical config → NO_OP.
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
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
		bucketStrategy, err := validateTokenizationChange(h.appState, class, collection, prop.Name, tok)
		if err != nil {
			return upsertPlan{}, err
		}
		return upsertPlan{migrationType: db.ReindexTypeChangeTokenization, targetTok: tok, bucketStrategy: bucketStrategy}, nil

	default:
		// Empty body: ensure the index exists with its current config.
		if !exists {
			// validateEnableSearchableProperty("") reports "not a text type"
			// for ineligible props and "tokenization required" for text ones.
			if err := validateEnableSearchableProperty(prop, ""); err != nil {
				return upsertPlan{}, err
			}
			return upsertPlan{}, errors.New("tokenization is required when creating a searchable index")
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
func resolveRebuildPlan(class *models.Class, prop *models.Property, indexType string) (upsertPlan, error) {
	switch indexType {
	case "searchable":
		if !searchableIndexOn(prop) {
			return upsertPlan{}, errors.New(db.NoSearchableIndexError(prop.Name))
		}
		// A WAND searchable index cannot be rebuilt — migrate to blockmax first.
		if class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.UsingBlockMaxWAND {
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

// submitReindexTask is the shared submit path for upsert and rebuild:
// validates tenant scope, checks for conflicts/cap, and submits the
// distributed task. Returns 202 STARTED or the mapped error.
func (h *indexesHandlers) submitReindexTask(ctx context.Context, principal *models.Principal, class *models.Class, collection, propertyName string, plan upsertPlan, tenants []string) middleware.Responder {
	if h.appState.ClusterService == nil {
		return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cluster service unavailable; cannot submit reindex task"))
	}

	migrationType := plan.migrationType
	properties := []string{propertyName}
	semantic := db.IsSemanticMigration(migrationType)
	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled

	// Tenant scope: only valid on MT collections, and only for format-only
	// (non-semantic) operations.
	if !isMT && len(tenants) > 0 {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal,
			"tenants parameter is only valid for multi-tenant collections"))
	}
	if semantic && len(tenants) > 0 {
		return jsonResponder(http.StatusBadRequest, errorResponse(principal,
			"tenants parameter cannot be used with semantic migrations; all tenants must be targeted"))
	}
	if isMT && len(tenants) > 0 {
		if err := validateTenants(h.appState.DB, ctx, collection, tenants); err != nil {
			return jsonResponder(http.StatusBadRequest, errorResponse(principal, err.Error()))
		}
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

	// Conflict + concurrency-cap checks. The submit lock held by the caller
	// serializes this check-and-submit against parallel DELETE and submits.
	// Fails closed (503) when the task list is unavailable — see
	// checkReindexAdmission.
	tasks, listErr := h.appState.ClusterService.ListDistributedTasks(ctx)
	if resp := h.checkReindexAdmission(principal, collection, migrationType, properties,
		tasks[db.ReindexNamespace], listErr); resp != nil {
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
