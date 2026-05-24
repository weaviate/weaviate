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

package reindex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// Service orchestrates the reindex submit / cancel lifecycle. HTTP
// handlers should authorize, parse params, call into the Service, and
// map [errors.Is]-matched sentinels in [errors.go] to HTTP status
// codes. No business logic should live in the handler itself.
type Service struct {
	deps   Deps
	logger logrus.FieldLogger
}

// New constructs a Service. SubmitLocks may be supplied via deps for
// shared lock identity across multiple handler instances; if nil, a
// no-op locker is used (tests).
func New(deps Deps, logger logrus.FieldLogger) *Service {
	if deps.SubmitLocks == nil {
		deps.SubmitLocks = noopLocker{}
	}
	return &Service{deps: deps, logger: logger}
}

// SubmitRequest is the parsed-and-authorized intent for one
// PUT /v1/schema/{class}/indexes/{prop}. The handler builds this from
// HTTP params + body and hands it to the service.
type SubmitRequest struct {
	Collection   string
	PropertyName string
	Body         *models.IndexUpdateRequest
	Tenants      []string
	// Principal is opaque to the service — kept only so the audit log
	// can record who submitted.
	PrincipalUsername string
}

// SubmitResult is the service-level outcome surfaced back to the
// handler. TaskID is the DTM task identifier; Status is "STARTED" on
// successful submit, "CANCELLED" on a cancel verb.
type SubmitResult struct {
	TaskID string
	Status string
}

// Submit validates the request, checks for conflicts, builds the
// payload, runs pre-submit cleanup, and submits the distributed task.
// Returns the task ID on success or a sentinel error (see errors.go)
// the handler maps to an HTTP status.
//
// Cancel verbs are routed to [Service.Cancel] internally so handlers
// can call a single entry point per HTTP method.
func (s *Service) Submit(ctx context.Context, req SubmitRequest) (SubmitResult, error) {
	body := req.Body
	if err := ValidateBodyExclusivity(body); err != nil {
		return SubmitResult{}, errors.Join(ErrBadRequest, err)
	}

	if cancelIndexType, cancelling := RequestedCancel(body); cancelling {
		return s.Cancel(ctx, req.Collection, req.PropertyName, cancelIndexType, req.PrincipalUsername)
	}

	propLock := s.deps.SubmitLocks.SubmitLockFor(req.Collection, req.PropertyName)
	propLock.Lock()
	defer propLock.Unlock()

	class := s.deps.SchemaManager.ReadOnlyClass(req.Collection)
	if class == nil {
		return SubmitResult{}, fmt.Errorf("%w: collection %q", ErrNotFound, req.Collection)
	}
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == req.PropertyName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return SubmitResult{}, fmt.Errorf("%w: property %q", ErrNotFound, req.PropertyName)
	}

	migrationType, properties, targetTok, bucketStrategy, err := s.dispatchMigration(body, class, req.Collection, req.PropertyName, targetProp)
	if err != nil {
		return SubmitResult{}, err
	}

	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
	semantic := dbreindex.IsSemanticMigration(migrationType)

	if !isMT && len(req.Tenants) > 0 {
		return SubmitResult{}, fmt.Errorf("%w: tenants parameter is only valid for multi-tenant collections", ErrBadRequest)
	}
	if semantic && len(req.Tenants) > 0 {
		return SubmitResult{}, fmt.Errorf("%w: tenants parameter cannot be used with semantic migrations (change-tokenization); all tenants must be targeted", ErrBadRequest)
	}

	if isMT && len(req.Tenants) > 0 {
		if err := ValidateTenants(ctx, s.deps.DB, req.Collection, req.Tenants); err != nil {
			return SubmitResult{}, errors.Join(ErrBadRequest, err)
		}
	}

	var shardOwnership map[string][]string
	if isMT {
		shardOwnership, err = s.deps.DB.ShardReplicaOwnershipForMT(ctx, req.Collection, req.Tenants)
	} else {
		shardOwnership, err = s.deps.DB.ShardReplicaOwnership(ctx, req.Collection)
	}
	if err != nil {
		return SubmitResult{}, fmt.Errorf("getting shard ownership: %w", err)
	}
	if len(shardOwnership) == 0 {
		return SubmitResult{}, fmt.Errorf("%w: collection has no shards", ErrBadRequest)
	}

	unitIDs, unitToShard, unitToNode := BuildUnitMaps(shardOwnership)

	// Capture the property's tokenization at submit-time so a
	// post-restart FSM-replay of an older task can't override a newer
	// task's already-applied schema flip. See OriginalTokenization on
	// ReindexTaskPayload for the full rationale.
	var originalTok string
	if migrationType == dbreindex.ReindexTypeChangeTokenization ||
		migrationType == dbreindex.ReindexTypeChangeTokenizationFilterable ||
		migrationType == dbreindex.ReindexTypeEnableSearchable {
		originalTok = targetProp.Tokenization
	}

	payload := dbreindex.ReindexTaskPayload{
		MigrationType:        migrationType,
		Collection:           req.Collection,
		Properties:           properties,
		TargetTokenization:   targetTok,
		OriginalTokenization: originalTok,
		BucketStrategy:       bucketStrategy,
		Tenants:              req.Tenants,
		UnitToNode:           unitToNode,
		UnitToShard:          unitToShard,
	}

	// Human-readable task ID: "Collection:migration-type:property:ab3f".
	suffix := shortRandomSuffix()
	taskID := fmt.Sprintf("%s:%s:%s", req.Collection, migrationType, suffix)
	if len(properties) > 0 {
		taskID = fmt.Sprintf("%s:%s:%s:%s", req.Collection, migrationType, properties[0], suffix)
	}

	if s.deps.Cluster == nil {
		return SubmitResult{}, fmt.Errorf("%w: cluster service unavailable", ErrServiceUnavailable)
	}

	tasks, listErr := s.deps.Cluster.ListDistributedTasks(ctx)
	if listErr == nil {
		reason, checkErr := CheckReindexConflict(req.Collection, migrationType, properties, tasks[dbreindex.ReindexNamespace])
		if checkErr != nil {
			return SubmitResult{}, errors.Join(ErrServiceUnavailable, checkErr)
		}
		if reason != "" {
			return SubmitResult{}, fmt.Errorf("%w: %s", ErrConflict, reason)
		}
		// Per-collection cap on concurrent in-flight reindex tasks.
		if inflight := CountStartedTasksForCollection(req.Collection, tasks[dbreindex.ReindexNamespace]); inflight >= MaxConcurrentReindexPerCollection {
			return SubmitResult{}, fmt.Errorf("%w: collection %q already has %d concurrent reindex tasks (max %d); wait for one to finish before submitting another",
				ErrServiceUnavailable, req.Collection, inflight, MaxConcurrentReindexPerCollection)
		}
	}

	// Defense in depth against the CANCEL→retry silent failure
	// (same Sev 1 family as DELETE→re-enable): if a previous
	// cancelled run left stale .migrations/<dir>/started.mig +
	// __reindex/__ingest sidecars on disk, the new task would resume
	// against them and finish in <1s with a 50-entry no-op, flipping
	// the schema flag while reporting success against an empty bucket.
	if indexTypesForCleanup, indexTypeKnown := IndexTypesFromMigrationType(migrationType); indexTypeKnown {
		for _, it := range indexTypesForCleanup {
			if err := s.deps.DB.CleanStalePartialReindexState(ctx, req.Collection, req.PropertyName, it); err != nil {
				s.logger.WithFields(logrus.Fields{
					"collection":     req.Collection,
					"property":       req.PropertyName,
					"migration_type": migrationType,
					"index_type":     it,
				}).Errorf("submit: pre-submit cleanup of stale partial reindex state failed: %v; the new task may short-circuit on the stale state and report a false success — operator inspection recommended", err)
			}
		}
	}

	if isMT && semantic {
		unitSpecs := BuildUnitSpecs(shardOwnership)
		if err := s.deps.Cluster.AddDistributedTaskWithGroupsBarrier(
			ctx, dbreindex.ReindexNamespace, taskID, payload, unitSpecs, semantic,
		); err != nil {
			return SubmitResult{}, fmt.Errorf("submitting task: %w", err)
		}
	} else {
		if err := s.deps.Cluster.AddDistributedTaskWithBarrier(
			ctx, dbreindex.ReindexNamespace, taskID, payload, unitIDs, semantic,
		); err != nil {
			return SubmitResult{}, fmt.Errorf("submitting task: %w", err)
		}
	}

	s.logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_submitted",
		"taskID":         taskID,
		"collection":     req.Collection,
		"property":       req.PropertyName,
		"migration_type": migrationType,
		"principal":      req.PrincipalUsername,
	}).Info("reindex provider: submitted task")

	return SubmitResult{TaskID: taskID, Status: "STARTED"}, nil
}

// dispatchMigration translates the request body into the migration
// type + properties + tokenization tuple that the rest of Submit
// passes to the distributed-task layer.
func (s *Service) dispatchMigration(
	body *models.IndexUpdateRequest,
	class *models.Class,
	collection, propertyName string,
	targetProp *models.Property,
) (migrationType dbreindex.ReindexMigrationType, properties []string, targetTok, bucketStrategy string, err error) {
	switch {
	// enable-searchable must be matched BEFORE change-tokenization: an
	// enable request carries tokenization in the same body, but a
	// property that has no searchable index yet cannot have its
	// tokenization "changed" — ValidateTokenizationChange would fail
	// looking for a non-existent searchable bucket.
	case body.Searchable != nil && body.Searchable.Enabled:
		targetTok = body.Searchable.Tokenization
		if err := ValidateEnableSearchableProperty(targetProp, targetTok); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeEnableSearchable, []string{propertyName}, targetTok, "", nil

	case body.Searchable != nil && body.Searchable.Tokenization != "":
		targetTok = body.Searchable.Tokenization
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return "", nil, "", "", fmt.Errorf("%w: property %q has no searchable index; use {\"filterable\":{\"tokenization\":...}} to retokenize the filterable bucket, or {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add a searchable index",
				ErrBadRequest, propertyName)
		}
		bucketStrategy, err := ValidateTokenizationChange(s.deps.DB, class, collection, propertyName, targetTok)
		if err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeChangeTokenization, []string{propertyName}, targetTok, bucketStrategy, nil

	case body.Filterable != nil && body.Filterable.Tokenization != "":
		targetTok = body.Filterable.Tokenization
		if err := ValidateFilterableTokenizationChange(targetProp, targetTok); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeChangeTokenizationFilterable, []string{propertyName}, targetTok, "", nil

	case body.Searchable != nil && body.Searchable.Rebuild:
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return "", nil, "", "", fmt.Errorf("%w: property %q does not have a searchable index", ErrBadRequest, propertyName)
		}
		if class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.UsingBlockMaxWAND {
			return "", nil, "", "", fmt.Errorf("%w: cannot rebuild a WAND searchable index — WAND is deprecated; use {\"searchable\":{\"algorithm\":\"blockmax\"}} to migrate first", ErrBadRequest)
		}
		return dbreindex.ReindexTypeRebuildSearchable, []string{propertyName}, "", "", nil

	case body.Searchable != nil && body.Searchable.Algorithm != "":
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return "", nil, "", "", fmt.Errorf("%w: property %q does not have a searchable index", ErrBadRequest, propertyName)
		}
		if normalised := NormalizeSearchableAlgorithm(body.Searchable.Algorithm); normalised == "" {
			return "", nil, "", "", fmt.Errorf("%w: unsupported algorithm %q; only %q is accepted (WAND is deprecated)",
				ErrBadRequest, body.Searchable.Algorithm, models.IndexStatusAlgorithmBlockmax)
		}
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return "", nil, "", "", fmt.Errorf("%w: searchable index is already on blockmax", ErrBadRequest)
		}
		return dbreindex.ReindexTypeChangeAlgorithm, []string{propertyName}, "", "", nil

	case body.Filterable != nil && body.Filterable.Enabled:
		if err := ValidateEnableFilterableProperty(targetProp); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeEnableFilterable, []string{propertyName}, "", "", nil

	case body.Filterable != nil && body.Filterable.Rebuild:
		if targetProp.IndexFilterable != nil && !*targetProp.IndexFilterable {
			return "", nil, "", "", fmt.Errorf("%w: property %q does not have a filterable index", ErrBadRequest, propertyName)
		}
		if err := ValidateRebuildFilterableDataType(targetProp); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeRepairFilterable, []string{propertyName}, "", "", nil

	case body.Rangeable != nil && body.Rangeable.Enabled:
		props := []string{propertyName}
		if err := ValidateRangeableProperties(class, props); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeEnableRangeable, props, "", "", nil

	case body.Rangeable != nil && body.Rangeable.Rebuild:
		if err := ValidateRebuildRangeableProperty(targetProp); err != nil {
			return "", nil, "", "", errors.Join(ErrBadRequest, err)
		}
		return dbreindex.ReindexTypeRepairRangeable, []string{propertyName}, "", "", nil
	}

	// Should be unreachable: ValidateBodyExclusivity rejects empty
	// bodies. Defensive — the verb list mirrors the one in
	// ValidateBodyExclusivity.
	return "", nil, "", "", fmt.Errorf("%w: no actionable change detected", ErrBadRequest)
}

// reindexCancelDrainTimeout caps how long Cancel waits for the local
// reindex goroutine to exit before falling back to "let the next
// submit clean up".
const reindexCancelDrainTimeout = 10 * time.Second

// Cancel finds the in-flight reindex task targeting (collection,
// propertyName, indexType), asks DTM to cancel it, drains the local
// worker, and wipes the on-disk partial state.
func (s *Service) Cancel(ctx context.Context, collection, propertyName, indexType, principalUsername string) (SubmitResult, error) {
	if s.deps.Cluster == nil {
		return SubmitResult{}, fmt.Errorf("%w: cluster service unavailable; cannot cancel reindex task", ErrServiceUnavailable)
	}

	tasks, err := s.deps.Cluster.ListDistributedTasks(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("listing tasks: %w", err)
	}

	var target *distributedtask.Task
	var targetPayload dbreindex.ReindexTaskPayload
	for _, task := range tasks[dbreindex.ReindexNamespace] {
		if task.Status != distributedtask.TaskStatusStarted {
			continue
		}
		var payload dbreindex.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}
		if !slices.Contains(payload.Properties, propertyName) {
			continue
		}
		if matches, _ := MigrationTypeTargetsIndex(payload.MigrationType, indexType); !matches {
			continue
		}
		target = task
		targetPayload = payload
		break
	}

	if target == nil {
		// Pin: TestBackupVsReindexSuite/CancelOnNoInFlightReturnsStructured404
		// requires the structured 404 body to name the tuple AND point
		// operators at GET /v1/schema/{collection}/indexes — bare 404
		// is indistinguishable from "endpoint not found" for an operator
		// chasing a stuck-state cancel.
		return SubmitResult{}, fmt.Errorf("%w: no in-flight reindex task to cancel for (collection=%q, property=%q, indexType=%q); the task may have already finished, been canceled, or never been started; use GET /v1/schema/%s/indexes to inspect the current state",
			ErrNotFound, collection, propertyName, indexType, collection)
	}

	if err := s.deps.Cluster.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		return SubmitResult{}, fmt.Errorf("cancelling task: %w", err)
	}

	// Drain the local goroutine BEFORE cleaning partial state.
	// Without this, the cleanup races against the worker which is
	// still writing to __reindex / __ingest buckets.
	if s.deps.Provider != nil {
		s.logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Info("cancel: starting drain+cleanup for cancelled reindex task")
		drainCtx, drainCancel := context.WithTimeout(ctx, reindexCancelDrainTimeout)
		drainErr := s.deps.Provider.WaitForLocalTaskDrain(drainCtx, target.TaskDescriptor)
		drainCancel()
		if drainErr != nil {
			s.logger.WithFields(logrus.Fields{
				"taskID":     target.ID,
				"collection": collection,
				"property":   propertyName,
				"index_type": indexType,
			}).Errorf("cancel: timed out waiting for local reindex goroutine to drain (%v); skipping inline cleanup — next submit will retry", drainErr)
		} else {
			indexTypesToClean, known := IndexTypesFromMigrationType(targetPayload.MigrationType)
			if !known || len(indexTypesToClean) == 0 {
				indexTypesToClean = []string{indexType}
			}
			var cleanupErrs []error
			for _, it := range indexTypesToClean {
				if err := s.deps.DB.CleanStalePartialReindexState(ctx, collection, propertyName, it); err != nil {
					cleanupErrs = append(cleanupErrs, fmt.Errorf("indexType=%q: %w", it, err))
				}
			}
			if len(cleanupErrs) > 0 {
				s.logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
					"strategies": indexTypesToClean,
				}).Errorf("cancel: cleaning partial reindex state on disk for %d strategies failed: %v; next submit's defense-in-depth cleanup will retry", len(cleanupErrs), cleanupErrs)
			} else {
				s.logger.WithFields(logrus.Fields{
					"taskID":     target.ID,
					"collection": collection,
					"property":   propertyName,
					"index_type": indexType,
				}).Info("cancel: on-disk cleanup complete")
			}
		}
	} else {
		s.logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Warn("cancel: reindex provider not wired up; skipping drain+cleanup")
	}

	s.logger.WithFields(logrus.Fields{
		"audit_event": "reindex_task_cancelled",
		"taskID":      target.ID,
		"collection":  collection,
		"property":    propertyName,
		"index_type":  indexType,
		"principal":   principalUsername,
	}).Info("reindex provider: cancelled task")

	return SubmitResult{TaskID: target.ID, Status: "CANCELLED"}, nil
}

// shortRandomSuffix returns four hex characters used to deduplicate
// otherwise identical task IDs submitted back-to-back.
func shortRandomSuffix() string {
	b := make([]byte, 2)
	if _, err := rand.Read(b); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b)
}
