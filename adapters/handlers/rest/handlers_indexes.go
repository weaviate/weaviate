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
	"fmt"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

func setupIndexesHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := &indexesHandlers{appState: appState}
	api.SchemaSchemaObjectsIndexesGetHandler = schema.SchemaObjectsIndexesGetHandlerFunc(h.getIndexes)
	api.SchemaSchemaObjectsIndexesUpdateHandler = schema.SchemaObjectsIndexesUpdateHandlerFunc(h.updateIndex)
}

type indexesHandlers struct {
	appState *state.State
}

// getIndexes implements GET /v1/schema/{className}/indexes.
func (h *indexesHandlers) getIndexes(params schema.SchemaObjectsIndexesGetParams, _ *models.Principal) middleware.Responder {
	collection := params.ClassName

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

	// Build per-property index status.
	props := make([]*models.PropertyIndexStatus, 0, len(class.Properties))
	for _, prop := range class.Properties {
		pis := &models.PropertyIndexStatus{
			Name:     prop.Name,
			DataType: dataTypeString(prop),
		}
		pis.Description = prop.Description

		var indexes []*models.IndexStatus

		// Filterable index. If the schema flag is on we always emit the
		// entry; if it's off we emit a synthetic entry when a reindex
		// task carries actionable signal for the user (in-progress,
		// pending, failed, or cancelled).
		if prop.IndexFilterable == nil || *prop.IndexFilterable {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			idx.Tokenization = prop.Tokenization
			mergeReindexStatus(idx, collection, prop.Name, "filterable", activeTasks, h.appState.Logger)
			indexes = append(indexes, idx)
		} else {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, collection, prop.Name, "filterable", activeTasks, h.appState.Logger)
			if isSyntheticStatus(idx.Status) {
				indexes = append(indexes, idx)
			}
		}

		// Searchable index. Same pattern as filterable: show a synthetic
		// entry while enable-searchable is in flight or has failed/cancelled.
		if prop.IndexSearchable == nil || *prop.IndexSearchable {
			idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
			idx.Tokenization = prop.Tokenization
			mergeReindexStatus(idx, collection, prop.Name, "searchable", activeTasks, h.appState.Logger)
			indexes = append(indexes, idx)
		} else {
			idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
			mergeReindexStatus(idx, collection, prop.Name, "searchable", activeTasks, h.appState.Logger)
			if isSyntheticStatus(idx.Status) {
				indexes = append(indexes, idx)
			}
		}

		// Rangeable index.
		dt, ok := entschema.AsPrimitive(prop.DataType)
		isNumeric := ok && (dt == entschema.DataTypeInt || dt == entschema.DataTypeNumber || dt == entschema.DataTypeDate)
		if isNumeric {
			if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				idx := &models.IndexStatus{Type: "rangeable", Status: "ready"}
				mergeReindexStatus(idx, collection, prop.Name, "rangeable", activeTasks, h.appState.Logger)
				indexes = append(indexes, idx)
			} else {
				// Check if there's an active or recently-terminated
				// enable-rangeable task for this property.
				idx := &models.IndexStatus{Type: "rangeable", Status: "ready"}
				mergeReindexStatus(idx, collection, prop.Name, "rangeable", activeTasks, h.appState.Logger)
				if isSyntheticStatus(idx.Status) {
					indexes = append(indexes, idx)
				}
				// If not active and not enabled, don't show it.
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
func (h *indexesHandlers) updateIndex(params schema.SchemaObjectsIndexesUpdateParams, _ *models.Principal) middleware.Responder {
	collection := params.ClassName
	propertyName := params.PropertyName

	if !h.appState.ServerConfig.Config.DistributedTasks.Enabled {
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(
			"distributed tasks must be enabled for reindex (set DISTRIBUTED_TASKS_ENABLED=true)"))
	}

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
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse("request body required"))
	}

	// Reject ambiguous bodies (multiple groups set, conflicting verbs within
	// a group, or zero verbs) before the switch silently picks one arm.
	if err := validateBodyExclusivity(body); err != nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
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
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Tokenization != "":
		// Change tokenization on a property whose searchable index already
		// exists. If Enabled was also set it would have matched the case
		// above.
		migrationType = db.ReindexTypeChangeTokenization
		properties = []string{propertyName}
		targetTok = body.Searchable.Tokenization

		var err error
		bucketStrategy, err = validateTokenizationChange(h.appState, class, collection, propertyName, targetTok)
		if err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Rebuild:
		migrationType = db.ReindexTypeRepairSearchable
		properties = []string{propertyName}
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(
				fmt.Sprintf("property %q does not have a searchable index", propertyName)))
		}

	case body.Filterable != nil && body.Filterable.Enabled:
		migrationType = db.ReindexTypeEnableFilterable
		properties = []string{propertyName}
		if err := validateEnableFilterableProperty(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
		}

	case body.Filterable != nil && body.Filterable.Rebuild:
		migrationType = db.ReindexTypeRepairFilterable
		properties = []string{propertyName}
		if targetProp.IndexFilterable != nil && !*targetProp.IndexFilterable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(
				fmt.Sprintf("property %q does not have a filterable index", propertyName)))
		}

	case body.Rangeable != nil && body.Rangeable.Enabled:
		migrationType = db.ReindexTypeEnableRangeable
		properties = []string{propertyName}
		if err := validateRangeableProperties(class, properties); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
		}

	default:
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(
			"no actionable change detected; set one of: searchable.tokenization, searchable.rebuild, searchable.enabled, filterable.rebuild, filterable.enabled, rangeable.enabled"))
	}

	// --- Multi-tenancy handling ---
	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
	tenants := params.Tenants
	semantic := isSemanticMigration(migrationType)

	// Validate MT + tenants combination.
	if !isMT && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse("tenants parameter is only valid for multi-tenant collections"))
	}
	if semantic && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse("tenants parameter cannot be used with semantic migrations (change-tokenization); all tenants must be targeted"))
	}

	// For MT collections with specific tenants, validate they exist and are not OFFLOADED/FROZEN.
	if isMT && len(tenants) > 0 {
		if err := validateTenants(h.appState.DB, params.HTTPRequest.Context(), collection, tenants); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
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
			errorResponse(fmt.Sprintf("getting shard ownership: %v", err)))
	}
	if len(shardOwnership) == 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse("collection has no shards"))
	}

	unitIDs, unitToShard, unitToNode := buildUnitMaps(shardOwnership)

	payload := db.ReindexTaskPayload{
		MigrationType:      migrationType,
		Collection:         collection,
		Properties:         properties,
		TargetTokenization: targetTok,
		BucketStrategy:     bucketStrategy,
		Tenants:            tenants,
		UnitToNode:         unitToNode,
		UnitToShard:        unitToShard,
	}

	// Build a human-readable task ID with a random suffix for uniqueness.
	// Format: "Collection:migration-type:property:ab3f" (or without property for whole-collection ops).
	suffix := shortRandomSuffix()
	taskID := fmt.Sprintf("%s:%s:%s", collection, migrationType, suffix)
	if len(properties) > 0 {
		taskID = fmt.Sprintf("%s:%s:%s:%s", collection, migrationType, properties[0], suffix)
	}

	// Check for conflicting active tasks. Two tasks conflict if they would
	// touch the same bucket type for the same property.
	if h.appState.ClusterService != nil {
		tasks, err := h.appState.ClusterService.ListDistributedTasks(ctx)
		if err == nil {
			reason, checkErr := checkReindexConflict(collection, migrationType, properties, tasks[db.ReindexNamespace])
			if checkErr != nil {
				// An in-flight task has an unparseable payload — we cannot
				// prove the new submit doesn't conflict with it, so refuse
				// rather than race. Return 503 so the caller knows to retry
				// after an operator inspects the in-flight task.
				return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(checkErr.Error()))
			}
			if reason != "" {
				return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(errorResponse(reason))
			}
		}
	}

	// Submit the task. For MT semantic migrations, use grouped units so that
	// OnGroupCompleted fires per-tenant (giving per-tenant barrier semantics).
	if isMT && semantic {
		unitSpecs := buildUnitSpecs(shardOwnership)
		if err := h.appState.ClusterService.AddDistributedTaskWithGroups(
			ctx, db.ReindexNamespace, taskID, payload, unitSpecs,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(fmt.Sprintf("submitting task: %v", err)))
		}
	} else {
		if err := h.appState.ClusterService.AddDistributedTask(
			ctx, db.ReindexNamespace, taskID, payload, unitIDs,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(fmt.Sprintf("submitting task: %v", err)))
		}
	}

	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		TaskID: taskID,
		Status: "STARTED",
	})
}

// mergeReindexStatus checks if there's an active or recently-terminated
// reindex task that targets the given property+indexType and updates the
// IndexStatus accordingly.
//
// Status values produced (in addition to the caller-supplied default
// "ready"):
//
//   - "pending":    STARTED task, no unit progress yet.
//   - "indexing":   STARTED task, some unit progress.
//   - "failed":     latest matching task ended in FAILED.
//   - "cancelled":  latest matching task ended in CANCELLED.
//
// FINISHED tasks are skipped — once a reindex finishes, the schema flag
// flips and the regular "ready" entry takes over.
//
// Property matching is uniform across all migration types: every branch
// requires payload.Properties to be non-empty and to contain propName.
// Previously the repair-* branches treated an empty Properties list as
// "match all properties" (via the now-removed propertyMatches helper),
// while every other branch treated it as "match nothing" — so a single
// repair-searchable payload with an empty list would fan out a synthetic
// "indexing" entry to every searchable property in the collection. The
// current REST handler always populates Properties with exactly one
// entry, so the empty-means-all branch was unreachable from the API and
// only reachable via direct cluster payload authoring; we now reject
// empty Properties consistently.
//
// The logger is used to flag unknown migration types: a future ReindexType
// added without updating this switch would otherwise silently report "ready"
// for an in-flight task. Passing a nil logger is allowed (test callers may
// rely on this); the entry is still skipped, just without a log line.
func mergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, allTasks map[string][]*distributedtask.Task, logger logrus.FieldLogger) {
	if allTasks == nil {
		return
	}
	tasks := allTasks[db.ReindexNamespace]
	for _, task := range tasks {
		// Skip FINISHED tasks — handled by the schema-flag-on path.
		// STARTED / FAILED / CANCELLED each carry user-visible signal.
		if task.Status == distributedtask.TaskStatusFinished {
			continue
		}

		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		// Require a non-empty Properties list. The REST handler always
		// populates this with one entry; an empty list only happens via
		// direct cluster payload authoring and is treated as "match
		// nothing" so we never silently fan out a synthetic entry to
		// every property in the collection.
		if !containsStr(payload.Properties, propName) {
			continue
		}

		targets := false
		switch payload.MigrationType {
		case db.ReindexTypeRepairSearchable:
			targets = indexType == "searchable"
		case db.ReindexTypeRepairFilterable:
			targets = indexType == "filterable"
		case db.ReindexTypeEnableFilterable:
			targets = indexType == "filterable"
		case db.ReindexTypeEnableSearchable:
			targets = indexType == "searchable"
			if targets && payload.TargetTokenization != "" {
				idx.Tokenization = payload.TargetTokenization
			}
		case db.ReindexTypeEnableRangeable:
			targets = indexType == "rangeable"
		case db.ReindexTypeChangeTokenization:
			targets = indexType == "searchable" || indexType == "filterable"
			if targets && payload.TargetTokenization != "" {
				idx.TargetTokenization = payload.TargetTokenization
			}
		default:
			// Unknown migration type. A new ReindexType was added without
			// being mapped to a bucket here, which would silently report
			// "ready" for an in-flight task. Log loudly so this surfaces in
			// CI/staging before it hits production. targets stays false so
			// we fall through and leave the synthetic entry alone.
			if logger != nil {
				logger.WithFields(logrus.Fields{
					"migration_type": payload.MigrationType,
					"task_id":        task.ID,
					"collection":     collection,
				}).Error(fmt.Errorf("mergeReindexStatus: unknown migration type %q; index status may be stale", payload.MigrationType))
			}
		}

		if !targets {
			continue
		}

		// Terminal failure modes carry user-visible signal even though
		// no progress is being made. STARTED falls through to the
		// indexing/pending logic below; FINISHED was already skipped at
		// the top of the loop.
		switch task.Status {
		case distributedtask.TaskStatusFailed:
			idx.Status = "failed"
			idx.Progress = aggregateProgress(task)
			return
		case distributedtask.TaskStatusCancelled:
			idx.Status = "cancelled"
			idx.Progress = aggregateProgress(task)
			return
		case distributedtask.TaskStatusStarted, distributedtask.TaskStatusFinished:
			// handled outside this switch
		}

		// STARTED: report indexing / pending.
		progress := aggregateProgress(task)
		idx.Progress = progress
		if progress > 0 {
			idx.Status = "indexing"
		} else {
			idx.Status = "pending"
		}
		return
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

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func errorResponse(msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: msg},
		},
	}
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
		if task.Status != distributedtask.TaskStatusStarted {
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

		if conflict := typesConflict(newType, newProps, payload.MigrationType, payload.Properties); conflict != "" {
			return fmt.Sprintf("reindex task %q conflicts: %s", task.ID, conflict), nil
		}
	}
	return "", nil
}

// typesConflict returns a non-empty reason string if two migration types on
// the same collection would touch the same bucket type for overlapping
// properties.
func typesConflict(newType db.ReindexMigrationType, newProps []string,
	existType db.ReindexMigrationType, existProps []string,
) string {
	newSearchable := touchesSearchable(newType)
	newFilterable := touchesFilterable(newType)
	existSearchable := touchesSearchable(existType)
	existFilterable := touchesFilterable(existType)

	// No overlap in bucket types → no conflict.
	if (!newSearchable || !existSearchable) && (!newFilterable || !existFilterable) {
		return ""
	}

	// Both touch the same bucket type. Check property overlap.
	// Empty props means "all properties" for that type.
	if propsOverlap(newProps, existProps) {
		if newSearchable && existSearchable && newFilterable && existFilterable {
			return "both touch searchable and filterable indexes for overlapping properties"
		}
		if newSearchable && existSearchable {
			return "both touch searchable indexes for overlapping properties"
		}
		return "both touch filterable indexes for overlapping properties"
	}
	return ""
}

// touchesSearchable reports whether migration type t writes to the searchable
// bucket. Implemented as an exhaustive switch so that a newly-added
// ReindexMigrationType cannot silently be treated as "doesn't touch
// searchable" — the default case panics with a clear message, surfacing the
// gap on the first request that exercises the new type. This matters
// because typesConflict relies on these answers to gate concurrent reindex
// submissions: a positive-list miss would allow conflicting writes to the
// same bucket through.
func touchesSearchable(t db.ReindexMigrationType) bool {
	switch t {
	case db.ReindexTypeRepairSearchable,
		db.ReindexTypeChangeTokenization,
		db.ReindexTypeEnableSearchable:
		return true
	case db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableFilterable,
		db.ReindexTypeEnableRangeable:
		return false
	default:
		panic(fmt.Sprintf("touchesSearchable: unknown ReindexMigrationType %q — add it to this switch", t))
	}
}

// touchesFilterable reports whether migration type t writes to the filterable
// bucket. Same exhaustive-switch contract as touchesSearchable: a new
// ReindexMigrationType must be added here so the conflict checker can reason
// about it; otherwise we panic loudly rather than allow a silent conflict.
func touchesFilterable(t db.ReindexMigrationType) bool {
	switch t {
	case db.ReindexTypeRepairFilterable,
		db.ReindexTypeChangeTokenization,
		db.ReindexTypeEnableFilterable:
		return true
	case db.ReindexTypeRepairSearchable,
		db.ReindexTypeEnableSearchable,
		db.ReindexTypeEnableRangeable:
		return false
	default:
		panic(fmt.Sprintf("touchesFilterable: unknown ReindexMigrationType %q — add it to this switch", t))
	}
}

// propsOverlap returns true if two property sets overlap. An empty set means
// "all properties", which overlaps with everything.
func propsOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true // one of them targets all properties
	}
	for _, ap := range a {
		for _, bp := range b {
			if ap == bp {
				return true
			}
		}
	}
	return false
}
