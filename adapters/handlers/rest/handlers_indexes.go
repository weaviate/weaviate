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

		// Filterable index.
		if prop.IndexFilterable == nil || *prop.IndexFilterable {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			idx.Tokenization = prop.Tokenization
			mergeReindexStatus(idx, collection, prop.Name, "filterable", activeTasks)
			indexes = append(indexes, idx)
		}

		// Searchable index.
		if prop.IndexSearchable == nil || *prop.IndexSearchable {
			idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
			idx.Tokenization = prop.Tokenization
			mergeReindexStatus(idx, collection, prop.Name, "searchable", activeTasks)
			indexes = append(indexes, idx)
		}

		// Rangeable index.
		dt, ok := entschema.AsPrimitive(prop.DataType)
		isNumeric := ok && (dt == entschema.DataTypeInt || dt == entschema.DataTypeNumber || dt == entschema.DataTypeDate)
		if isNumeric {
			if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
				idx := &models.IndexStatus{Type: "rangeable", Status: "ready"}
				mergeReindexStatus(idx, collection, prop.Name, "rangeable", activeTasks)
				indexes = append(indexes, idx)
			} else {
				// Check if there's an active enable-rangeable task for this property.
				idx := &models.IndexStatus{Type: "rangeable", Status: "ready"}
				mergeReindexStatus(idx, collection, prop.Name, "rangeable", activeTasks)
				if idx.Status == "indexing" || idx.Status == "pending" {
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

	// Determine which migration type to submit based on the diff.
	var (
		migrationType  db.ReindexMigrationType
		properties     []string
		targetTok      string
		bucketStrategy string
	)

	switch {
	case body.Searchable != nil && body.Searchable.Tokenization != "":
		// Change tokenization.
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

	case body.Filterable != nil && body.Filterable.Rebuild:
		migrationType = db.ReindexTypeRepairFilterable

	case body.Rangeable != nil && body.Rangeable.Enabled:
		migrationType = db.ReindexTypeEnableRangeable
		properties = []string{propertyName}
		if err := validateRangeableProperties(class, properties); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(err.Error()))
		}

	default:
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(
			"no actionable change detected; set one of: searchable.tokenization, searchable.rebuild, filterable.rebuild, rangeable.enabled"))
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
			if reason := checkReindexConflict(collection, migrationType, properties, tasks[db.ReindexNamespace]); reason != "" {
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

// mergeReindexStatus checks if there's an active reindex task that targets
// the given property+indexType and updates the IndexStatus accordingly.
func mergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, allTasks map[string][]*distributedtask.Task) {
	if allTasks == nil {
		return
	}
	tasks := allTasks[db.ReindexNamespace]
	for _, task := range tasks {
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

		targets := false
		switch payload.MigrationType {
		case db.ReindexTypeRepairSearchable:
			targets = indexType == "searchable"
		case db.ReindexTypeRepairFilterable:
			targets = indexType == "filterable"
		case db.ReindexTypeEnableRangeable:
			targets = indexType == "rangeable" && containsStr(payload.Properties, propName)
		case db.ReindexTypeChangeTokenization:
			targets = (indexType == "searchable" || indexType == "filterable") &&
				containsStr(payload.Properties, propName)
			if targets && payload.TargetTokenization != "" {
				idx.TargetTokenization = payload.TargetTokenization
			}
		}

		if !targets {
			continue
		}

		// Compute aggregate progress across all units.
		var totalProgress float32
		var unitCount int
		for _, unit := range task.Units {
			unitCount++
			totalProgress += unit.Progress
		}
		if unitCount > 0 {
			progress := totalProgress / float32(unitCount)
			idx.Progress = float32(progress)
			if progress > 0 {
				idx.Status = "indexing"
			} else {
				idx.Status = "pending"
			}
		} else {
			idx.Status = "pending"
		}
		return
	}
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
// running tasks. Returns an empty string if no conflict, or a human-readable
// reason if a conflict is detected.
//
// Two tasks conflict if they touch the same index bucket type for the same
// property. The bucket types each migration touches:
//   - repair-searchable:    searchable buckets (ALL properties)
//   - repair-filterable:    filterable buckets (ALL properties)
//   - change-tokenization:  searchable + filterable buckets (specified property)
//   - enable-rangeable:     rangeable buckets (specified property) — no cross-type conflicts
func checkReindexConflict(collection string, newType db.ReindexMigrationType,
	newProps []string, tasks []*distributedtask.Task,
) string {
	for _, task := range tasks {
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

		if conflict := typesConflict(newType, newProps, payload.MigrationType, payload.Properties); conflict != "" {
			return fmt.Sprintf("reindex task %q conflicts: %s", task.ID, conflict)
		}
	}
	return ""
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

func touchesSearchable(t db.ReindexMigrationType) bool {
	return t == db.ReindexTypeRepairSearchable || t == db.ReindexTypeChangeTokenization
}

func touchesFilterable(t db.ReindexMigrationType) bool {
	return t == db.ReindexTypeRepairFilterable || t == db.ReindexTypeChangeTokenization
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
