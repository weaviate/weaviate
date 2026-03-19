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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// setupReindexHandler registers the POST /v1/schema/{collection}/reindex endpoint.
// This is wired via raw http.HandleFunc (not go-swagger) as a temporary measure
// until the endpoint is added to the OpenAPI spec.
func setupReindexHandler(appState *state.State) {
	http.HandleFunc("/v1/schema/", func(w http.ResponseWriter, r *http.Request) {
		// Only match POST /v1/schema/{collection}/reindex
		if r.Method != http.MethodPost {
			return // let other handlers deal with it
		}

		path := strings.TrimPrefix(r.URL.Path, "/v1/schema/")
		if !strings.HasSuffix(path, "/reindex") {
			return // not our endpoint
		}
		collection := strings.TrimSuffix(path, "/reindex")
		if collection == "" || strings.Contains(collection, "/") {
			return // invalid path
		}

		handleReindex(w, r, appState, collection)
	})
}

type reindexRequest struct {
	Type               string   `json:"type"`
	Properties         []string `json:"properties,omitempty"`
	TargetTokenization string   `json:"targetTokenization,omitempty"`
}

func handleReindex(w http.ResponseWriter, r *http.Request, appState *state.State, collection string) {
	if !appState.ServerConfig.Config.DistributedTasks.Enabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "distributed tasks must be enabled for reindex (set DISTRIBUTED_TASKS_ENABLED=true)",
		})
		return
	}

	var req reindexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	migrationType := db.ReindexMigrationType(req.Type)
	switch migrationType {
	case db.ReindexTypeRepairSearchable, db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableRangeable, db.ReindexTypeChangeTokenization:
		// valid
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("invalid type %q, must be one of: repair-searchable, repair-filterable, enable-rangeable, change-tokenization", req.Type),
		})
		return
	}

	// Validate collection exists.
	class := appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("collection %q not found", collection)})
		return
	}

	// Type-specific validation.
	var bucketStrategy string
	switch migrationType {
	case db.ReindexTypeRepairSearchable, db.ReindexTypeRepairFilterable:
		// No additional validation needed.

	case db.ReindexTypeEnableRangeable:
		if len(req.Properties) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "enable-rangeable requires at least one property"})
			return
		}
		if err := validateRangeableProperties(class, req.Properties); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

	case db.ReindexTypeChangeTokenization:
		if len(req.Properties) != 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "change-tokenization requires exactly one property"})
			return
		}
		if req.TargetTokenization == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "change-tokenization requires targetTokenization"})
			return
		}
		var err error
		bucketStrategy, err = validateTokenizationChange(appState, class, collection, req.Properties[0], req.TargetTokenization)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}

	// Build unit maps from shard placement.
	shardOwnership, err := appState.DB.ShardOwnership(r.Context(), collection)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("getting shard ownership: %v", err)})
		return
	}
	if len(shardOwnership) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "collection has no shards"})
		return
	}

	unitIDs, unitToShard, unitToNode := buildUnitMaps(shardOwnership)

	// Build payload.
	payload := db.ReindexTaskPayload{
		MigrationType:      migrationType,
		Collection:         collection,
		Properties:         req.Properties,
		TargetTokenization: req.TargetTokenization,
		BucketStrategy:     bucketStrategy,
		UnitToNode:         unitToNode,
		UnitToShard:        unitToShard,
	}

	// Task ID: "collection:migrationType"
	taskID := fmt.Sprintf("%s:%s", collection, migrationType)

	// Submit via Raft.
	if err := appState.ClusterService.AddDistributedTask(
		r.Context(), db.ReindexNamespace, taskID, payload, unitIDs,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("submitting task: %v", err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"taskId": taskID,
		"status": "STARTED",
	})
}

// buildUnitMaps creates per-replica unit IDs and maps from shard ownership.
// ShardOwnership returns map[nodeName][]shardName (node→shards it owns).
// Unit ID format: "shardName__nodeName".
func buildUnitMaps(shardOwnership map[string][]string) (unitIDs []string, unitToShard, unitToNode map[string]string) {
	unitToShard = make(map[string]string)
	unitToNode = make(map[string]string)

	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			unitIDs = append(unitIDs, unitID)
			unitToShard[unitID] = shardName
			unitToNode[unitID] = nodeName
		}
	}
	sort.Strings(unitIDs)
	return unitIDs, unitToShard, unitToNode
}

func validateRangeableProperties(class *models.Class, propNames []string) error {
	propsByName := make(map[string]*models.Property, len(class.Properties))
	for _, p := range class.Properties {
		propsByName[p.Name] = p
	}

	for _, pn := range propNames {
		prop, ok := propsByName[pn]
		if !ok {
			return fmt.Errorf("property %q not found", pn)
		}
		dt, ok := entschema.AsPrimitive(prop.DataType)
		if !ok || (dt != entschema.DataTypeInt && dt != entschema.DataTypeNumber && dt != entschema.DataTypeDate) {
			return fmt.Errorf("property %q is not a numeric type (int, number, date)", pn)
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			return fmt.Errorf("property %q already has indexRangeFilters enabled", pn)
		}
	}
	return nil
}

func validateTokenizationChange(
	appState *state.State,
	class *models.Class,
	collection, propName, targetTokenization string,
) (bucketStrategy string, err error) {
	// Find the property.
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == propName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return "", fmt.Errorf("property %q not found", propName)
	}

	dt, ok := entschema.AsPrimitive(targetProp.DataType)
	if !ok || (dt != entschema.DataTypeText && dt != entschema.DataTypeTextArray) {
		return "", fmt.Errorf("property %q is not a text type", propName)
	}

	// Validate target tokenization.
	validTokenizations := map[string]struct{}{
		models.PropertyTokenizationWord:       {},
		models.PropertyTokenizationLowercase:  {},
		models.PropertyTokenizationWhitespace: {},
		models.PropertyTokenizationField:      {},
		models.PropertyTokenizationTrigram:    {},
		models.PropertyTokenizationGse:        {},
		models.PropertyTokenizationKagomeKr:   {},
		models.PropertyTokenizationKagomeJa:   {},
		models.PropertyTokenizationGseCh:      {},
	}
	if _, ok := validTokenizations[targetTokenization]; !ok {
		return "", fmt.Errorf("invalid tokenization %q", targetTokenization)
	}

	if targetProp.Tokenization == targetTokenization {
		return "", fmt.Errorf("property %q already uses tokenization %q", propName, targetTokenization)
	}

	// Detect bucket strategy from the first shard's searchable bucket.
	className := entschema.ClassName(collection)
	idx := appState.DB.GetIndex(className)
	if idx == nil {
		return "", fmt.Errorf("collection index not found")
	}

	idx.ForEachShard(func(_ string, shard db.ShardLike) error {
		if bucketStrategy != "" {
			return nil
		}
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard.Store().Bucket(bucketName)
		if bucket != nil {
			bucketStrategy = bucket.Strategy()
		}
		return nil
	})
	if bucketStrategy == "" {
		return "", fmt.Errorf("searchable bucket not found for property %q", propName)
	}

	return bucketStrategy, nil
}
