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

package db

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

var _ distributedtask.CompletedTaskRetainer = (*ReindexProvider)(nil)

// ReindexIndexTypes reports which index types a migration type affects;
// known is false for a migration type this build doesn't recognize.
func ReindexIndexTypes(mt ReindexMigrationType) (indexTypes []string, known bool) {
	switch mt {
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable:
		return []string{"searchable"}, true
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable, ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}, true
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}, true
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}, true
	}
	return nil, false
}

// ShouldRetainCompletedTask keeps a FAILED task's record until newer runs on
// the same property cover every index type it reports on; without it the
// status endpoint reports "ready". Such a newer run is kept while that failure
// is, so the failure does not resurface on the index type the run fixed.
func (p *ReindexProvider) ShouldRetainCompletedTask(task *distributedtask.Task,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	var payload ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return false // the status endpoint cannot read it either
	}
	newestOn := indexTypesNotRunSince(task, payload, namespaceTasks)
	if len(newestOn) == 0 {
		return false
	}
	if task.Status == distributedtask.TaskStatusFailed {
		return true
	}
	for _, older := range namespaceTasks {
		if older.Version >= task.Version || older.Status != distributedtask.TaskStatusFailed {
			continue
		}
		var olderPayload ReindexTaskPayload
		if err := json.Unmarshal(older.Payload, &olderPayload); err != nil {
			continue
		}
		olderIndexTypes, _ := ReindexIndexTypes(olderPayload.MigrationType)
		hidesOlderFailure := slices.ContainsFunc(indexTypesRunOnSameProperties(task, olderPayload), func(indexType string) bool {
			return slices.Contains(newestOn, indexType) && slices.Contains(olderIndexTypes, indexType)
		})
		if hidesOlderFailure && len(indexTypesNotRunSince(older, olderPayload, namespaceTasks)) > 0 {
			return true
		}
	}
	return false
}

// indexTypesNotRunSince returns task's index types that no newer run on the
// same properties has covered.
func indexTypesNotRunSince(task *distributedtask.Task, payload ReindexTaskPayload,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) []string {
	remaining, _ := ReindexIndexTypes(payload.MigrationType)
	for _, other := range namespaceTasks {
		if other.Version <= task.Version {
			continue
		}
		covered := indexTypesRunOnSameProperties(other, payload)
		remaining = slices.DeleteFunc(remaining, func(indexType string) bool {
			return slices.Contains(covered, indexType)
		})
	}
	return remaining
}

// indexTypesRunOnSameProperties returns task's index types, or nil unless
// task covers payload's collection and every one of its properties.
func indexTypesRunOnSameProperties(task *distributedtask.Task, payload ReindexTaskPayload) []string {
	var other ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &other); err != nil {
		return nil
	}
	if !strings.EqualFold(other.Collection, payload.Collection) {
		return nil
	}
	for _, prop := range payload.Properties {
		if !slices.Contains(other.Properties, prop) {
			return nil
		}
	}
	indexTypes, _ := ReindexIndexTypes(other.MigrationType)
	return indexTypes
}
