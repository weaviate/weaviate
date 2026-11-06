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
	"github.com/weaviate/weaviate/entities/models"
)

var _ distributedtask.CompletedTaskRetainer = (*ReindexProvider)(nil)

// ReindexIndexTypes reports which index types a migration type affects.
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

// ShouldRetainCompletedTask keeps a FAILED record until newer runs on its
// property cover all its index types, so the status endpoint still reads
// "failed". A newer run that hides the failure is kept until the failed record
// is gone, so the failure cannot resurface. "failed" lasts until a newer run of
// that index type (even a cancelled one), a collection delete, or, for a
// non-enable run, dropping the index.
// Assumes one property per payload, as every submit path builds today.
func (p *ReindexProvider) ShouldRetainCompletedTask(task *distributedtask.Task,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	targets := reindexTargets{}
	target := targets.of(task)
	if target == nil {
		return false // the status endpoint cannot read it either
	}
	newestOn := p.indexTypesNotRunSince(task, target, namespaceTasks, targets)
	if len(newestOn) == 0 {
		return false
	}
	if task.Status == distributedtask.TaskStatusFailed {
		return slices.ContainsFunc(newestOn, func(indexType string) bool {
			return !p.indexDroppedSince(target, indexType)
		})
	}
	for _, older := range namespaceTasks {
		if older.Version >= task.Version || older.Status != distributedtask.TaskStatusFailed {
			continue
		}
		olderTarget := targets.of(older)
		if olderTarget == nil {
			continue
		}
		olderIndexTypes, _ := ReindexIndexTypes(olderTarget.MigrationType)
		if slices.ContainsFunc(p.indexTypesRunOnSameProperties(target, olderTarget), func(indexType string) bool {
			return slices.Contains(newestOn, indexType) && slices.Contains(olderIndexTypes, indexType)
		}) {
			return true
		}
	}
	return false
}

// reindexTarget is the subset of ReindexTaskPayload the retainer reads.
// Skipping the per-unit maps keeps decoding cheap, since every node decodes
// this on every scheduler tick.
type reindexTarget struct {
	MigrationType ReindexMigrationType `json:"migrationType"`
	Collection    string               `json:"collection"`
	Properties    []string             `json:"properties"`
}

// reindexTargets decodes each task at most once per retention decision; nil
// marks an unreadable payload.
type reindexTargets map[distributedtask.TaskDescriptor]*reindexTarget

func (ts reindexTargets) of(task *distributedtask.Task) *reindexTarget {
	if target, ok := ts[task.TaskDescriptor]; ok {
		return target
	}
	target := &reindexTarget{}
	if err := json.Unmarshal(task.Payload, target); err != nil {
		target = nil
	}
	ts[task.TaskDescriptor] = target
	return target
}

// indexTypesNotRunSince returns task's index types that no newer run on the
// same properties has covered.
func (p *ReindexProvider) indexTypesNotRunSince(task *distributedtask.Task, target *reindexTarget,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task, targets reindexTargets,
) []string {
	remaining, _ := ReindexIndexTypes(target.MigrationType)
	for _, other := range namespaceTasks {
		if other.Version <= task.Version {
			continue
		}
		otherTarget := targets.of(other)
		if otherTarget == nil {
			continue
		}
		covered := p.indexTypesRunOnSameProperties(otherTarget, target)
		remaining = slices.DeleteFunc(remaining, func(indexType string) bool {
			return slices.Contains(covered, indexType)
		})
	}
	return remaining
}

// indexTypesRunOnSameProperties returns other's index types when other runs on
// target's collection and every property in target, and nil otherwise.
func (p *ReindexProvider) indexTypesRunOnSameProperties(other, target *reindexTarget) []string {
	if !strings.EqualFold(other.Collection, target.Collection) {
		return nil
	}
	for _, prop := range target.Properties {
		if !slices.Contains(other.Properties, prop) {
			return nil
		}
	}
	indexTypes, _ := ReindexIndexTypes(other.MigrationType)
	return indexTypes
}

// indexDroppedSince reports whether indexType is off on all of target's
// properties. Only an enable run leaves its index off on failure; any other
// run found off had it dropped afterward, and no run can cover a dropped
// index, so its failure would otherwise be kept forever.
func (p *ReindexProvider) indexDroppedSince(target *reindexTarget, indexType string) bool {
	enable := []ReindexMigrationType{ReindexTypeEnableSearchable, ReindexTypeEnableFilterable, ReindexTypeEnableRangeable}
	if slices.Contains(enable, target.MigrationType) || p.schemaManager == nil || len(target.Properties) == 0 {
		return false
	}
	class := p.schemaManager.ReadOnlyClass(target.Collection)
	if class == nil {
		return false
	}
	for _, name := range target.Properties {
		i := slices.IndexFunc(class.Properties, func(prop *models.Property) bool { return prop.Name == name })
		if i < 0 || p.indexEnabled(class.Properties[i], indexType) {
			return false
		}
	}
	return true
}

// indexEnabled reads the schema flags with the defaults the status endpoint uses.
func (p *ReindexProvider) indexEnabled(prop *models.Property, indexType string) bool {
	switch indexType {
	case "filterable":
		return prop.IndexFilterable == nil || *prop.IndexFilterable
	case "searchable":
		return prop.IndexSearchable == nil || *prop.IndexSearchable
	case "rangeable":
		return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
	}
	return true
}
