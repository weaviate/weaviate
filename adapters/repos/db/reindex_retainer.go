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
	"cmp"
	"encoding/json"
	"fmt"
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

func ReindexLeftPartial(task *distributedtask.Task) bool {
	return (task.Status == distributedtask.TaskStatusFailed || task.Status == distributedtask.TaskStatusCancelled) &&
		len(task.SwapDecidedGroups) > 0
}

func ReindexRunLanded(class *models.Class, mt ReindexMigrationType, targetTok string, props []string) bool {
	visible, _ := migrationEffectReader(mt)
	if class == nil || visible == nil || len(props) == 0 {
		return false
	}
	for _, name := range props {
		i := slices.IndexFunc(class.Properties, func(p *models.Property) bool { return p.Name == name })
		if i < 0 || !visible(MigrationSubject{MigrationType: mt, TargetTokenization: targetTok}, class.Properties[i]) {
			return false
		}
	}
	return true
}

func (ReindexDetector) CutoverLanded(task *distributedtask.Task, readClass func(string) *models.Class) bool {
	var payload ReindexTaskPayload
	if json.Unmarshal(task.Payload, &payload) != nil {
		return false
	}
	return ReindexRunLanded(readClass(payload.Collection), payload.MigrationType,
		payload.TargetTokenization, payload.Properties)
}

func (ReindexDetector) CheckMigrationPropertyUpdate(className, propertyName string, task *distributedtask.Task) error {
	if task != nil && task.Status == distributedtask.TaskStatusSwapping {
		var payload ReindexTaskPayload
		if json.Unmarshal(task.Payload, &payload) == nil &&
			strings.EqualFold(payload.Collection, className) && slices.Contains(payload.Properties, propertyName) {
			return nil
		}
	}
	return fmt.Errorf("no reindex run is completing on %s.%s; its cutover write is refused", className, propertyName)
}

type ReindexRun struct {
	Task    *distributedtask.Task
	Payload ReindexTaskPayload
}

// ReindexMigratedTenants reads property's failed/cancelled runs since the last finished flip. migrated:
// newest decided swap went to target; covered: held by target's runs; paid: no replica still owes it.
func ReindexMigratedTenants(runs []ReindexRun, property string, target *ReindexTaskPayload) (migrated, paid, covered map[string]struct{}) {
	targetTypes, _ := ReindexIndexTypes(target.MigrationType)
	var onProperty []ReindexRun
	for _, run := range runs {
		if strings.EqualFold(run.Payload.Collection, target.Collection) && slices.Contains(run.Payload.Properties, property) {
			onProperty = append(onProperty, run)
		}
	}
	slices.SortFunc(onProperty, func(a, b ReindexRun) int { return cmp.Compare(b.Task.Version, a.Task.Version) })

	migrated, paid, covered = map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
	swappedSince := map[string]struct{}{}
	for _, run := range onProperty {
		types, _ := ReindexIndexTypes(run.Payload.MigrationType)
		if run.Task.Status == distributedtask.TaskStatusFinished && IsSemanticMigration(run.Payload.MigrationType) &&
			!slices.ContainsFunc(targetTypes, func(t string) bool { return !slices.Contains(types, t) }) {
			break
		}
		overlaps := slices.ContainsFunc(types, func(t string) bool { return slices.Contains(targetTypes, t) })
		if !overlaps || (run.Task.Status != distributedtask.TaskStatusFailed && run.Task.Status != distributedtask.TaskStatusCancelled) {
			continue
		}
		same := run.Payload.MigrationType == target.MigrationType &&
			run.Payload.TargetTokenization == target.TargetTokenization && run.Payload.BucketStrategy == target.BucketStrategy
		for group := range run.Task.SwapDecidedGroups {
			if _, seen := swappedSince[group]; same && !seen {
				migrated[group] = struct{}{}
				if len(run.Task.SwapsOwed[group]) == 0 {
					paid[group] = struct{}{}
				}
			}
			swappedSince[group] = struct{}{}
		}
		for _, group := range run.Task.Groups() {
			if same {
				covered[group] = struct{}{}
			}
		}
	}
	return migrated, paid, covered
}

// ReindexTenantCounts reports ok=false when migrated holds "", the unnamed group of a collection without multi-tenancy.
func ReindexTenantCounts(migrated, covered map[string]struct{}) (migratedTenants, totalTenants int, ok bool) {
	if _, ungrouped := migrated[""]; ungrouped {
		return 0, 0, false
	}
	return len(migrated), len(covered), true
}

func ReindexMigratedPhrase(migrated, covered map[string]struct{}, property string, tenantCounts bool) string {
	migratedTenants, totalTenants, ok := ReindexTenantCounts(migrated, covered)
	if !ok {
		return fmt.Sprintf("property %q of the collection is migrated", property)
	}
	if !tenantCounts {
		return fmt.Sprintf("some tenants of property %q are migrated", property)
	}
	return fmt.Sprintf("%d of %d tenants of property %q are migrated", migratedTenants, totalTenants, property)
}

func ReindexRuns(tasks []*distributedtask.Task) []ReindexRun {
	runs := make([]ReindexRun, 0, len(tasks))
	for _, task := range tasks {
		var payload ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err == nil {
			runs = append(runs, ReindexRun{Task: task, Payload: payload})
		}
	}
	return runs
}

func ReindexPartialRunConflict(tasks []*distributedtask.Task, collection, property string,
	accepts func(*ReindexRun) bool, tenantCounts bool,
) string {
	if !slices.ContainsFunc(tasks, ReindexLeftPartial) {
		return ""
	}
	runs := ReindexRuns(tasks)
	for _, scope := range [][]string{{"searchable"}, {"filterable"}, {"rangeable"}} {
		run, migrated, covered := ReindexPartialRun(runs, collection, property, scope)
		if run == nil || (accepts != nil && accepts(run)) {
			continue
		}
		return fmt.Sprintf("%s; finish the migration with %s",
			ReindexMigratedPhrase(migrated, covered, property, tenantCounts), ReindexResubmitCommand(&run.Payload, property))
	}
	return ""
}

func ReindexPartialRun(runs []ReindexRun, collection, property string, indexTypes []string) (partial *ReindexRun, migrated, covered map[string]struct{}) {
	for i := range runs {
		run := &runs[i]
		types, _ := ReindexIndexTypes(run.Payload.MigrationType)
		if strings.EqualFold(run.Payload.Collection, collection) && slices.Contains(run.Payload.Properties, property) &&
			ReindexLeftPartial(run.Task) && slices.ContainsFunc(types, func(t string) bool { return slices.Contains(indexTypes, t) }) &&
			(partial == nil || run.Task.Version > partial.Task.Version) {
			partial = run
		}
	}
	if partial == nil {
		return nil, nil, nil
	}
	if migrated, _, covered = ReindexMigratedTenants(runs, property, &partial.Payload); len(migrated) == 0 {
		return nil, nil, nil // a newer finished flip covered it, or no tenant still serves target
	}
	return partial, migrated, covered
}

// ShouldRetainCompletedTask keeps a task while a replica owes a swap, or while it is FAILED or partial and uncovered.
func (p *ReindexProvider) ShouldRetainCompletedTask(task *distributedtask.Task,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	if len(task.SwapsOwed) > 0 {
		return true
	}
	targets := reindexTargets{}
	target := targets.of(task)
	if target == nil {
		return false // the status endpoint cannot read it either
	}
	partial := ReindexLeftPartial(task)
	newestOn := p.indexTypesNotRunSince(task, target, namespaceTasks, targets, partial)
	if len(newestOn) == 0 {
		return false
	}
	if task.Status == distributedtask.TaskStatusFailed || partial {
		return slices.ContainsFunc(newestOn, func(indexType string) bool {
			return !p.indexDroppedSince(target, indexType)
		})
	}
	for _, older := range namespaceTasks {
		if older.Version >= task.Version || (older.Status != distributedtask.TaskStatusFailed && !ReindexLeftPartial(older)) {
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

func (p *ReindexProvider) indexTypesNotRunSince(task *distributedtask.Task, target *reindexTarget,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task, targets reindexTargets, flipsOnly bool,
) []string {
	remaining, _ := ReindexIndexTypes(target.MigrationType)
	for _, other := range namespaceTasks {
		if other.Version <= task.Version {
			continue
		}
		otherTarget := targets.of(other)
		if otherTarget == nil || (flipsOnly &&
			(other.Status != distributedtask.TaskStatusFinished || !IsSemanticMigration(otherTarget.MigrationType))) {
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
