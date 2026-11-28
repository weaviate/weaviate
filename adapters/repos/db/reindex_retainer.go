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
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
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

// CutoverLanded reports a run without a generation as landed, since it never serves.
func (ReindexDetector) CutoverLanded(task *distributedtask.Task, classLive func(string) map[string]map[string]uint64) bool {
	h, ok := ReindexHeaderOf(task)
	if !ok {
		return false
	}
	if !h.SwitchedByView() {
		return true
	}
	indexTypes, known := ReindexIndexTypes(h.MigrationType)
	if !known || len(h.Properties) == 0 {
		return false
	}
	live := classLive(h.Collection())
	for _, prop := range h.Properties {
		for _, indexType := range indexTypes {
			if gen, present := live[prop][indexType]; !present || gen != h.Generation {
				return false
			}
		}
	}
	return true
}

func (ReindexDetector) CheckMigrationPropertyUpdate(className, propertyName string, gen uint64, task *distributedtask.Task) error {
	if task != nil && task.Status == distributedtask.TaskStatusSwapping {
		if h, ok := ReindexHeaderOf(task); ok && h.Generation == gen &&
			strings.EqualFold(h.Collection(), className) && slices.Contains(h.Properties, propertyName) {
			return nil
		}
	}
	return fmt.Errorf("no reindex run is completing on %s.%s; its cutover write is refused", className, propertyName)
}

type ReindexRun struct {
	Task    *distributedtask.Task
	Payload ReindexTaskPayload
}

func ReindexMigratedPhrase[V any](migrated map[string]V, total int, property string, tenantCounts bool) string {
	if _, ungrouped := migrated[""]; ungrouped {
		return fmt.Sprintf("property %q of the collection is migrated", property)
	}
	if !tenantCounts {
		return fmt.Sprintf("some tenants of property %q are migrated", property)
	}
	return fmt.Sprintf("%d of %d tenants of property %q are migrated", len(migrated), total, property)
}

func ReindexRuns(tasks []*distributedtask.Task) []ReindexRun {
	runs := make([]ReindexRun, 0, len(tasks))
	for _, task := range tasks {
		if h, ok := ReindexHeaderOf(task); ok {
			runs = append(runs, ReindexRun{Task: task, Payload: h.ReindexTaskPayload})
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
		run := ReindexPartialRun(runs, collection, property, scope)
		if run == nil || (accepts != nil && accepts(run)) {
			continue
		}
		return fmt.Sprintf("%s; finish the migration with %s",
			ReindexMigratedPhrase(run.Task.SwapDecidedGroups, run.Task.GroupTotal(), property, tenantCounts),
			ReindexResubmitCommand(&run.Payload, property))
	}
	return ""
}

// ReindexPartialRun skips runs without a generation, which never serve.
func ReindexPartialRun(runs []ReindexRun, collection, property string, indexTypes []string) *ReindexRun {
	var partial *ReindexRun
	for i := range runs {
		run := &runs[i]
		types, _ := ReindexIndexTypes(run.Payload.MigrationType)
		if run.Payload.Generation > 0 && strings.EqualFold(run.Payload.Collection, collection) &&
			slices.Contains(run.Payload.Properties, property) && ReindexLeftPartial(run.Task) &&
			slices.ContainsFunc(types, func(t string) bool { return slices.Contains(indexTypes, t) }) &&
			(partial == nil || run.Task.Version > partial.Task.Version) {
			partial = run
		}
	}
	return partial
}

func (p *ReindexProvider) ShouldRetainCompletedTask(task *distributedtask.Task,
	_ map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	h, ok := ReindexHeaderOf(task)
	if !ok || !h.SwitchedByView() {
		return false
	}
	if task.Status == distributedtask.TaskStatusFinished {
		return len(task.SwapDecidedGroups) > 0
	}
	return task.Status == distributedtask.TaskStatusFailed || task.Status == distributedtask.TaskStatusCancelled
}
