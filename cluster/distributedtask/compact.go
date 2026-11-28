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

package distributedtask

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

const maxSummaryErrors = 20

type UnitSummary struct {
	Groups int                `json:"groups"`
	Counts map[UnitStatus]int `json:"counts"`
	Errors []string           `json:"errors,omitempty"`
}

// LocalEndedTasks copies collection's ended tasks with a generation, which the end rules already compacted.
func (m *Manager) LocalEndedTasks(namespace, collection string) []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*Task
	for _, task := range m.tasks[namespace] {
		if task.Status.IsTerminal() && switchedByView(task.header) && strings.EqualFold(task.collection(), collection) {
			out = append(out, task.compactCopy())
		}
	}
	sortTasksForDisplay(out)
	return out
}

// compactCopy builds the copy field by field, because Clone copies every unit.
func (t *Task) compactCopy() *Task {
	return &Task{
		Namespace:               t.Namespace,
		TaskDescriptor:          t.TaskDescriptor,
		Payload:                 t.Payload,
		NeedsPreparationBarrier: t.NeedsPreparationBarrier,
		Status:                  t.Status,
		StartedAt:               t.StartedAt,
		FinishedAt:              t.FinishedAt,
		Error:                   t.Error,
		SwapDecidedGroups:       maps.Clone(t.SwapDecidedGroups),
		Summary:                 t.Summary,
	}
}

// GroupTotal counts a task's groups, switched ones without units (born, taken over) included.
func (t *Task) GroupTotal() int {
	if t.Summary != nil {
		return t.Summary.Groups
	}
	n := t.groupCount
	for group := range t.SwapDecidedGroups {
		if !t.hasGroup(group) {
			n++
		}
	}
	return n
}

func (t *Task) summarizeUnits() *UnitSummary {
	if t.Summary != nil {
		return t.Summary
	}
	s := &UnitSummary{Groups: t.GroupTotal(), Counts: map[UnitStatus]int{}}
	var failed []*Unit
	for _, u := range t.Units {
		s.Counts[u.Status]++
		if u.Status == UnitStatusFailed {
			failed = append(failed, u)
		}
	}
	slices.SortFunc(failed, func(a, b *Unit) int {
		return cmp.Or(a.FinishedAt.Compare(b.FinishedAt), strings.Compare(a.ID, b.ID))
	})
	for _, u := range failed[:min(len(failed), maxSummaryErrors)] {
		s.Errors = append(s.Errors, u.ID+": "+u.Error)
	}
	return s
}
