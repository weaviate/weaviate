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
	"encoding/json"
	"fmt"
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

// CompactEndedTasks holds the read lock for one pass over the units of collection's ended tasks.
func (m *Manager) CompactEndedTasks(namespace, collection string) ([]*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*Task
	for _, task := range m.tasks[namespace] {
		if !task.Status.IsTerminal() || task.collection() == "" || task.OnKnownOtherCollection(collection) {
			continue
		}
		compact, err := task.compact()
		if err != nil {
			return nil, err
		}
		out = append(out, compact)
	}
	sortTasksForDisplay(out)
	return out, nil
}

// compact builds the copy field by field, because Clone copies every unit.
func (t *Task) compact() (*Task, error) {
	payload, err := json.Marshal(t.header)
	if err != nil {
		return nil, fmt.Errorf("encode header of task %s/%s/%d: %w", t.Namespace, t.ID, t.Version, err)
	}
	return &Task{
		Namespace:               t.Namespace,
		TaskDescriptor:          t.TaskDescriptor,
		Payload:                 payload,
		NeedsPreparationBarrier: t.NeedsPreparationBarrier,
		Status:                  t.Status,
		StartedAt:               t.StartedAt,
		FinishedAt:              t.FinishedAt,
		Error:                   t.Error,
		SwapDecidedGroups:       maps.Clone(t.SwapDecidedGroups),
		Summary:                 t.summarizeUnits(),
	}, nil
}

func (t *Task) summarizeUnits() *UnitSummary {
	s := &UnitSummary{Groups: t.groupCount, Counts: map[UnitStatus]int{}}
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
