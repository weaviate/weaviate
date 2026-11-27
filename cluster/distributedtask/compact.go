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
	"slices"
	"strings"
)

const maxSummaryErrors = 20

type UnitSummary struct {
	Groups int                `json:"groups"`
	Counts map[UnitStatus]int `json:"counts"`
	Errors []string           `json:"errors,omitempty"`
}

// LocalEndedTasks returns no task: no Task records the index generation a restore needs.
func (m *Manager) LocalEndedTasks(namespace, collection string) ([]*Task, error) {
	return nil, nil
}

func (t *Task) summarizeUnits() *UnitSummary {
	if t.Summary != nil {
		return t.Summary
	}
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
