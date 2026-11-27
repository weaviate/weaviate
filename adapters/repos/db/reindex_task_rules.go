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
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

type ReindexTaskRules struct{}

var _ distributedtask.TaskRules = ReindexTaskRules{}

// Admit gives a run a new generation, or the generation of the run it takes over.
func (ReindexTaskRules) Admit(task *distributedtask.Task, existing []*distributedtask.Task,
	live map[string]map[string]uint64,
) (*distributedtask.Task, error) {
	h, ok := ReindexHeaderOf(task)
	if !ok || !h.ViewServed {
		return nil, nil
	}
	highest := uint64(0)
	for _, prop := range h.Properties {
		for _, gen := range live[prop] {
			highest = max(highest, gen)
		}
	}
	var kept *distributedtask.Task
	var keptGen uint64
	for _, other := range existing {
		oh, ok := ReindexHeaderOf(other)
		if !ok || oh.Generation == 0 || !sameReindexProperties(oh, h) {
			continue
		}
		highest = max(highest, oh.Generation)
		if !ReindexLeftPartial(other) {
			continue
		}
		if !sameReindexTarget(oh.ReindexTaskPayload, h.ReindexTaskPayload) {
			return nil, distributedtask.NewTaskPartlyMigratedError(h.Collection())
		}
		if oh.Generation > keptGen {
			kept, keptGen = other, oh.Generation
		}
	}
	h.Generation = highest + 1
	if kept != nil {
		h.Generation = keptGen
		task.TakeOver(kept)
	}
	payload, err := json.Marshal(h.ReindexTaskPayload)
	if err != nil {
		return nil, fmt.Errorf("encode reindex payload: %w", err)
	}
	task.SetHeader(payload, h)
	return kept, nil
}

// Ended takes the run's switched tenants from older ended runs on its properties
// whose index types it covers. A finished full run deletes itself and all such
// runs, generation or not, because its flip already made the class serve it.
func (ReindexTaskRules) Ended(task *distributedtask.Task, existing []*distributedtask.Task) distributedtask.EndRuling {
	h, ok := ReindexHeaderOf(task)
	if !ok || h.Generation == 0 {
		return distributedtask.EndRuling{}
	}
	flipped := task.Status == distributedtask.TaskStatusFinished && len(h.Tenants) == 0
	ruling := distributedtask.EndRuling{Compact: !flipped}
	if flipped {
		ruling.Remove = append(ruling.Remove, task)
	}
	types, _ := ReindexIndexTypes(h.MigrationType)
	for _, other := range existing {
		oh, ok := ReindexHeaderOf(other)
		if !ok || other == task || !other.Status.IsTerminal() || !sameReindexProperties(oh, h) ||
			!coversIndexTypes(types, oh.MigrationType) {
			continue
		}
		if flipped {
			ruling.Remove = append(ruling.Remove, other)
			continue
		}
		if oh.Generation == 0 || oh.Generation >= h.Generation {
			continue
		}
		lost := switchedInBoth(task, other)
		switch {
		case len(lost) == 0:
		case len(lost) == len(other.SwapDecidedGroups):
			ruling.Remove = append(ruling.Remove, other)
		default:
			if ruling.Forget == nil {
				ruling.Forget = map[*distributedtask.Task][]string{}
			}
			ruling.Forget[other] = lost
		}
	}
	return ruling
}

// BornSwitched holds for full runs, because a tenant created during one has nothing to build.
func (ReindexTaskRules) BornSwitched(task *distributedtask.Task) bool {
	h, ok := ReindexHeaderOf(task)
	return ok && h.Generation > 0 && len(h.Tenants) == 0
}

func coversIndexTypes(types []string, older ReindexMigrationType) bool {
	olderTypes, known := ReindexIndexTypes(older)
	return known && containsAll(types, olderTypes)
}

func containsAll(have, want []string) bool {
	return !slices.ContainsFunc(want, func(s string) bool { return !slices.Contains(have, s) })
}

func switchedInBoth(a, b *distributedtask.Task) []string {
	var both []string
	for group := range a.SwapDecidedGroups {
		if _, ok := b.SwapDecidedGroups[group]; ok {
			both = append(both, group)
		}
	}
	return both
}

// sameReindexProperties refuses overlaps, or the other properties' switched tenants lose their task.
func sameReindexProperties(a, b ReindexTaskHeader) bool {
	if !strings.EqualFold(a.Collection(), b.Collection()) || len(a.Properties) != len(b.Properties) {
		return false
	}
	return containsAll(b.Properties, a.Properties)
}

func sameReindexTarget(a, b ReindexTaskPayload) bool {
	return a.MigrationType == b.MigrationType && a.TargetTokenization == b.TargetTokenization &&
		a.BucketStrategy == b.BucketStrategy
}
