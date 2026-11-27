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

// sameReindexProperties refuses overlaps, or the other properties' switched tenants lose their task.
func sameReindexProperties(a, b ReindexTaskHeader) bool {
	if !strings.EqualFold(a.Collection(), b.Collection()) || len(a.Properties) != len(b.Properties) {
		return false
	}
	return !slices.ContainsFunc(a.Properties, func(p string) bool { return !slices.Contains(b.Properties, p) })
}

func sameReindexTarget(a, b ReindexTaskPayload) bool {
	return a.MigrationType == b.MigrationType && a.TargetTokenization == b.TargetTokenization &&
		a.BucketStrategy == b.BucketStrategy
}
