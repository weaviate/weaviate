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
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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

// IndexOff removes ended runs of that index type alone, since the returned Live covers them.
func (ReindexTaskRules) IndexOff(collection, property, indexType string, existing []*distributedtask.Task,
) (uint64, distributedtask.EndRuling) {
	var live uint64
	var covered distributedtask.EndRuling
	for _, task := range existing {
		h, ok := ReindexRunServingIndex(task, collection, property, indexType)
		if !ok {
			continue
		}
		live = max(live, h.Generation)
		if types, _ := ReindexIndexTypes(h.MigrationType); task.Status.IsTerminal() && len(types) == 1 && len(h.Properties) == 1 {
			covered.Remove = append(covered.Remove, task)
		}
	}
	return live, covered
}

func ReindexRunServingIndex(task *distributedtask.Task, collection, property, indexType string) (ReindexTaskHeader, bool) {
	h, ok := ReindexHeaderOf(task)
	if !ok || h.Generation == 0 || len(task.SwapDecidedGroups) == 0 ||
		!strings.EqualFold(h.Collection(), collection) || !slices.Contains(h.Properties, property) {
		return ReindexTaskHeader{}, false
	}
	types, _ := ReindexIndexTypes(h.MigrationType)
	return h, slices.Contains(types, indexType)
}

// BornSwitched holds for full runs, because a tenant created during one has nothing to build.
func (ReindexTaskRules) BornSwitched(task *distributedtask.Task) bool {
	h, ok := ReindexHeaderOf(task)
	return ok && h.Generation > 0 && len(h.Tenants) == 0
}

func MigrationUnitID(shardName, nodeName string) string {
	return shardName + "__" + nodeName
}

// Units gives each replica of each shard in scope one unit, shards by name and replicas in their stored
// order. Each tenant switches as its own group. A missing named tenant, or any shard in scope that
// is not HOT, refuses the whole run.
func (ReindexTaskRules) Units(header distributedtask.TaskHeader, state *sharding.State) ([]distributedtask.UnitSpec, error) {
	h, ok := header.(ReindexTaskHeader)
	if !ok {
		return nil, fmt.Errorf("not a reindex task header: %T", header)
	}
	shards := slices.Compact(slices.Sorted(slices.Values(h.Tenants)))
	if len(shards) == 0 {
		shards = slices.Sorted(maps.Keys(state.Physical))
	}
	specs := make([]distributedtask.UnitSpec, 0, 3*len(shards))
	missing, notHot := false, 0
	for _, name := range shards {
		physical, ok := state.Physical[name]
		switch {
		case !ok:
			missing = true
			continue
		case !isHotTenant(physical.Status):
			notHot++
			continue
		}
		group := ""
		if state.PartitioningEnabled {
			group = name
		}
		for _, node := range physical.BelongsToNodes {
			if node != "" {
				specs = append(specs, distributedtask.UnitSpec{
					ID: MigrationUnitID(name, node), GroupID: group, Shard: name, Node: node,
				})
			}
		}
	}
	if missing {
		return nil, distributedtask.NewTenantsMissingError(h.Collection())
	}
	if notHot > 0 {
		return nil, distributedtask.NewTenantsNotHotError(h.Collection(), notHot)
	}
	return specs, nil
}

// isHotTenant also accepts an unset status, which a shard of a single-tenant collection has.
func isHotTenant(status string) bool {
	switch entschema.ActivityStatus(status) {
	case models.TenantActivityStatusHOT, models.TenantActivityStatusACTIVE:
		return true
	default:
		return false
	}
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
