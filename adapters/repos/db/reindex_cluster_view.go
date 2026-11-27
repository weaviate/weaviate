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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	indexTypeFilterable = "filterable"
	indexTypeSearchable = "searchable"
	indexTypeRangeable  = "rangeable"
)

var reindexIndexTypes = []string{indexTypeFilterable, indexTypeSearchable, indexTypeRangeable}

type reindexBuildTarget struct {
	indexTypes   []string
	tokenization string // empty keeps the property's
	blockmax     bool
}

func reindexBuildTargetOf(h ReindexTaskHeader) (reindexBuildTarget, error) {
	types, known := ReindexIndexTypes(h.MigrationType)
	if !known {
		return reindexBuildTarget{}, fmt.Errorf("unknown migration type %q", h.MigrationType)
	}
	target := reindexBuildTarget{indexTypes: types}
	switch h.MigrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		target.tokenization = h.TargetTokenization
	case ReindexTypeEnableSearchable:
		target.tokenization, target.blockmax = h.TargetTokenization, true
	case ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable:
		target.blockmax = true
	default:
	}
	return target, nil
}

type reindexStaged struct {
	taskID      string
	gen         uint64
	target      reindexBuildTarget
	unitStarted bool
}

type reindexPropView struct {
	gen          map[string]uint64
	on           map[string]bool
	tokenization string
	blockmax     bool
	staged       []reindexStaged
}

// computeReindexView maps each property with a generation to what it serves.
// An index serves the highest of the class's live generation and the
// generation of every task that switched this shard, with the winner's
// settings. A running task with an unswitched unit here is staged.
func computeReindexView(class *models.Class, live map[string]map[string]uint64,
	tasks []distributedtask.ShardTaskState,
) (map[string]reindexPropView, error) {
	type winner struct {
		gen    uint64
		target reindexBuildTarget
	}
	wins := map[string]map[string]winner{}
	staged := map[string][]reindexStaged{}
	for _, st := range tasks {
		h, ok := st.Header.(ReindexTaskHeader)
		if !ok || h.Generation == 0 {
			continue
		}
		target, err := reindexBuildTargetOf(h)
		if err != nil {
			return nil, fmt.Errorf("reindex task %s: %w", st.ID, err)
		}
		for _, prop := range h.Properties {
			switch {
			case st.Switched:
				if wins[prop] == nil {
					wins[prop] = map[string]winner{}
				}
				for _, t := range target.indexTypes {
					if h.Generation > wins[prop][t].gen {
						wins[prop][t] = winner{gen: h.Generation, target: target}
					}
				}
			case st.HasUnit && !st.Status.IsTerminal():
				staged[prop] = append(staged[prop], reindexStaged{
					taskID: st.ID, gen: h.Generation, target: target, unitStarted: st.LocalUnitStarted,
				})
			}
		}
	}

	out := map[string]reindexPropView{}
	for _, prop := range class.Properties {
		if !hasLiveGeneration(live[prop.Name]) && wins[prop.Name] == nil && staged[prop.Name] == nil {
			continue
		}
		v := reindexPropView{gen: map[string]uint64{}, on: map[string]bool{}, tokenization: prop.Tokenization}
		var tokenizationGen uint64
		for _, t := range reindexIndexTypes {
			w, liveGen := wins[prop.Name][t], live[prop.Name][t]
			if w.gen <= liveGen {
				v.gen[t], v.on[t] = liveGen, classIndexOn(prop, t)
				continue
			}
			v.gen[t], v.on[t] = w.gen, true
			if w.target.tokenization != "" && w.gen > tokenizationGen {
				v.tokenization, tokenizationGen = w.target.tokenization, w.gen
			}
			v.blockmax = v.blockmax || (t == indexTypeSearchable && w.target.blockmax)
		}
		for _, s := range staged[prop.Name] {
			for _, t := range s.target.indexTypes {
				if s.gen > v.gen[t] {
					v.staged = append(v.staged, s)
					break
				}
			}
		}
		out[prop.Name] = v
	}
	return out, nil
}

func hasLiveGeneration(gens map[string]uint64) bool {
	for _, g := range gens {
		if g > 0 {
			return true
		}
	}
	return false
}

func classIndexOn(prop *models.Property, indexType string) bool {
	switch indexType {
	case indexTypeSearchable:
		return inverted.HasSearchableIndex(prop)
	case indexTypeRangeable:
		return inverted.HasRangeableIndex(prop)
	default:
		return inverted.HasFilterableIndex(prop)
	}
}

func reindexBucketName(prop, indexType string, gen uint64) string {
	switch indexType {
	case indexTypeSearchable:
		return helpers.BucketSearchableFromPropNameLSMAtGen(prop, int64(gen))
	case indexTypeRangeable:
		return helpers.BucketRangeableFromPropNameLSMAtGen(prop, int64(gen))
	default:
		return helpers.BucketFromPropNameLSMAtGen(prop, int64(gen))
	}
}

type ReindexTaskStateSource interface {
	ShardTaskStates(namespace, collection, shard, node string) []distributedtask.ShardTaskState
}

func (db *DB) SetReindexTaskStateSource(src ReindexTaskStateSource) {
	db.reindexLookupsMu.Lock()
	defer db.reindexLookupsMu.Unlock()
	db.reindexTaskStates = src
}

func (db *DB) reindexTaskStateSource() ReindexTaskStateSource {
	if db == nil {
		return nil
	}
	db.reindexLookupsMu.RLock()
	defer db.reindexLookupsMu.RUnlock()
	return db.reindexTaskStates
}

// readReindexView reads the shard's tasks, then the live generations, then the
// class. A full run writes Live and the class settings in its flip before its
// task goes, so this order never misses both the task and what it flipped. A
// DB not wired to cluster state has no view.
func (s *Shard) readReindexView() (*models.Class, map[string]reindexPropView, error) {
	src := s.index.db.reindexTaskStateSource()
	if src == nil {
		return nil, nil, nil
	}
	className := s.index.Config.ClassName.String()
	tasks := src.ShardTaskStates(ReindexNamespace, className, s.name, s.index.db.localNodeName)
	live := s.index.getSchema.ClassLive(className)
	class := s.index.getSchema.ReadOnlyClass(className)
	if class == nil {
		return nil, nil, nil
	}
	view, err := computeReindexView(class, live, tasks)
	return class, view, err
}

func (db *DB) ReindexTaskStateListener(namespace string, header distributedtask.TaskHeader, groups []string) {
	if namespace != ReindexNamespace || header == nil {
		return
	}
	db.reindexRefresher().notify(header.Collection(), groups)
}
