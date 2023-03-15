//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"sort"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type groupMerger struct {
	objects    []*storobj.Object
	dists      []float32
	groupBy    *searchparams.GroupBy
	limit      int
	shardCount int
}

func newGroupMerger(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy, limit, shardCount int,
) *groupMerger {
	return &groupMerger{objects, dists, groupBy, limit, shardCount}
}

func (gm *groupMerger) Do() ([]*storobj.Object, []float32, error) {
	groups := map[string][]additional.Group{}
	objects := map[string][]int{}

	for i, obj := range gm.objects {
		g, ok := obj.AdditionalProperties()["group"]
		if !ok {
			continue
		}
		group, ok := g.(additional.Group)
		if !ok {
			continue
		}
		groups[group.GroupValue] = append(groups[group.GroupValue], group)
		objects[group.GroupValue] = append(objects[group.GroupValue], i)
	}

	i := 0
	objs := make([]*storobj.Object, len(groups))
	dists := make([]float32, len(groups))
	for val, group := range groups {
		if i >= (gm.groupBy.Groups + gm.shardCount) {
			break
		}
		count := 0
		hits := []map[string]interface{}{}
		for _, g := range group {
			count += g.Count
			hits = append(hits, g.Hits...)
		}

		sort.Slice(hits, func(i, j int) bool {
			return hits[i]["_additional"].(*additional.GroupHitAdditional).Distance <
				hits[j]["_additional"].(*additional.GroupHitAdditional).Distance
		})

		if len(hits) > gm.groupBy.ObjectsPerGroup {
			hits = hits[:gm.groupBy.ObjectsPerGroup]
			count = len(hits)
		}

		indx := objects[val][0]
		obj, dist := gm.objects[indx], gm.dists[indx]
		obj.AdditionalProperties()["group"] = additional.Group{
			ID:          i,
			GroupValue:  val,
			Count:       count,
			Hits:        hits,
			MaxDistance: hits[0]["_additional"].(*additional.GroupHitAdditional).Distance,
			MinDistance: hits[len(hits)-1]["_additional"].(*additional.GroupHitAdditional).Distance,
		}
		objs[i], dists[i] = obj, dist
		i++
	}

	objs, dists = newObjectsByGroupsSorter().sort(objs[:i], dists[:i])
	cutoff := gm.groupBy.Groups
	if cutoff > i {
		cutoff = i
	}
	return objs[:cutoff], dists[:cutoff], nil
}
