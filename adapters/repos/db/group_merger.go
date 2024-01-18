//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type groupMerger struct {
	objects []*storobj.Object
	dists   []float32
	groupBy *searchparams.GroupBy
}

func newGroupMerger(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy,
) *groupMerger {
	return &groupMerger{objects, dists, groupBy}
}

func (gm *groupMerger) Do() ([]*storobj.Object, []float32, error) {
	groups := map[string][]*additional.Group{}
	objects := map[string][]int{}

	for i, obj := range gm.objects {
		g, ok := obj.AdditionalProperties()["group"]
		if !ok {
			return nil, nil, fmt.Errorf("group not found for object: %v", obj.ID())
		}
		group, ok := g.(*additional.Group)
		if !ok {
			return nil, nil, fmt.Errorf("wrong group type for object: %v", obj.ID())
		}
		groups[group.GroupedBy.Value] = append(groups[group.GroupedBy.Value], group)
		objects[group.GroupedBy.Value] = append(objects[group.GroupedBy.Value], i)
	}

	getMinDistance := func(groups []*additional.Group) float32 {
		min := groups[0].MinDistance
		for i := range groups {
			if groups[i].MinDistance < min {
				min = groups[i].MinDistance
			}
		}
		return min
	}

	type groupMinDistance struct {
		value    string
		distance float32
	}

	groupDistances := []groupMinDistance{}
	for val, group := range groups {
		groupDistances = append(groupDistances, groupMinDistance{
			value: val, distance: getMinDistance(group),
		})
	}

	sort.Slice(groupDistances, func(i, j int) bool {
		return groupDistances[i].distance < groupDistances[j].distance
	})

	desiredLength := len(groups)
	if desiredLength > gm.groupBy.Groups {
		desiredLength = gm.groupBy.Groups
	}

	objs := make([]*storobj.Object, desiredLength)
	dists := make([]float32, desiredLength)
	for i, groupDistance := range groupDistances[:desiredLength] {
		val := groupDistance.value
		group := groups[groupDistance.value]
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
		obj.AdditionalProperties()["group"] = &additional.Group{
			ID: i,
			GroupedBy: &additional.GroupedBy{
				Value: val,
				Path:  []string{gm.groupBy.Property},
			},
			Count:       count,
			Hits:        hits,
			MaxDistance: hits[0]["_additional"].(*additional.GroupHitAdditional).Distance,
			MinDistance: hits[len(hits)-1]["_additional"].(*additional.GroupHitAdditional).Distance,
		}
		objs[i], dists[i] = obj, dist
	}

	return objs, dists, nil
}
