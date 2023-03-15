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
	"github.com/weaviate/weaviate/entities/storobj"
)

type sortByGroups struct {
	objects []*storobj.Object
	scores  []float32
}

func (sbd *sortByGroups) Len() int {
	return len(sbd.objects)
}

func (sbd *sortByGroups) Less(i, j int) bool {
	ii := sbd.objects[i].AdditionalProperties()["group"].(additional.Group).MaxDistance
	jj := sbd.objects[j].AdditionalProperties()["group"].(additional.Group).MaxDistance
	return ii < jj
}

func (sbd *sortByGroups) Swap(i, j int) {
	sbd.scores[i], sbd.scores[j] = sbd.scores[j], sbd.scores[i]
	sbd.objects[i], sbd.objects[j] = sbd.objects[j], sbd.objects[i]
}

type sortObjectsByGroups struct{}

func newObjectsByGroupsSorter() *sortObjectsByGroups {
	return &sortObjectsByGroups{}
}

func (s *sortObjectsByGroups) sort(objects []*storobj.Object, distances []float32) ([]*storobj.Object, []float32) {
	sbd := &sortByGroups{objects, distances}
	sort.Sort(sbd)
	return sbd.objects, sbd.scores
}
