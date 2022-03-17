//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"sort"

	"github.com/semi-technologies/weaviate/entities/storobj"
)

type sortByDistances struct {
	objects []*storobj.Object
	scores  []float32
}

func (sbd sortByDistances) Len() int {
	return len(sbd.objects)
}

func (sbd sortByDistances) Less(i, j int) bool {
	return sbd.scores[i] < sbd.scores[j]
}

func (sbd sortByDistances) Swap(i, j int) {
	sbd.scores[i], sbd.scores[j] = sbd.scores[j], sbd.scores[i]
	sbd.objects[i], sbd.objects[j] = sbd.objects[j], sbd.objects[i]
}

type sortObjectsByDistance struct{}

func newDistancesSorter() *sortObjectsByDistance {
	return &sortObjectsByDistance{}
}

func (s *sortObjectsByDistance) sort(objects []*storobj.Object, distances []float32) ([]*storobj.Object, []float32) {
	sbd := sortByScores{objects, distances}
	sort.Sort(sbd)
	return sbd.objects, sbd.scores
}
