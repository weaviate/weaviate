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
	"sort"

	"github.com/weaviate/weaviate/entities/storobj"
)

type sortByDistances struct {
	objects []*storobj.Object
	scores  []float32
}

func (sbd *sortByDistances) Len() int {
	return len(sbd.objects)
}

func (sbd *sortByDistances) Less(i, j int) bool {
	return sbd.scores[i] < sbd.scores[j]
}

func (sbd *sortByDistances) Swap(i, j int) {
	sbd.scores[i], sbd.scores[j] = sbd.scores[j], sbd.scores[i]
	sbd.objects[i], sbd.objects[j] = sbd.objects[j], sbd.objects[i]
}

type sortObjectsByDistance struct{}

func newDistancesSorter() *sortObjectsByDistance {
	return &sortObjectsByDistance{}
}

func (s *sortObjectsByDistance) sort(objects []*storobj.Object, distances []float32) ([]*storobj.Object, []float32) {
	sbd := &sortByDistances{objects, distances}
	sort.Sort(sbd)
	return sbd.objects, sbd.scores
}
