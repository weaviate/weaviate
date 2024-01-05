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

type sortByID struct {
	objects []*storobj.Object
	scores  []float32
}

func (s *sortByID) Swap(i, j int) {
	if len(s.objects) == len(s.scores) {
		s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
	}
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
}

func (s *sortByID) Less(i, j int) bool {
	return s.objects[i].ID() < s.objects[j].ID()
}

func (s *sortByID) Len() int {
	return len(s.objects)
}

type sortObjectsByID struct{}

func newIDSorter() *sortObjectsByID {
	return &sortObjectsByID{}
}

func (s *sortObjectsByID) sort(objects []*storobj.Object, scores []float32,
) ([]*storobj.Object, []float32) {
	sbd := &sortByID{objects, scores}
	sort.Sort(sbd)
	return sbd.objects, sbd.scores
}
