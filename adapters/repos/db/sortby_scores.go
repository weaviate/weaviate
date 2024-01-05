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

// sortByScores aka RankedResults implements sort.Interface, allowing
// results aggregated from multiple shards to be
// sorted according to their BM25 ranking
type sortByScores struct {
	objects []*storobj.Object
	scores  []float32
}

func (r *sortByScores) Swap(i, j int) {
	r.objects[i], r.objects[j] = r.objects[j], r.objects[i]
	r.scores[i], r.scores[j] = r.scores[j], r.scores[i]
}

func (r *sortByScores) Less(i, j int) bool {
	return r.scores[i] > r.scores[j]
}

func (r *sortByScores) Len() int {
	return len(r.scores)
}

type sortObjectsByScore struct{}

func newScoresSorter() *sortObjectsByScore {
	return &sortObjectsByScore{}
}

func (s *sortObjectsByScore) sort(objects []*storobj.Object, scores []float32) ([]*storobj.Object, []float32) {
	sbd := &sortByScores{objects, scores}
	sort.Sort(sbd)
	return sbd.objects, sbd.scores
}
