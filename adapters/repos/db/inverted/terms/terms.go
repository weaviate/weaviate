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

package terms

import (
	"sort"

	"github.com/weaviate/weaviate/entities/schema"
)

type DocPointerWithScore struct {
	Id         uint64
	Frequency  float32
	PropLength float32
}

type ById []DocPointerWithScore

func (s ById) Len() int {
	return len(s)
}

func (s ById) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func (s ById) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type Term interface {
	ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64, DocPointerWithScore)
	AdvanceAtLeast(minID uint64)
	IsExhausted() bool
	IdPointer() uint64
	IDF() float64
	QueryTerm() string
	QueryTermIndex() int
}

type Terms struct {
	T     []Term
	Count int
}

func (t *Terms) CompletelyExhausted() bool {
	for i := range t.T {
		if !t.T[i].IsExhausted() {
			return false
		}
	}
	return true
}

func (t *Terms) Pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.findMinID(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.advanceAllAtLeast(minID)
	t.PartialSort()
	return false
}

func (t *Terms) advanceAllAtLeast(minID uint64) {
	for i := range t.T {
		t.T[i].AdvanceAtLeast(minID)
	}
}

func (t *Terms) findMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)
	i := 0
	for i < len(t.T) && len(t.T) > 0 {
		term := t.T[i]
		if term.IsExhausted() {
			// remove from list
			t.T = append(t.T[:i], t.T[i+1:]...)
			continue
		}
		cumScore += term.IDF()
		if cumScore >= minScore {
			// fmt.Printf("Pivot at %v, %v\n", term.IdPointer(), term.QueryTerm())
			return term.IdPointer(), i, false
		}
		i++
	}

	return 0, 0, true
}

func (t *Terms) ScoreNext(averagePropLength float64, config schema.BM25Config, additionalExplanations bool) (uint64, float64, []*DocPointerWithScore) {
	var docInfos []*DocPointerWithScore

	if len(t.T) == 0 {
		return 0, 0, docInfos
	}

	if additionalExplanations {
		docInfos = make([]*DocPointerWithScore, t.Count)
	}

	id := t.T[0].IdPointer()
	var cumScore float64
	for i := 0; i < len(t.T); i++ {
		if t.T[i].IdPointer() != id || t.T[i].IsExhausted() {
			if t.T[i].IsExhausted() {
				// remove from list
				t.T = append(t.T[:i], t.T[i+1:]...)
				i--
			}
			continue
		}
		_, score, docInfo := t.T[i].ScoreAndAdvance(averagePropLength, config)
		if additionalExplanations {
			docInfos[t.T[i].QueryTermIndex()] = &docInfo
		}
		cumScore += score
	}

	return id, cumScore, docInfos
}

// provide sort interface
func (t *Terms) Len() int {
	return len(t.T)
}

func (t *Terms) Less(i, j int) bool {
	return t.T[i].IdPointer() < t.T[j].IdPointer()
}

func (t *Terms) Swap(i, j int) {
	t.T[i], t.T[j] = t.T[j], t.T[i]
}

func (t *Terms) FullSort() {
	sort.Sort(t)
}

func (t *Terms) PartialSort() {
	// ensure the first element is the one with the lowest id instead of doing a full sort
	if len(t.T) < 2 {
		return
	}
	min := t.T[0].IdPointer()
	minIndex := 0
	for i := 1; i < len(t.T); i++ {
		if t.T[i].IdPointer() < min {
			min = t.T[i].IdPointer()
			minIndex = i
		}
	}
	if minIndex != 0 {
		t.T[0], t.T[minIndex] = t.T[minIndex], t.T[0]
	}
}
