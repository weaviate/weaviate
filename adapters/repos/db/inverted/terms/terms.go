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

type Terms []Term

func (t Terms) CompletelyExhausted() bool {
	for i := range t {
		if !t[i].IsExhausted() {
			return false
		}
	}
	return true
}

func (t Terms) Pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.findMinID(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.advanceAllAtLeast(minID)
	sort.Sort(t)
	return false
}

func (t Terms) advanceAllAtLeast(minID uint64) {
	for i := range t {
		t[i].AdvanceAtLeast(minID)
	}
}

func (t Terms) findMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t {
		if term.IsExhausted() {
			continue
		}
		cumScore += term.IDF()
		if cumScore >= minScore {
			return term.IdPointer(), i, false
		}
	}

	return 0, 0, true
}

func (t Terms) findFirstNonExhausted() (int, bool) {
	for i := range t {
		if !t[i].IsExhausted() {
			return i, true
		}
	}

	return -1, false
}

func (t Terms) ScoreNext(averagePropLength float64, config schema.BM25Config, additionalExplanations bool) (uint64, float64, []*DocPointerWithScore) {
	pos, ok := t.findFirstNonExhausted()

	var docInfos []*DocPointerWithScore

	if additionalExplanations {
		docInfos = make([]*DocPointerWithScore, len(t))
	}

	if !ok {
		// done, nothing left to score
		return 0, 0, docInfos
	}

	id := t[pos].IdPointer()
	var cumScore float64
	for i := pos; i < len(t); i++ {
		if t[i].IdPointer() != id || t[i].IsExhausted() {
			continue
		}
		_, score, docInfo := t[i].ScoreAndAdvance(averagePropLength, config)
		if additionalExplanations {
			docInfos[t[i].QueryTermIndex()] = &docInfo
		}
		cumScore += score
	}

	sort.Sort(t) // pointer was advanced in scoreAndAdvance

	return id, cumScore, docInfos
}

// provide sort interface
func (t Terms) Len() int {
	return len(t)
}

func (t Terms) Less(i, j int) bool {
	return t[i].IdPointer() < t[j].IdPointer()
}

func (t Terms) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
