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

package lsmkv

import (
	"context"
	"math"
	"sort"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

// propIdx lets MergedTerm.Score take one contribution per property.
type mergedSource struct {
	term    *SegmentBlockMax
	propIdx int
}

// MergedTerm merges one query term's sources across properties/segments. Unlike a
// per-property term, it is exhausted only when every source is, not on the first.
type MergedTerm struct {
	// sources are kept sorted by propIdx so the per-property collapse in Score is
	// a single contiguous-group pass with no allocation.
	sources        []mergedSource
	idPointer      uint64
	exhausted      bool
	queryTermIndex int
	totalCount     int
}

// BuildCrossPropMergedTerms groups allResults ([property][segment][term]) by
// query-term index. ok is false when some query term has no source anywhere, so
// the cross-property AND cannot match and the caller should return empty.
func BuildCrossPropMergedTerms(allResults [][][]*SegmentBlockMax, termCount int) ([]*MergedTerm, bool) {
	byTerm := make([]*MergedTerm, termCount)
	for propIdx, perProperty := range allResults {
		for _, perSegment := range perProperty {
			for _, t := range perSegment {
				if t == nil {
					continue
				}
				ti := t.QueryTermIndex()
				if ti < 0 || ti >= termCount {
					continue
				}
				if byTerm[ti] == nil {
					byTerm[ti] = &MergedTerm{queryTermIndex: ti}
				}
				mt := byTerm[ti]
				mt.sources = append(mt.sources, mergedSource{term: t, propIdx: propIdx})
				mt.totalCount += t.Count()
			}
		}
	}

	merged := make([]*MergedTerm, 0, termCount)
	for _, mt := range byTerm {
		if mt == nil {
			return nil, false
		}
		mt.sortSourcesByProp()
		mt.recompute()
		merged = append(merged, mt)
	}
	return merged, true
}

func (m *MergedTerm) sortSourcesByProp() {
	// stable so that, within a property, sources keep their segment order
	sort.SliceStable(m.sources, func(i, j int) bool {
		return m.sources[i].propIdx < m.sources[j].propIdx
	})
}

func (m *MergedTerm) recompute() {
	minID := uint64(math.MaxUint64)
	allExhausted := true
	for i := range m.sources {
		s := m.sources[i].term
		if s.exhausted {
			continue
		}
		allExhausted = false
		if s.idPointer < minID {
			minID = s.idPointer
		}
	}
	m.idPointer = minID
	m.exhausted = allExhausted
}

func (m *MergedTerm) AdvanceAtLeast(id uint64) {
	for i := range m.sources {
		s := m.sources[i].term
		if s.exhausted || s.idPointer >= id {
			continue
		}
		s.AdvanceAtLeast(id)
	}
	m.recompute()
}

func (m *MergedTerm) Advance() {
	cur := m.idPointer
	for i := range m.sources {
		s := m.sources[i].term
		if !s.exhausted && s.idPointer == cur {
			s.Advance()
		}
	}
	m.recompute()
}

// tombstoned reports whether any non-exhausted source currently at docID sees it
// as deleted. Sources span segments and properties with different tombstone
// views, so no single source can answer for the merged term.
func (m *MergedTerm) tombstoned(docID uint64) bool {
	for i := range m.sources {
		s := m.sources[i].term
		if !s.exhausted && s.idPointer == docID && s.tombstoned(docID) {
			return true
		}
	}
	return false
}

func (m *MergedTerm) Count() int {
	return m.totalCount
}

func (m *MergedTerm) IdPointer() uint64 {
	return m.idPointer
}

func (m *MergedTerm) Exhausted() bool {
	return m.exhausted
}

// Score sums one contribution per property (the largest, so two live segments
// holding the same id — which tombstone layering normally prevents — can't
// double-count). The DocPointerWithScore comes from the top-contributing property.
func (m *MergedTerm) Score(averagePropLength float64, additionalExplanations bool) (uint64, float64, *terms.DocPointerWithScore) {
	cur := m.idPointer
	total := 0.0
	var bestDoc *terms.DocPointerWithScore
	bestPropScore := math.Inf(-1)

	curProp := -1
	propMax := 0.0
	var propDoc *terms.DocPointerWithScore
	have := false

	for i := range m.sources {
		src := m.sources[i]
		s := src.term
		if s.exhausted || s.idPointer != cur {
			continue
		}
		_, sc, d := s.Score(averagePropLength, additionalExplanations)
		if !have || src.propIdx != curProp {
			if have {
				total += propMax
				if propMax > bestPropScore {
					bestPropScore = propMax
					bestDoc = propDoc
				}
			}
			curProp = src.propIdx
			propMax = sc
			propDoc = d
			have = true
		} else if sc > propMax {
			propMax = sc
			propDoc = d
		}
	}
	if have {
		total += propMax
		if propMax > bestPropScore {
			bestDoc = propDoc
		}
	}
	return cur, total, bestDoc
}

type mergedTermsBySize []*MergedTerm

func (t mergedTermsBySize) Len() int           { return len(t) }
func (t mergedTermsBySize) Less(i, j int) bool { return t[i].totalCount < t[j].totalCount }
func (t mergedTermsBySize) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// DoBlockMaxAndCrossProp is the cross-property analogue of DoBlockMaxAnd. It omits
// block-max pruning (a MergedTerm's block bounds are only aggregates); the plain
// document-at-a-time intersection is the correctness reference, pruning can come later.
func DoBlockMaxAndCrossProp(ctx context.Context, limit int, mergedTerms []*MergedTerm, averagePropLength float64,
	additionalExplanations bool, termCount int, logger logrus.FieldLogger,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit + 1)
	worstDist := float64(-10000) // tf score can be negative

	if len(mergedTerms) == 0 {
		return topKHeap
	}

	sort.Sort(mergedTermsBySize(mergedTerms))

	pivotID := uint64(0)
	iterations := 0
	ctxCheck := 0
	for {
		iterations++

		ctxCheck++
		if ctxCheck == 100000 {
			ctxCheck = 0
			if ctx != nil && ctx.Err() != nil {
				termStr := ""
				for _, mt := range mergedTerms {
					termStr += strconv.Itoa(mt.queryTermIndex) + ":" + strconv.Itoa(int(mt.idPointer)) + ":" + strconv.Itoa(mt.totalCount) + ", "
				}
				logger.WithFields(logrus.Fields{
					"iterations": iterations,
					"pivotID":    pivotID,
					"lenTerms":   len(mergedTerms),
					"terms":      termStr,
					"limit":      limit,
				}).Warnf("DoBlockMaxAndCrossProp: search timed out, returning partial results")
				return topKHeap
			}
		}

		mergedTerms[0].AdvanceAtLeast(pivotID)
		if mergedTerms[0].exhausted {
			return topKHeap
		}
		pivotID = mergedTerms[0].idPointer

		isCandidate := true
		for i := 1; i < len(mergedTerms); i++ {
			mergedTerms[i].AdvanceAtLeast(pivotID)
			if mergedTerms[i].exhausted {
				return topKHeap
			}
			if mergedTerms[i].idPointer != pivotID {
				isCandidate = false
				break
			}
		}

		if !isCandidate {
			pivotID += 1
			continue
		}

		// deferred tombstone hit (see deferTombstoneToScore): advance past the
		// candidate without scoring it.
		if deferTombstoneToScore {
			pivotTombstoned := false
			for _, mt := range mergedTerms {
				if mt.tombstoned(pivotID) {
					pivotTombstoned = true
					break
				}
			}
			if pivotTombstoned {
				for _, mt := range mergedTerms {
					mt.Advance()
				}
				continue
			}
		}

		score := 0.0
		if additionalExplanations {
			docInfos = make([]*terms.DocPointerWithScore, termCount)
		}
		for _, mt := range mergedTerms {
			_, s, d := mt.Score(averagePropLength, additionalExplanations)
			score += s
			if additionalExplanations {
				docInfos[mt.queryTermIndex] = d
			}
		}
		if topKHeap.ShouldEnqueue(float32(score), limit) {
			topKHeap.InsertAndPop(pivotID, score, limit, &worstDist, docInfos)
		}
		for _, mt := range mergedTerms {
			mt.Advance()
		}
	}
}
