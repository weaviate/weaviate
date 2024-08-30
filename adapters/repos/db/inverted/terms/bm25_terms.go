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
	"context"
	"encoding/binary"
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

type DocPointerWithScore struct {
	Id         uint64
	Frequency  float32
	PropLength float32
}

func (d *DocPointerWithScore) FromBytes(in []byte, boost float32) error {
	var read uint16

	keyLen := binary.LittleEndian.Uint16(in[:2])
	read += 2 // uint16 -> 2 bytes

	key := in[read : read+keyLen]
	read += keyLen

	valueLen := binary.LittleEndian.Uint16(in[read : read+2])
	read += 2

	if valueLen == 0 {
		d.Id = binary.BigEndian.Uint64(key)
		d.Frequency = 0
		d.PropLength = 0
		return nil
	}

	value := in[read : read+valueLen]
	read += valueLen

	return d.FromKeyVal(key, value, boost)
}

func (d *DocPointerWithScore) FromKeyVal(key []byte, value []byte, boost float32) error {
	d.Id = binary.BigEndian.Uint64(key)
	d.Frequency = math.Float32frombits(binary.LittleEndian.Uint32(value[:4])) * boost
	d.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(value[4:]))
	return nil
}

type SortedDocPointerWithScoreMerger struct {
	input   [][]DocPointerWithScore
	output  []DocPointerWithScore
	offsets []int
}

func NewSortedDocPointerWithScoreMerger() *SortedDocPointerWithScoreMerger {
	return &SortedDocPointerWithScoreMerger{}
}

func (s *SortedDocPointerWithScoreMerger) init(segments [][]DocPointerWithScore) error {
	s.input = segments

	// all offset pointers initialized at 0 which is where we want to start
	s.offsets = make([]int, len(segments))

	// The maximum output is the sum of all the input segments if there are only
	// unique keys and zero tombstones. If there are duplicate keys (i.e.
	// updates) or tombstones, we will slice off some elements of the output
	// later, but this way we can be sure each index will always be initialized
	// correctly
	maxOutput := 0
	for _, seg := range segments {
		maxOutput += len(seg)
	}
	s.output = make([]DocPointerWithScore, maxOutput)

	return nil
}

func (s *SortedDocPointerWithScoreMerger) findSegmentWithLowestKey() (DocPointerWithScore, bool) {
	bestSeg := -1
	bestKey := uint64(0)

	for segmentID := 0; segmentID < len(s.input); segmentID++ {
		// check if a segment is already exhausted, then skip
		if s.offsets[segmentID] >= len(s.input[segmentID]) {
			continue
		}

		currKey := s.input[segmentID][s.offsets[segmentID]].Id
		if bestSeg == -1 {
			// first time we're running, no need to compare, just set to current
			bestSeg = segmentID
			bestKey = currKey
			continue
		}

		if currKey > bestKey {
			// the segment we are currently looking at has a higher key than our
			// current best so we can completely ignore it
			continue
		}

		if currKey < bestKey {
			// the segment we are currently looking at is a better match than the
			// previous, this means, we have found a new favorite, but the previous
			// best will still be valid in a future round
			bestSeg = segmentID
			bestKey = currKey
			continue
		}

		if currKey == bestKey {
			// this the most interesting case: we are looking at a duplicate key. In
			// this case the rightmost ("latest") segment takes precedence, however,
			// we must make sure that the previous match gets discarded, otherwise we
			// will find it again in the next round.
			//
			// We can simply increase the offset before updating the bestSeg pointer,
			// which means we will never look at this element again
			s.offsets[bestSeg]++

			// now that the old element is discarded, we can update our pointers
			bestSeg = segmentID
			bestKey = currKey
		}
	}

	if bestSeg == -1 {
		// we didn't find anything, looks like we have exhausted all segments
		return DocPointerWithScore{}, false
	}

	// we can now be sure that bestSeg,bestKey is the latest version of the
	// lowest key, there is only one job left to do: increase the offset, so we
	// never find this segment again
	bestMatch := s.input[bestSeg][s.offsets[bestSeg]]
	s.offsets[bestSeg]++

	return bestMatch, true
}

func (s *SortedDocPointerWithScoreMerger) Do(ctx context.Context, segments [][]DocPointerWithScore) ([]DocPointerWithScore, error) {
	if err := s.init(segments); err != nil {
		return nil, errors.Wrap(err, "init sorted map decoder")
	}

	i := 0
	for {
		if i%100 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}

		match, ok := s.findSegmentWithLowestKey()
		if !ok {
			break
		}

		if match.Frequency == 0 { // tombstone
			// the latest version of this key was a tombstone, so we can ignore it
			continue
		}

		s.output[i] = match
		i++
	}

	return s.output[:i], nil
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
	Count() int
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
	minID, pivotPoint, abort := t.FindMinID(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.AdvanceAllAtLeast(minID)
	t.PartialSort()
	return false
}

func (t *Terms) AdvanceAllAtLeast(minID uint64) {
	for i := range t.T {
		t.T[i].AdvanceAtLeast(minID)
	}
}

func (t *Terms) FindMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t.T {
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

func (t *Terms) FindFirstNonExhausted() (int, bool) {
	for i := range t.T {
		if !t.T[i].IsExhausted() {
			return i, true
		}
	}

	return -1, false
}

func (t *Terms) ScoreNext(averagePropLength float64, config schema.BM25Config, additionalExplanations bool) (uint64, float64, []*DocPointerWithScore) {
	var docInfos []*DocPointerWithScore

	pos, ok := t.FindFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0, docInfos
	}

	if len(t.T) == 0 {
		return 0, 0, docInfos
	}

	if additionalExplanations {
		docInfos = make([]*DocPointerWithScore, t.Count)
	}

	id := t.T[pos].IdPointer()
	var cumScore float64
	for i := pos; i < len(t.T); i++ {
		if t.T[i].IdPointer() != id || t.T[i].IsExhausted() {
			continue
		}
		_, score, docInfo := t.T[i].ScoreAndAdvance(averagePropLength, config)
		if additionalExplanations {
			docInfos[t.T[i].QueryTermIndex()] = &docInfo
		}
		cumScore += score
	}

	t.FullSort()
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
