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
	Id uint64
	// A Frequency of 0 indicates a tombstone
	Frequency  float32
	PropLength float32
}

func (d *DocPointerWithScore) FromBytes(in []byte, isTombstone bool, boost float32) error {
	if len(in) < 12 {
		return errors.Errorf("DocPointerWithScore: FromBytes: input too short, expected at least 12 bytes, got %d", len(in))
	}
	// This class is only to be used with a MapList that has fixed key and value lengths (8 and 8) for posting lists
	// Thus, we can proceed with fixed offsets, and ignore reading the key and value lengths, at offset 0 and 10
	// key will be at offset 2, value at offset 12
	return d.FromKeyVal(in[2:10], in[12:], isTombstone, boost)
}

func (d *DocPointerWithScore) FromBytesInverted(in []byte, boost float32, propLen float32) error {
	isTombstone := len(in) == 8
	d.FromKeyVal(in[0:8], in[8:], isTombstone, boost)
	d.PropLength = propLen
	return nil
}

func (d *DocPointerWithScore) FromKeyVal(key []byte, value []byte, isTombstone bool, boost float32) error {
	if len(key) != 8 {
		return errors.Errorf("DocPointerWithScore: FromKeyVal: key length must be 8, got %d", len(key))
	}

	d.Id = binary.BigEndian.Uint64(key)
	if isTombstone || len(value) < 8 { // tombstone, value length is also checked due to #4125
		// Id and Freq are automatically set to 0
		return nil
	}
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

type TermInterface interface {
	// doubles as max impact (with tf=1, the max impact would be 1*Idf), if there
	// is a boost for a queryTerm, simply apply it here once
	Idf() float64
	IdPointer() uint64
	Exhausted() bool
	Count() int
	QueryTermIndex() int
	AdvanceAtLeast(minID uint64)
	AdvanceAtLeastShallow(minID uint64)
	Advance()
	Score(averagePropLength float64, additionalExplanations bool) (uint64, float64, *DocPointerWithScore)
	CurrentBlockImpact() float32
	CurrentBlockMaxId() uint64
}

type Term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*Idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer      uint64
	posPointer     uint64
	Data           []DocPointerWithScore
	exhausted      bool
	queryTerm      string
	queryTermIndex int
	propertyBoost  float64
	config         schema.BM25Config
}

func NewTerm(queryTerm string, queryTermIndex int, propertyBoost float32, config schema.BM25Config) *Term {
	return &Term{
		queryTerm:      queryTerm,
		queryTermIndex: queryTermIndex,
		propertyBoost:  float64(propertyBoost),
		config:         config,
	}
}

func (t *Term) Score(averagePropLength float64, additionalExplanations bool) (uint64, float64, *DocPointerWithScore) {
	pair := t.Data[t.posPointer]
	freq := float64(pair.Frequency)
	tf := freq / (freq + t.config.K1*(1-t.config.B+t.config.B*float64(pair.PropLength)/averagePropLength))
	if !additionalExplanations {
		return t.idPointer, tf * t.idf * t.propertyBoost, nil
	}
	return t.idPointer, tf * t.idf * t.propertyBoost, &pair
}

func (t *Term) Advance() {
	t.posPointer++
	if t.posPointer >= uint64(len(t.Data)) {
		t.exhausted = true
		t.idPointer = math.MaxUint64 // force them to the end of the term list
	} else {
		t.idPointer = t.Data[t.posPointer].Id
	}
}

func (t *Term) AdvanceAtLeast(minID uint64) {
	for t.idPointer < minID {
		t.posPointer++
		if t.posPointer >= uint64(len(t.Data)) {
			t.exhausted = true
			t.idPointer = math.MaxUint64 // force them to the end of the term list
			return
		}
		t.idPointer = t.Data[t.posPointer].Id
	}
}

func (t *Term) AdvanceAtLeastShallow(minID uint64) {
	t.AdvanceAtLeast(minID)
	// go back one document, as the advance blockmax implementation relies on going to the document right before on a shallow advance,
	// due to the way decoding works in the SegmentBlockMax implementation
	t.posPointer--
	t.exhausted = false
	t.idPointer = t.Data[t.posPointer].Id
}

func (t *Term) Count() int {
	return len(t.Data)
}

func (t *Term) Idf() float64 {
	return t.idf
}

func (t *Term) IdPointer() uint64 {
	return t.idPointer
}

func (t *Term) PosPointer() uint64 {
	return t.posPointer
}

func (t *Term) Exhausted() bool {
	return t.exhausted
}

func (t *Term) QueryTerm() string {
	return t.queryTerm
}

func (t *Term) QueryTermIndex() int {
	return t.queryTermIndex
}

func (t *Term) SetIdf(idf float64) {
	t.idf = idf
}

func (t *Term) SetPosPointer(posPointer uint64) {
	t.posPointer = posPointer
}

func (t *Term) SetIdPointer(idPointer uint64) {
	t.idPointer = idPointer
}

func (t *Term) CurrentBlockImpact() float32 {
	return float32(t.idf * t.propertyBoost)
}

func (t *Term) CurrentBlockMaxId() uint64 {
	return t.idPointer
}

type Terms struct {
	T     []TermInterface
	Count int
}

func (t *Terms) CompletelyExhausted() bool {
	for i := range t.T {
		if !t.T[i].Exhausted() {
			return false
		}
	}
	return true
}

func (t *Terms) FindMinIDWand(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t.T {
		if term.Exhausted() {
			continue
		}
		cumScore += term.Idf()
		if cumScore >= minScore {
			return term.IdPointer(), i, false
		}
	}

	return 0, 0, true
}

func (t *Terms) Pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.FindMinIDWand(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.AdvanceAllAtLeast(minID, len(t.T)-1)

	// we don't need to sort the entire list, just the first pivotPoint elements
	t.SortFirst()

	return false
}

func (t *Terms) AdvanceAllAtLeast(minID uint64, pivot int) {
	for i := range t.T[:pivot] {
		t.T[i].AdvanceAtLeast(minID)
	}
}

func (t *Terms) FindMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)
	for i, term := range t.T {
		if term.Exhausted() {
			continue
		}
		cumScore += float64(term.CurrentBlockImpact())
		if cumScore >= minScore {
			// find if there is another term with the same id
			for j := i + 1; j < len(t.T); j++ {
				if t.T[j].IdPointer() != term.IdPointer() {
					return t.T[j-1].IdPointer(), j - 1, false
				}
			}
			return t.T[len(t.T)-1].IdPointer(), len(t.T) - 1, false
		}
	}

	return 0, 0, true
}

func (t *Terms) FindFirstNonExhausted() (int, bool) {
	for i := range t.T {
		if !t.T[i].Exhausted() {
			return i, true
		}
	}

	return -1, false
}

func (t *Terms) ScoreNext(averagePropLength float64, additionalExplanations bool, minimumOrTokensMatch int) (uint64, float64, []*DocPointerWithScore, bool) {
	var docInfos []*DocPointerWithScore

	pos, ok := t.FindFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0, docInfos, false
	}

	if len(t.T) == 0 {
		return 0, 0, docInfos, false
	}

	if additionalExplanations {
		docInfos = make([]*DocPointerWithScore, t.Count)
	}

	id := t.T[pos].IdPointer()
	var cumScore float64

	matchedTerms := 0

	if len(t.T)-pos < minimumOrTokensMatch {
		return 0, 0, docInfos, false
	}

	for i := pos; i < len(t.T); i++ {
		if t.T[i].IdPointer() != id || t.T[i].Exhausted() {
			continue
		}
		matchedTerms++
		term := t.T[i]
		_, score, docInfo := term.Score(averagePropLength, additionalExplanations)
		term.Advance()
		if additionalExplanations {
			docInfos[term.QueryTermIndex()] = docInfo
		}
		cumScore += score
	}

	if matchedTerms < minimumOrTokensMatch {
		// not enough terms matched, return 0
		return 0, 0, docInfos, false
	}

	// t.FullSort()
	return id, cumScore, docInfos, true
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

func (t *Terms) SortFull() {
	sort.Sort(t)
}

func (t *Terms) SortFirst() {
	min := uint64(0)
	minIndex := -1
	for i := 0; i < len(t.T); i++ {
		if minIndex == -1 || (t.T[i].IdPointer() < min && !t.T[i].Exhausted()) {
			min = t.T[i].IdPointer()
			minIndex = i
		}
	}
	if minIndex > 0 {
		t.T[0], t.T[minIndex] = t.T[minIndex], t.T[0]
	}
}

func (t *Terms) SortPartial(nextList int) {
	for i := nextList + 1; i < len(t.T); i++ {
		if t.T[i].IdPointer() <= t.T[i-1].IdPointer() {
			// swap
			t.T[i], t.T[i-1] = t.T[i-1], t.T[i]
		} else {
			break
		}
	}
}

func (t *Terms) GetBlockUpperBound(pivot int, pivotId uint64) float32 {
	blockMaxScore := float32(0)
	for i := 0; i < pivot+1; i++ {
		if t.T[i].Exhausted() {
			continue
		}
		if t.T[i].CurrentBlockMaxId() < pivotId {
			t.T[i].AdvanceAtLeastShallow(pivotId)
		}
		blockMaxScore += t.T[i].CurrentBlockImpact()
	}
	return blockMaxScore
}
