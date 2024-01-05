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

package lsmkv

import (
	"bytes"

	"github.com/pkg/errors"
)

type sortedMapMerger struct {
	input   [][]MapPair
	output  []MapPair
	offsets []int
}

func newSortedMapMerger() *sortedMapMerger {
	return &sortedMapMerger{}
}

func (s *sortedMapMerger) do(segments [][]MapPair) ([]MapPair, error) {
	if err := s.init(segments); err != nil {
		return nil, errors.Wrap(err, "init sorted map decoder")
	}

	i := 0
	for {
		match, ok := s.findSegmentWithLowestKey()
		if !ok {
			break
		}

		if match.Tombstone {
			// the latest version of this key was a tombstone, so we can ignore it
			continue
		}

		s.output[i] = match
		i++
	}

	return s.output[:i], nil
}

// same as .do() but does not remove the tombstone if the most latest version
// of a key is a tombstone. It can thus also be used in compactions
func (s *sortedMapMerger) doKeepTombstones(segments [][]MapPair) ([]MapPair, error) {
	if err := s.init(segments); err != nil {
		return nil, errors.Wrap(err, "init sorted map decoder")
	}

	i := 0
	for {
		match, ok := s.findSegmentWithLowestKey()
		if !ok {
			break
		}

		s.output[i] = match
		i++
	}

	return s.output[:i], nil
}

// same as .doKeepTombstone() but requires initialization from the outside and
// can thus reuse state from previous rounds without having to allocate again.
// must be pre-faced by a call of reset()
func (s *sortedMapMerger) doKeepTombstonesReusable() ([]MapPair, error) {
	i := 0
	for {
		match, ok := s.findSegmentWithLowestKey()
		if !ok {
			break
		}

		s.output[i] = match
		i++
	}

	return s.output[:i], nil
}

// init is automatically called by .do() or .doKeepTombstones()
func (s *sortedMapMerger) init(segments [][]MapPair) error {
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
	s.output = make([]MapPair, maxOutput)

	return nil
}

// reset can be manually called if sharing allocated state is desired, such as
// with .doKeepTombstonesReusable()
func (s *sortedMapMerger) reset(segments [][]MapPair) error {
	s.input = segments

	if cap(s.offsets) >= len(segments) {
		s.offsets = s.offsets[:len(segments)]

		// it existed before so we need to reset all offsets to 0
		for i := range s.offsets {
			s.offsets[i] = 0
		}
	} else {
		s.offsets = make([]int, len(segments), int(float64(len(segments))*1.25))
	}

	// The maximum output is the sum of all the input segments if there are only
	// unique keys and zero tombstones. If there are duplicate keys (i.e.
	// updates) or tombstones, we will slice off some elements of the output
	// later, but this way we can be sure each index will always be initialized
	// correctly
	maxOutput := 0
	for _, seg := range segments {
		maxOutput += len(seg)
	}

	if cap(s.output) >= maxOutput {
		s.output = s.output[:maxOutput]
		// no need to reset any values as all of them will be overridden anyway
	} else {
		s.output = make([]MapPair, maxOutput, int(float64(maxOutput)*1.25))
	}

	return nil
}

func (s *sortedMapMerger) findSegmentWithLowestKey() (MapPair, bool) {
	bestSeg := -1
	bestKey := []byte(nil)

	for segmentID := 0; segmentID < len(s.input); segmentID++ {
		// check if a segment is already exhausted, then skip
		if s.offsets[segmentID] >= len(s.input[segmentID]) {
			continue
		}

		currKey := s.input[segmentID][s.offsets[segmentID]].Key
		if bestSeg == -1 {
			// first time we're running, no need to compare, just set to current
			bestSeg = segmentID
			bestKey = currKey
			continue
		}

		cmp := bytes.Compare(currKey, bestKey)
		if cmp > 0 {
			// the segment we are currently looking at has a higher key than our
			// current best so we can completely ignore it
			continue
		}

		if cmp < 0 {
			// the segment we are currently looking at is a better match than the
			// previous, this means, we have found a new favorite, but the previous
			// best will still be valid in a future round
			bestSeg = segmentID
			bestKey = currKey
		}

		if cmp == 0 {
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
		return MapPair{}, false
	}

	// we can now be sure that bestSeg,bestKey is the latest version of the
	// lowest key, there is only one job left to do: increase the offset, so we
	// never find this segment again
	bestMatch := s.input[bestSeg][s.offsets[bestSeg]]
	s.offsets[bestSeg]++

	return bestMatch, true
}
