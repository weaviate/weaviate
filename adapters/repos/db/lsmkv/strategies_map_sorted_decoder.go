package lsmkv

import (
	"bytes"

	"github.com/pkg/errors"
)

type sortedMapDecoder struct {
	input   [][]MapPair
	output  []MapPair
	offsets []int
}

func newSortedMapDecoder() *sortedMapDecoder {
	return &sortedMapDecoder{}
}

func (s *sortedMapDecoder) do(segments [][]value) ([]MapPair, error) {
	if len(segments) == 1 {
		return s.parseSingleSegment(segments[0])
	}

	return s.mergeSegments(segments)
}

func (s *sortedMapDecoder) parseSingleSegment(seg []value) ([]MapPair, error) {
	out := make([]MapPair, len(seg))

	i := 0
	for _, segVal := range seg {
		if segVal.tombstone {
			// in a single segment a tombstone doesn't have a meaning as there is no
			// previous segment that it could remove something from. However, we
			// still don't want to to serve the tombstoned key to the user as an
			// actual value, so we still need to skip it
			continue
		}

		if err := out[i].FromBytes(segVal.value, false); err != nil {
			return nil, err
		}
		i++
	}

	return out[:i], nil
}

func (s *sortedMapDecoder) mergeSegments(segments [][]value) ([]MapPair, error) {
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

func (s *sortedMapDecoder) init(segments [][]value) error {
	// first parse all the inputs, i.e. split them from pure byte slices into
	// map-key byte slices and map-value byte slices, this will make it much
	// simpler to determine which segment to work on and we will make sure that
	// every map pair was parsed exactly once
	s.input = make([][]MapPair, len(segments))
	for segID, seg := range segments {
		s.input[segID] = make([]MapPair, len(seg))
		for valID, val := range seg {
			if err := s.input[segID][valID].FromBytes(val.value, false); err != nil {
				return err
			}
			s.input[segID][valID].Tombstone = val.tombstone
		}
	}

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

func (s *sortedMapDecoder) findSegmentWithLowestKey() (MapPair, bool) {
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
