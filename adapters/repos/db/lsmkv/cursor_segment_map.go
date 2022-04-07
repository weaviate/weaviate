//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type segmentCursorMap struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newMapCursor() *segmentCursorMap {
	return &segmentCursorMap{
		segment: s,
	}
}

func (s *SegmentGroup) newMapCursors() ([]innerCursorMap, func()) {
	s.maintenanceLock.RLock()
	out := make([]innerCursorMap, len(s.segments))

	for i, segment := range s.segments {
		out[i] = segment.newMapCursor()
	}

	return out, s.maintenanceLock.RUnlock
}

func (s *segmentCursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, nil, NotFound
		}

		return nil, nil, err
	}

	parsed, err := s.segment.collectionStratParseDataWithKey(
		s.segment.contents[node.Start:node.End])
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	s.nextOffset = node.End

	pairs := make([]MapPair, len(parsed.values))
	for i := range pairs {
		if err := pairs[i].FromBytes(parsed.values[i].value, false); err != nil {
			return nil, nil, err
		}
		pairs[i].Tombstone = parsed.values[i].tombstone
	}

	return parsed.primaryKey, pairs, nil
}

func (s *segmentCursorMap) next() ([]byte, []MapPair, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, NotFound
	}

	parsed, err := s.segment.collectionStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	pairs := make([]MapPair, len(parsed.values))
	for i := range pairs {
		if err := pairs[i].FromBytes(parsed.values[i].value, false); err != nil {
			return nil, nil, err
		}
		pairs[i].Tombstone = parsed.values[i].tombstone
	}

	return parsed.primaryKey, pairs, nil
}

func (s *segmentCursorMap) first() ([]byte, []MapPair, error) {
	s.nextOffset = s.segment.dataStartPos
	parsed, err := s.segment.collectionStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	s.nextOffset = s.nextOffset + uint64(parsed.offset)

	pairs := make([]MapPair, len(parsed.values))
	for i := range pairs {
		if err := pairs[i].FromBytes(parsed.values[i].value, false); err != nil {
			return nil, nil, err
		}
		pairs[i].Tombstone = parsed.values[i].tombstone
	}

	return parsed.primaryKey, pairs, nil
}
