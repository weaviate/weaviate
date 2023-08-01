//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import "github.com/weaviate/weaviate/entities/lsmkv"

type segmentCursorMap struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newMapCursor() *segmentCursorMap {
	return &segmentCursorMap{
		segment: s,
	}
}

func (sg *SegmentGroup) newMapCursors() ([]innerCursorMap, func()) {
	sg.maintenanceLock.RLock()
	out := make([]innerCursorMap, len(sg.segments))

	for i, segment := range sg.segments {
		out[i] = segment.newMapCursor()
	}

	return out, sg.maintenanceLock.RUnlock
}

func (s *segmentCursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	r, err := s.segment.newNodeReader(nodeOffset{node.Start, node.End})
	if err != nil {
		return nil, nil, err
	}

	parsed, err := ParseCollectionNode(r)
	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = node.End
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

func (s *segmentCursorMap) next() ([]byte, []MapPair, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	r, err := s.segment.newNodeReader(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}

	parsed, err := ParseCollectionNode(r)
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

	r, err := s.segment.newNodeReader(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}

	parsed, err := ParseCollectionNode(r)
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
