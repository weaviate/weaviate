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
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type segmentCursorCollection struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newCollectionCursor() *segmentCursorCollection {
	return &segmentCursorCollection{
		segment: s,
	}
}

func (sg *SegmentGroup) newCollectionCursors() ([]innerCursorCollection, func()) {
	sg.maintenanceLock.RLock()
	out := make([]innerCursorCollection, len(sg.segments))

	for i, segment := range sg.segments {
		out[i] = segment.newCollectionCursor()
	}

	return out, sg.maintenanceLock.RUnlock
}

func (s *segmentCursorCollection) seek(key []byte) ([]byte, []value, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	parsed, err := s.parseCollectionNode(nodeOffset{node.Start, node.End})
	// make sure to set the next offset before checking the error. The error
	// could be 'entities.Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = node.End
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	return parsed.primaryKey, parsed.values, nil
}

func (s *segmentCursorCollection) next() ([]byte, []value, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	parsed, err := s.parseCollectionNode(nodeOffset{start: s.nextOffset})
	// make sure to set the next offset before checking the error. The error
	// could be 'entities.Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	return parsed.primaryKey, parsed.values, nil
}

func (s *segmentCursorCollection) first() ([]byte, []value, error) {
	s.nextOffset = s.segment.dataStartPos

	parsed, err := s.parseCollectionNode(nodeOffset{start: s.nextOffset})
	// make sure to set the next offset before checking the error. The error
	// could be 'entities.Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	return parsed.primaryKey, parsed.values, nil
}

func (s *segmentCursorCollection) parseCollectionNode(offset nodeOffset) (segmentCollectionNode, error) {
	r, err := s.segment.newNodeReader(offset)
	if err != nil {
		return segmentCollectionNode{}, err
	}

	return ParseCollectionNode(r)
}
