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

type segmentCursorReplace struct {
	segment      *segment
	nextOffset   uint64
	reusableNode *segmentReplaceNode
}

func (s *segment) newCursor() *segmentCursorReplace {
	return &segmentCursorReplace{
		segment:      s,
		reusableNode: &segmentReplaceNode{},
	}
}

func (s *SegmentGroup) newCursors() ([]innerCursorReplace, func()) {
	s.maintenanceLock.RLock()
	out := make([]innerCursorReplace, len(s.segments))

	for i, segment := range s.segments {
		out[i] = segment.newCursor()
	}

	return out, s.maintenanceLock.RUnlock
}

func (s *segmentCursorReplace) seek(key []byte) ([]byte, []byte, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, nil, NotFound
		}

		return nil, nil, err
	}

	err = s.segment.replaceStratParseDataWithKeyInto(
		s.segment.contents[node.Start:node.End], s.reusableNode)

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = node.End

	if err != nil {
		return s.reusableNode.primaryKey, nil, err
	}

	return s.reusableNode.primaryKey, s.reusableNode.value, nil
}

func (s *segmentCursorReplace) next() ([]byte, []byte, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, NotFound
	}

	err := s.segment.replaceStratParseDataWithKeyInto(
		s.segment.contents[s.nextOffset:], s.reusableNode)

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(s.reusableNode.offset)
	if err != nil {
		return s.reusableNode.primaryKey, nil, err
	}

	return s.reusableNode.primaryKey, s.reusableNode.value, nil
}

func (s *segmentCursorReplace) first() ([]byte, []byte, error) {
	s.nextOffset = s.segment.dataStartPos
	err := s.segment.replaceStratParseDataWithKeyInto(
		s.segment.contents[s.nextOffset:], s.reusableNode)

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(s.reusableNode.offset)
	if err != nil {
		return s.reusableNode.primaryKey, nil, err
	}

	return s.reusableNode.primaryKey, s.reusableNode.value, nil
}

func (s *segmentCursorReplace) nextWithAllKeys() (segmentReplaceNode, error) {
	out := segmentReplaceNode{}
	if s.nextOffset >= s.segment.dataEndPos {
		return out, NotFound
	}

	parsed, err := s.segment.replaceStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

func (s *segmentCursorReplace) firstWithAllKeys() (segmentReplaceNode, error) {
	s.nextOffset = s.segment.dataStartPos
	parsed, err := s.segment.replaceStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}
