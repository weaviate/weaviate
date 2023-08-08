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

import (
	"github.com/weaviate/weaviate/entities/lsmkv"
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

func (sg *SegmentGroup) newCursors() ([]innerCursorReplace, func()) {
	sg.maintenanceLock.RLock()
	out := make([]innerCursorReplace, len(sg.segments))

	for i, segment := range sg.segments {
		out[i] = segment.newCursor()
	}

	return out, sg.maintenanceLock.RUnlock
}

func (s *segmentCursorReplace) seek(key []byte) ([]byte, []byte, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	contents := make([]byte, node.End-node.Start)
	if err = s.segment.pread(contents, node.Start, node.End); err != nil {
		return nil, nil, err
	}

	err = s.segment.replaceStratParseDataWithKeyInto(
		contents, s.reusableNode)

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
		return nil, nil, lsmkv.NotFound
	}

	contents := make([]byte, s.segment.dataEndPos)
	if err := s.segment.pread(contents, 0, s.segment.dataEndPos); err != nil {
		return nil, nil, err
	}
	err := s.segment.replaceStratParseDataWithKeyInto(
		contents[s.nextOffset:], s.reusableNode)

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
	contents := make([]byte, s.segment.dataEndPos)
	if err := s.segment.pread(contents, 0, s.segment.dataEndPos); err != nil {
		return nil, nil, err
	}
	s.nextOffset = s.segment.dataStartPos
	err := s.segment.replaceStratParseDataWithKeyInto(
		contents[s.nextOffset:], s.reusableNode)

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
		return out, lsmkv.NotFound
	}

	contents := make([]byte, s.segment.dataEndPos)
	if err := s.segment.pread(contents, 0, s.segment.dataEndPos); err != nil {
		return segmentReplaceNode{}, err
	}
	parsed, err := s.segment.replaceStratParseDataWithKey(
		contents[s.nextOffset:])

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
	contents := make([]byte, s.segment.dataEndPos)
	if err := s.segment.pread(contents, 0, s.segment.dataEndPos); err != nil {
		return segmentReplaceNode{}, err
	}
	s.nextOffset = s.segment.dataStartPos
	parsed, err := s.segment.replaceStratParseDataWithKey(
		contents[s.nextOffset:])

	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}
