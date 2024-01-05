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
	"github.com/weaviate/weaviate/usecases/byteops"
)

type segmentCursorReplace struct {
	segment      *segment
	nextOffset   uint64
	reusableNode *segmentReplaceNode
	reusableBORW byteops.ReadWriter
}

func (s *segment) newCursor() *segmentCursorReplace {
	return &segmentCursorReplace{
		segment:      s,
		reusableNode: &segmentReplaceNode{},
		reusableBORW: byteops.NewReadWriter(nil),
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

	err = s.parseReplaceNodeInto(nodeOffset{start: node.Start, end: node.End},
		s.segment.contents[node.Start:node.End])
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

	err := s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset},
		s.segment.contents[s.nextOffset:])
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
	err := s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset},
		s.segment.contents[s.nextOffset:])
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

	parsed, err := s.parseReplaceNode(nodeOffset{start: s.nextOffset})
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
	parsed, err := s.parseReplaceNode(nodeOffset{start: s.nextOffset})
	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

func (s *segmentCursorReplace) parseReplaceNode(offset nodeOffset) (segmentReplaceNode, error) {
	r, err := s.segment.newNodeReader(offset)
	if err != nil {
		return segmentReplaceNode{}, err
	}
	out, err := ParseReplaceNode(r, s.segment.secondaryIndexCount)
	if out.tombstone {
		return out, lsmkv.Deleted
	}
	return out, err
}

func (s *segmentCursorReplace) parseReplaceNodeInto(offset nodeOffset, buf []byte) error {
	if s.segment.mmapContents {
		return s.parse(buf)
	}

	r, err := s.segment.newNodeReader(offset)
	if err != nil {
		return err
	}

	err = ParseReplaceNodeIntoPread(r, s.segment.secondaryIndexCount, s.reusableNode)
	if err != nil {
		return err
	}

	if s.reusableNode.tombstone {
		return lsmkv.Deleted
	}

	return nil
}

func (s *segmentCursorReplace) parse(in []byte) error {
	if len(in) == 0 {
		return lsmkv.NotFound
	}

	s.reusableBORW.ResetBuffer(in)

	err := ParseReplaceNodeIntoMMAP(&s.reusableBORW, s.segment.secondaryIndexCount,
		s.reusableNode)
	if err != nil {
		return err
	}

	if s.reusableNode.tombstone {
		return lsmkv.Deleted
	}

	return nil
}
