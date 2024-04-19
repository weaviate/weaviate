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

	err = s.parseReplaceNodeInto(nodeOffset{start: node.Start, end: node.End})
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

	err := s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset})
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
	err := s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset})
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

func (s *segmentCursorReplace) parseReplaceNodeInto(readOffset nodeOffset) error {
	contentReader, err := s.segment.contentReader.NewWithOffsetStart(readOffset.start)
	if err != nil {
		return err
	}
	offset := uint64(0)
	tombstoneByte, offset := contentReader.ReadValue(offset)
	s.reusableNode.tombstone = tombstoneByte != 0

	valueLength, offset := contentReader.ReadUint64(offset)
	if valueLength > uint64(len(s.reusableNode.value)) {
		s.reusableNode.value = make([]byte, valueLength)
	} else {
		s.reusableNode.value = s.reusableNode.value[:valueLength]
	}
	_, offset = contentReader.ReadRange(offset, valueLength, s.reusableNode.value)

	keyLength, offset := contentReader.ReadUint32(offset)
	if keyLength > uint32(len(s.reusableNode.primaryKey)) {
		s.reusableNode.primaryKey = make([]byte, keyLength)
	} else {
		s.reusableNode.primaryKey = s.reusableNode.primaryKey[:keyLength]
	}
	_, offset = contentReader.ReadRange(offset, uint64(keyLength), s.reusableNode.primaryKey)

	if s.segment.secondaryIndexCount > 0 {
		s.reusableNode.secondaryKeys = make([][]byte, s.segment.secondaryIndexCount)
	}

	var secKeyLen uint32
	for j := 0; j < int(s.segment.secondaryIndexCount); j++ {
		secKeyLen, offset = contentReader.ReadUint32(offset)
		if secKeyLen == 0 {
			continue
		}
		secKey := make([]byte, secKeyLen)
		_, offset = contentReader.ReadRange(offset, uint64(secKeyLen), secKey)
		s.reusableNode.secondaryKeys[j] = secKey
	}
	s.reusableNode.offset = int(offset)
	if s.reusableNode.tombstone {
		return lsmkv.Deleted
	}
	return nil
}
