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
	segment       *segment
	reusableNode  *segmentReplaceNode
	index         diskIndex
	keyFn         func(n *segmentReplaceNode) []byte
	firstOffsetFn func() (uint64, error)
	nextOffsetFn  func(n *segmentReplaceNode) (uint64, error)
	currOffset    uint64
}

func (s *segment) newCursor() *segmentCursorReplace {
	cursor := &segmentCursorReplace{
		segment: s,
		index:   s.index,
		firstOffsetFn: func() (uint64, error) {
			return s.dataStartPos, nil
		},
		currOffset: s.dataStartPos,
		keyFn: func(n *segmentReplaceNode) []byte {
			return n.primaryKey
		},
		reusableNode: &segmentReplaceNode{},
	}

	cursor.nextOffsetFn = func(n *segmentReplaceNode) (uint64, error) {
		return cursor.currOffset + uint64(n.offset), nil
	}

	return cursor
}

// Note: scanning over secondary keys is sub-optimal
// i.e. no sequential scan is possible as when scanning over the primary key

func (s *segment) newCursorWithSecondaryIndex(pos int) *segmentCursorReplace {
	return &segmentCursorReplace{
		segment: s,
		index:   s.secondaryIndices[pos],
		keyFn: func(n *segmentReplaceNode) []byte {
			return n.secondaryKeys[pos]
		},
		firstOffsetFn: func() (uint64, error) {
			index := s.secondaryIndices[pos]
			n, err := index.Seek(nil)
			if err != nil {
				return 0, err
			}
			return n.Start, nil
		},
		nextOffsetFn: func(n *segmentReplaceNode) (uint64, error) {
			index := s.secondaryIndices[pos]
			next, err := index.Next(n.secondaryKeys[pos])
			if err != nil {
				return 0, err
			}
			return next.Start, nil
		},
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

func (sg *SegmentGroup) newCursorsWithSecondaryIndex(pos int) ([]innerCursorReplace, func()) {
	sg.maintenanceLock.RLock()
	out := make([]innerCursorReplace, len(sg.segments))

	for i, segment := range sg.segments {
		out[i] = segment.newCursorWithSecondaryIndex(pos)
	}

	return out, sg.maintenanceLock.RUnlock
}

func (s *segmentCursorReplace) seek(key []byte) ([]byte, []byte, error) {
	node, err := s.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}
	s.currOffset = node.Start

	err = s.parseReplaceNodeInto(nodeOffset{start: node.Start, end: node.End})
	if err != nil {
		return s.keyFn(s.reusableNode), nil, err
	}

	return s.keyFn(s.reusableNode), s.reusableNode.value, nil
}

func (s *segmentCursorReplace) next() ([]byte, []byte, error) {
	nextOffset, err := s.nextOffsetFn(s.reusableNode)
	if err != nil {
		return nil, nil, err
	}

	if nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	s.currOffset = nextOffset
	err = s.parseReplaceNodeInto(nodeOffset{start: s.currOffset})
	if err != nil {
		return s.keyFn(s.reusableNode), nil, err
	}

	return s.keyFn(s.reusableNode), s.reusableNode.value, nil
}

func (s *segmentCursorReplace) first() ([]byte, []byte, error) {
	firstOffset, err := s.firstOffsetFn()
	if err != nil {
		return nil, nil, err
	}

	s.currOffset = firstOffset

	err = s.parseReplaceNodeInto(nodeOffset{start: s.currOffset})
	if err != nil {
		return s.keyFn(s.reusableNode), nil, err
	}

	return s.keyFn(s.reusableNode), s.reusableNode.value, nil
}

func (s *segmentCursorReplace) nextWithAllKeys() (n segmentReplaceNode, err error) {
	nextOffset, err := s.nextOffsetFn(s.reusableNode)
	if err != nil {
		return n, err
	}

	if nextOffset >= s.segment.dataEndPos {
		return n, lsmkv.NotFound
	}

	s.currOffset = nextOffset

	n, err = s.parseReplaceNode(nodeOffset{start: s.currOffset})

	s.reusableNode = &n

	return n, err
}

func (s *segmentCursorReplace) firstWithAllKeys() (n segmentReplaceNode, err error) {
	firstOffset, err := s.firstOffsetFn()
	if err != nil {
		return n, err
	}

	s.currOffset = firstOffset

	n, err = s.parseReplaceNode(nodeOffset{start: s.currOffset})

	s.reusableNode = &n

	return n, err
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
