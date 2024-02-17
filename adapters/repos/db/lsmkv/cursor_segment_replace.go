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
	segment       *segment
	index         diskIndex
	keyFn         func(n *segmentReplaceNode) []byte
	firstOffsetFn func() (uint64, error)
	nextOffsetFn  func(n *segmentReplaceNode) (uint64, error)
	nextOffset    uint64
	reusableNode  *segmentReplaceNode
	reusableBORW  byteops.ReadWriter
}

func (s *segment) newCursor() *segmentCursorReplace {
	cursor := &segmentCursorReplace{
		segment: s,
		index:   s.index,
		firstOffsetFn: func() (uint64, error) {
			return s.dataStartPos, nil
		},
		keyFn: func(n *segmentReplaceNode) []byte {
			return n.primaryKey
		},
		reusableNode: &segmentReplaceNode{},
		reusableBORW: byteops.NewReadWriter(nil),
	}

	cursor.nextOffsetFn = func(n *segmentReplaceNode) (uint64, error) {
		return cursor.nextOffset + uint64(n.offset), nil
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

	err = s.parseReplaceNodeInto(nodeOffset{start: node.Start, end: node.End},
		s.segment.contents[node.Start:node.End])
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

	s.nextOffset = nextOffset

	err = s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset},
		s.segment.contents[s.nextOffset:])
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

	s.nextOffset = firstOffset

	err = s.parseReplaceNodeInto(nodeOffset{start: s.nextOffset},
		s.segment.contents[s.nextOffset:])
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

	s.nextOffset = nextOffset

	n, err = s.parseReplaceNode(nodeOffset{start: s.nextOffset})

	s.reusableNode = &n

	return n, err
}

func (s *segmentCursorReplace) firstWithAllKeys() (n segmentReplaceNode, err error) {
	firstOffset, err := s.firstOffsetFn()
	if err != nil {
		return n, err
	}

	s.nextOffset = firstOffset

	n, err = s.parseReplaceNode(nodeOffset{start: s.nextOffset})

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
