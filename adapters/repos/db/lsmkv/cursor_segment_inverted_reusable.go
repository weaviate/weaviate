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
	"encoding/binary"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type segmentCursorInvertedReusable struct {
	segment     *segment
	nextOffset  uint64
	nodeBuf     binarySearchNodeMap
	propLengths map[uint64]uint32
}

func (s *segment) newInvertedCursorReusable() *segmentCursorInvertedReusable {
	propLengths, err := s.GetPropertyLengths()
	if err != nil {
		return nil
	}
	return &segmentCursorInvertedReusable{
		segment:     s,
		propLengths: propLengths,
	}
}

func (s *segmentCursorInvertedReusable) seek(key []byte) ([]byte, []MapPair, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	err = s.parseInvertedNodeInto(nodeOffset{node.Start, node.End})
	if err != nil {
		return nil, nil, err
	}

	s.nextOffset = node.End

	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) next() ([]byte, []MapPair, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	err := s.parseInvertedNodeInto(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}

	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) first() ([]byte, []MapPair, error) {
	s.nextOffset = s.segment.dataStartPos

	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	err := s.parseInvertedNodeInto(nodeOffset{start: s.nextOffset})
	if err != nil {
		return nil, nil, err
	}
	return s.nodeBuf.key, s.nodeBuf.values, nil
}

func (s *segmentCursorInvertedReusable) parseInvertedNodeInto(offset nodeOffset) error {
	buffer := make([]byte, 16)
	r, err := s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}
	_, err = r.Read(buffer)
	if err != nil {
		return err
	}
	docCount := binary.LittleEndian.Uint64(buffer[:8])
	end := uint64(20)
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		end = binary.LittleEndian.Uint64(buffer[8:16]) + 16
	}
	offset.end = offset.start + end + 4

	r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}

	allBytes := make([]byte, offset.end-offset.start)

	_, err = r.Read(allBytes)
	if err != nil {
		return err
	}

	nodes, _ := decodeAndConvertFromBlocks(allBytes)

	keyLen := binary.LittleEndian.Uint32(allBytes[len(allBytes)-4:])

	offset.start = offset.end
	offset.end += uint64(keyLen)
	r, err = s.segment.newNodeReader(offset, "segmentCursorInvertedReusable")
	if err != nil {
		return err
	}

	key := make([]byte, keyLen)
	_, err = r.Read(key)
	if err != nil {
		return err
	}

	s.nodeBuf.key = key
	s.nodeBuf.values = nodes

	s.nextOffset = offset.end

	return nil
}
