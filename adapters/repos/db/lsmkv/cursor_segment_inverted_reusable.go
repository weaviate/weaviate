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

type segmentCursorInvertedReusableWithCache struct {
	segment           *segment
	readCache         []byte
	positionInSegment uint64
	positionInCache   uint64
}

func (s *segment) newInvertedCursorReusableWithCache() *segmentCursorInvertedReusableWithCache {
	cacheSize := uint64(4096)
	if s.dataEndPos-s.dataStartPos < cacheSize {
		cacheSize = s.dataEndPos - s.dataStartPos
	}
	return &segmentCursorInvertedReusableWithCache{
		segment:           s,
		readCache:         make([]byte, 0, cacheSize),
		positionInSegment: s.dataStartPos,
		positionInCache:   cacheSize, // will be reset on first read
	}
}

func (s *segmentCursorInvertedReusableWithCache) loadDataIntoCache() error {
	at, err := s.segment.newNodeReader(nodeOffset{start: s.positionInSegment}, "ReadFromSegmentInvertedReusableWithCache")
	if err != nil {
		return err
	}

	// Restore the original buffer capacity before reading
	s.readCache = s.readCache[:cap(s.readCache)]

	read, err := at.Read(s.readCache)
	if err != nil {
		return err
	}
	s.readCache = s.readCache[:read]
	s.positionInCache = 0
	return nil
}

func (s *segmentCursorInvertedReusableWithCache) getDataFromCache(length uint64, movePointers bool) ([]byte, error) {
	if s.positionInCache+length > uint64(len(s.readCache)) {
		if err := s.loadDataIntoCache(); err != nil {
			return nil, err
		}
	}

	ret := s.readCache[s.positionInCache : s.positionInCache+length]
	if movePointers {
		s.positionInSegment += length
		s.positionInCache += length
	}

	return ret, nil
}

func (s *segmentCursorInvertedReusableWithCache) next() ([]byte, []MapPair, error) {
	if s.positionInSegment >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	return s.parseInvertedNodeInto()
}

func (s *segmentCursorInvertedReusableWithCache) first() ([]byte, []MapPair, error) {
	s.positionInSegment = s.segment.dataStartPos

	if s.positionInSegment >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	return s.parseInvertedNodeInto()
}

func (s *segmentCursorInvertedReusableWithCache) parseInvertedNodeInto() ([]byte, []MapPair, error) {
	buffer, err := s.getDataFromCache(16, false)
	if err != nil {
		return nil, nil, err
	}
	docCount := binary.LittleEndian.Uint64(buffer[:8])
	end := uint64(20)
	if docCount > uint64(terms.ENCODE_AS_FULL_BYTES) {
		end = binary.LittleEndian.Uint64(buffer[8:16]) + 16
	}
	readLength := end + 4
	allBytes, err := s.getDataFromCache(readLength, true)
	if err != nil {
		return nil, nil, err
	}

	nodes, _ := decodeAndConvertFromBlocks(allBytes)

	keyLen := binary.LittleEndian.Uint32(allBytes[len(allBytes)-4:])
	key, err := s.getDataFromCache(uint64(keyLen), true)
	if err != nil {
		return nil, nil, err
	}

	return key, nodes, nil
}

type segmentCursorInvertedReusable struct {
	segment    *segment
	nextOffset uint64
	nodeBuf    binarySearchNodeMap
}

func (s *segment) newInvertedCursorReusable() *segmentCursorInvertedReusable {
	return &segmentCursorInvertedReusable{
		segment: s,
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
