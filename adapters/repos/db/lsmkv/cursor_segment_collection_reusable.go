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
	"errors"
	"io"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type segmentCursorCollectionReusable struct {
	cache   *cacheReader
	nodeBuf segmentCollectionNode
}

func (s *segment) newCollectionCursorReusable() *segmentCursorCollectionReusable {
	return &segmentCursorCollectionReusable{
		cache: newCacheReader(s),
	}
}

func (s *segmentCursorCollectionReusable) next() ([]byte, []value, error) {
	if err := s.cache.CheckPosition(); err != nil {
		return nil, nil, err
	}
	return s.parseCollectionNodeInto()
}

func (s *segmentCursorCollectionReusable) first() ([]byte, []value, error) {
	s.cache.Reset()
	if err := s.cache.CheckPosition(); err != nil {
		return nil, nil, err
	}
	return s.parseCollectionNodeInto()
}

func (s *segmentCursorCollectionReusable) parseCollectionNodeInto() ([]byte, []value, error) {
	err := ParseCollectionNodeInto(s.cache, &s.nodeBuf)
	if err != nil {
		return s.nodeBuf.primaryKey, nil, err
	}

	return s.nodeBuf.primaryKey, s.nodeBuf.values, nil
}

type cacheReader struct {
	readCache         []byte
	positionInCache   uint64
	segment           *segment
	positionInSegment uint64
}

func newCacheReader(s *segment) *cacheReader {
	cacheSize := uint64(4096)
	if s.dataEndPos-s.dataStartPos < cacheSize {
		cacheSize = s.dataEndPos - s.dataStartPos
	}

	return &cacheReader{
		readCache:         make([]byte, 0, cacheSize),
		segment:           s,
		positionInSegment: s.dataStartPos,
	}
}

func (c *cacheReader) CheckPosition() error {
	if c.positionInSegment >= c.segment.dataEndPos {
		return lsmkv.NotFound
	}
	return nil
}

func (c *cacheReader) Reset() {
	c.positionInCache = 0
	c.positionInSegment = c.segment.dataStartPos
	c.readCache = c.readCache[:0] // forces a new read
}

func (c *cacheReader) Read(p []byte) (n int, err error) {
	length := uint64(len(p))
	if c.positionInSegment+length > c.segment.dataEndPos {
		return 0, lsmkv.NotFound
	}
	if c.positionInCache+length > uint64(len(c.readCache)) {
		if err := c.loadDataIntoCache(len(p)); err != nil {
			return 0, err
		}
	}
	copy(p, c.readCache[c.positionInCache:c.positionInCache+length])

	c.positionInSegment += length
	c.positionInCache += length

	return len(p), nil
}

func (c *cacheReader) loadDataIntoCache(readLength int) error {
	at, err := c.segment.newNodeReader(nodeOffset{start: c.positionInSegment}, "CursorCollectionReusable")
	if err != nil {
		return err
	}

	// Restore the original buffer capacity before reading
	c.readCache = c.readCache[:cap(c.readCache)]

	if readLength > len(c.readCache) {
		c.readCache = make([]byte, readLength)
	}

	read, err := at.Read(c.readCache)
	if err != nil && (!errors.Is(err, io.EOF) || read == 0) {
		return err
	}
	if read < readLength {
		return lsmkv.NotFound
	}

	c.readCache = c.readCache[:read]
	c.positionInCache = 0
	return nil
}
