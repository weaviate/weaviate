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

type segmentCursorCollectionReusable struct {
	segment    *segment
	nextOffset uint64
	nodeBuf    segmentCollectionNode
}

func (s *segment) newCollectionCursorReusable() *segmentCursorCollectionReusable {
	return &segmentCursorCollectionReusable{
		segment: s,
	}
}

func (s *segmentCursorCollectionReusable) seek(key []byte) ([]byte, []value, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	err = s.parseCollectionNodeInto(nodeOffset{node.Start, node.End})
	if err != nil {
		return s.nodeBuf.primaryKey, nil, err
	}

	s.nextOffset = node.End

	return s.nodeBuf.primaryKey, s.nodeBuf.values, nil
}

func (s *segmentCursorCollectionReusable) next() ([]byte, []value, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	err := s.parseCollectionNodeInto(nodeOffset{start: s.nextOffset})
	// make sure to set the next offset before checking the error. The error
	// could be 'entities.Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(s.nodeBuf.offset)
	if err != nil {
		return s.nodeBuf.primaryKey, nil, err
	}

	return s.nodeBuf.primaryKey, s.nodeBuf.values, nil
}

func (s *segmentCursorCollectionReusable) first() ([]byte, []value, error) {
	s.nextOffset = s.segment.dataStartPos

	err := s.parseCollectionNodeInto(nodeOffset{start: s.nextOffset})
	if err != nil {
		return s.nodeBuf.primaryKey, nil, err
	}

	s.nextOffset = s.nextOffset + uint64(s.nodeBuf.offset)

	return s.nodeBuf.primaryKey, s.nodeBuf.values, nil
}

func (s *segmentCursorCollectionReusable) parseCollectionNodeInto(offset nodeOffset) error {
	r, err := s.segment.newNodeReader(offset)
	if err != nil {
		return err
	}

	return ParseCollectionNodeInto(r, &s.nodeBuf)
}
