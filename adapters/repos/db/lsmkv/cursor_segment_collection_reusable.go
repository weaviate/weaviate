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

	//contents := make([]byte, node.End-node.Start)
	//if err = s.segment.pread(contents, node.Start, node.End); err != nil {
	//	return nil, nil, err
	//}

	r, err := s.segment.bufferedReaderAt(node.Start)
	if err != nil {
		return nil, nil, err
	}

	/*	if len(contents) == 0 {
		return nil, nil, lsmkv.NotFound
	}*/

	err = ParseCollectionNodeInto(r, &s.nodeBuf)
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

	r, err := s.segment.bufferedReaderAt(s.nextOffset)
	if err != nil {
		return nil, nil, err
	}

	err = ParseCollectionNodeInto(r, &s.nodeBuf)
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

	r, err := s.segment.bufferedReaderAt(s.nextOffset)
	if err != nil {
		return nil, nil, err
	}

	err = ParseCollectionNodeInto(r, &s.nodeBuf)
	if err != nil {
		return s.nodeBuf.primaryKey, nil, err
	}

	s.nextOffset = s.nextOffset + uint64(s.nodeBuf.offset)

	return s.nodeBuf.primaryKey, s.nodeBuf.values, nil
}
