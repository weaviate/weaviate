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
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type segmentCursorMap struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newMapCursor() *segmentCursorMap {
	return &segmentCursorMap{
		segment: s,
	}
}

func (sg *SegmentGroup) newMapCursors() ([]innerCursorMap, func()) {
	segments, release := sg.getAndLockSegments()

	out := make([]innerCursorMap, len(segments))

	for i, segment := range segments {
		sgm := segment.getSegment()
		if sgm.getStrategy() == segmentindex.StrategyInverted {
			out[i] = sgm.newInvertedCursorReusable()
		} else {
			out[i] = sgm.newMapCursor()
		}
	}

	return out, release
}

func (s *segmentCursorMap) decode(parsed segmentCollectionNode) ([]MapPair, error) {
	pairs := make([]MapPair, len(parsed.values))
	for i := range pairs {
		if s.segment.strategy == segmentindex.StrategyInverted {
			if err := pairs[i].FromBytesInverted(parsed.values[i].value, false); err != nil {
				return nil, err
			}
		} else {
			if err := pairs[i].FromBytes(parsed.values[i].value, false); err != nil {
				return nil, err
			}
		}
		pairs[i].Tombstone = parsed.values[i].tombstone
	}
	return pairs, nil
}

func (s *segmentCursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		return nil, nil, err
	}

	var parsed segmentCollectionNode

	if s.segment.strategy == segmentindex.StrategyInverted {
		parsed, err = s.parseInvertedNode(nodeOffset{node.Start, node.End})
	} else {
		parsed, err = s.parseCollectionNode(nodeOffset{node.Start, node.End})
	}
	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = node.End
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	pairs, err := s.decode(parsed)
	return parsed.primaryKey, pairs, err
}

func (s *segmentCursorMap) next() ([]byte, []MapPair, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	var parsed segmentCollectionNode
	var err error

	if s.segment.strategy == segmentindex.StrategyInverted {
		parsed, err = s.parseInvertedNode(nodeOffset{start: s.nextOffset})
	} else {
		parsed, err = s.parseCollectionNode(nodeOffset{start: s.nextOffset})
	}
	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		return parsed.primaryKey, nil, err
	}

	pairs, err := s.decode(parsed)
	return parsed.primaryKey, pairs, err
}

func (s *segmentCursorMap) first() ([]byte, []MapPair, error) {
	if s.segment.dataStartPos == s.segment.dataEndPos {
		return nil, nil, lsmkv.NotFound
	}

	s.nextOffset = s.segment.dataStartPos

	var parsed segmentCollectionNode
	var err error

	if s.segment.strategy == segmentindex.StrategyInverted {
		parsed, err = s.parseInvertedNode(nodeOffset{start: s.nextOffset})
	} else {
		parsed, err = s.parseCollectionNode(nodeOffset{start: s.nextOffset})
	}
	// make sure to set the next offset before checking the error. The error
	// could be 'Deleted' which would require that the offset is still advanced
	// for the next cycle
	s.nextOffset = s.nextOffset + uint64(parsed.offset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// an empty map could have been generated due to an issue in compaction
			return nil, nil, lsmkv.NotFound
		}

		return parsed.primaryKey, nil, err
	}

	pairs, err := s.decode(parsed)
	return parsed.primaryKey, pairs, err
}

func (s *segmentCursorMap) parseCollectionNode(offset nodeOffset) (segmentCollectionNode, error) {
	r, err := s.segment.newNodeReader(offset, "segmentCursorMap")
	if err != nil {
		return segmentCollectionNode{}, err
	}
	return ParseCollectionNode(r)
}

func (s *segmentCursorMap) parseInvertedNode(offset nodeOffset) (segmentCollectionNode, error) {
	r, err := s.segment.newNodeReader(offset, "segmentCursorMap")
	if err != nil {
		return segmentCollectionNode{}, err
	}
	return ParseInvertedNode(r)
}
