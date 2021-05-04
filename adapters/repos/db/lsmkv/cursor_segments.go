package lsmkv

import (
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type segmentCursor struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newCursor() *segmentCursor {
	return &segmentCursor{
		segment: s,
	}
}

func (s *SegmentGroup) newCursors() []innerCursor {
	out := make([]innerCursor, len(s.segments))

	for i, segment := range s.segments {
		out[i] = segment.newCursor()
	}

	return out
}

func (s *segmentCursor) seek(key []byte) ([]byte, []byte, error) {
	node, err := s.segment.index.Seek(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, nil, NotFound
		}

		return nil, nil, err
	}

	parsed, err := s.segment.replaceStratParseDataWithKey(
		s.segment.contents[node.Start:node.End])
	if err != nil {
		return nil, nil, err
	}

	s.nextOffset = node.End

	return parsed.key, parsed.value, nil
}

func (s *segmentCursor) next() ([]byte, []byte, error) {
	if s.nextOffset >= s.segment.dataEndPos {
		return nil, nil, NotFound
	}
	parsed, err := s.segment.replaceStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])
	if err != nil {
		return nil, nil, err
	}

	s.nextOffset = s.nextOffset + uint64(parsed.read)

	return parsed.key, parsed.value, nil
}

func (s *segmentCursor) first() ([]byte, []byte, error) {
	s.nextOffset = s.segment.dataStartPos
	parsed, err := s.segment.replaceStratParseDataWithKey(
		s.segment.contents[s.nextOffset:])
	if err != nil {
		return nil, nil, err
	}

	s.nextOffset = s.nextOffset + uint64(parsed.read)

	return parsed.key, parsed.value, nil
}
