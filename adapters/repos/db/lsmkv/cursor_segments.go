package lsmkv

import "github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"

type segmentCursor struct {
	segment *segment
}

func (s *segment) newCursor() *segmentCursor {
	return &segmentCursor{
		segment: s,
	}
}

func (s *SegmentGroup) newCursors() []*segmentCursor {
	out := make([]*segmentCursor, len(s.segments))

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

	value, err := s.segment.parseReplaceData(s.segment.contents[node.Start:node.End])
	if err != nil {
		return nil, nil, err
	}

	foundKey := []byte("missing")
	return foundKey, value, nil
}
