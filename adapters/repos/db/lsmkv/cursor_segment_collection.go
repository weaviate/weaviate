//nolint // TODO
package lsmkv

type segmentCursorCollection struct {
	segment    *segment
	nextOffset uint64
}

func (s *segment) newCollectionCursor() *segmentCursorCollection {
	return &segmentCursorCollection{
		segment: s,
	}
}

func (s *SegmentGroup) newCollectionCursors() []innerCursorCollection {
	out := make([]innerCursorCollection, len(s.segments))

	for i, segment := range s.segments {
		out[i] = segment.newCollectionCursor()
	}

	return out
}

func (s *segmentCursorCollection) seek(key []byte) ([]byte, []value, error) {
	panic("not implemented")
	// node, err := s.segment.index.Seek(key)
	// if err != nil {
	// 	if err == segmentindex.NotFound {
	// 		return nil, nil, NotFound
	// 	}

	// 	return nil, nil, err
	// }

	// parsed, err := s.segment.collectionStratParseDataWithKey(
	// 	s.segment.contents[node.Start:node.End])
	// if err != nil {
	// 	return parsed.key, nil, err
	// }

	// s.nextOffset = node.End

	// return parsed.key, parsed.value, nil
}

func (s *segmentCursorCollection) next() ([]byte, []value, error) {
	panic("not implemented")
	// if s.nextOffset >= s.segment.dataEndPos {
	// 	return nil, nil, NotFound
	// }

	// parsed, err := s.segment.collectionStratParseDataWithKey(
	// 	s.segment.contents[s.nextOffset:])

	// // make sure to set the next offset before checking the error. The error
	// // could be 'Deleted' which would require that the offset is still advanced
	// // for the next cycle
	// s.nextOffset = s.nextOffset + uint64(parsed.read)
	// if err != nil {
	// 	return parsed.key, nil, err
	// }

	// return parsed.key, parsed.value, nil
}

func (s *segmentCursorCollection) first() ([]byte, []value, error) {
	panic("not implemented")
	// s.nextOffset = s.segment.dataStartPos
	// parsed, err := s.segment.collectionStratParseDataWithKey(
	// 	s.segment.contents[s.nextOffset:])
	// if err != nil {
	// 	return nil, nil, err
	// }

	// s.nextOffset = s.nextOffset + uint64(parsed.read)

	// return parsed.key, parsed.value, nil
}
