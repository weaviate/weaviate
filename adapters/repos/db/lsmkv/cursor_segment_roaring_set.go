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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (s *segment) newRoaringSetCursor() (*roaringset.SegmentCursor, error) {
	contents := make([]byte, s.dataEndPos-s.dataStartPos)
	if err := s.pread(contents, s.dataStartPos, s.dataEndPos); err != nil {
		return nil, err
	}
	return roaringset.NewSegmentCursor(contents,
		&roaringSetSeeker{s.index}), nil
}

func (sg *SegmentGroup) newRoaringSetCursors() ([]roaringset.InnerCursor, func(), error) {
	sg.maintenanceLock.RLock()
	out := make([]roaringset.InnerCursor, len(sg.segments))

	for i, seg := range sg.segments {
		curs, err := seg.newRoaringSetCursor()
		if err != nil {
			return nil, sg.maintenanceLock.RUnlock, err
		}
		out[i] = curs
	}

	return out, sg.maintenanceLock.RUnlock, nil
}

// diskIndex returns node's Start and End offsets
// taking into account HeaderSize. SegmentCursor of RoaringSet
// accepts only payload part of underlying segment content, therefore
// offsets should be adjusted and reduced by HeaderSize
type roaringSetSeeker struct {
	diskIndex diskIndex
}

func (s *roaringSetSeeker) Seek(key []byte) (segmentindex.Node, error) {
	node, err := s.diskIndex.Seek(key)
	if err != nil {
		return segmentindex.Node{}, err
	}
	return segmentindex.Node{
		Key:   node.Key,
		Start: node.Start - segmentindex.HeaderSize,
		End:   node.End - segmentindex.HeaderSize,
	}, nil
}
