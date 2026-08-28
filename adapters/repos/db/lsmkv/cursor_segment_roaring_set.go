//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func (s *segment) newRoaringSetCursor() roaringset.SegmentCursor {
	return roaringset.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos],
		&roaringSetSeeker{index: s.index, payloadStart: s.dataStartPos})
}

func (sg *SegmentGroup) newRoaringSetCursors() ([]roaringset.InnerCursor, func()) {
	segments, release := sg.getConsistentViewOfSegments()

	out := make([]roaringset.InnerCursor, len(segments))

	for i, segment := range segments {
		out[i] = segment.newRoaringSetCursor()
	}

	return out, release
}

// roaringSetSeeker rebases the disk index's segment-absolute start offset onto
// the payload slice a roaring-set SegmentCursor is given, which begins at the
// segment's dataStartPos.
type roaringSetSeeker struct {
	index        diskIndex
	payloadStart uint64
}

func (s *roaringSetSeeker) SeekPayloadStart(key []byte) (uint64, error) {
	start, _, err := s.index.SeekOffsets(key)
	if err != nil {
		return 0, err
	}
	return start - s.payloadStart, nil
}
