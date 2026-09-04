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
	return roaringset.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos], s)
}

func (sg *SegmentGroup) newRoaringSetCursors() ([]roaringset.InnerCursor, func()) {
	segments, release := sg.getConsistentViewOfSegments()

	out := make([]roaringset.InnerCursor, len(segments))

	for i, segment := range segments {
		out[i] = segment.newRoaringSetCursor()
	}

	return out, release
}

// SeekPayloadStart rebases the index's segment-absolute start offset onto the
// payload slice newRoaringSetCursor hands out, which begins at dataStartPos. The
// index bounds every offset against that same region, so the subtraction cannot
// wrap.
func (s *segment) SeekPayloadStart(key []byte) (uint64, error) {
	start, _, err := s.index.SeekOffsets(key)
	if err != nil {
		return 0, s.reportIndexErr(err)
	}
	return start - s.dataStartPos, nil
}
