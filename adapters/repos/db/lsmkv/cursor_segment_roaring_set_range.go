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

	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
)

func (sg *SegmentGroup) newRoaringSetRangeReaders() ([]roaringsetrange.InnerReader, func()) {
	segments, release := sg.getAndLockSegments()

	readers := make([]roaringsetrange.InnerReader, len(segments))
	for i, segment := range segments {
		readers[i] = segment.newRoaringSetRangeReader()
	}

	return readers, release
}

func (s *segment) newRoaringSetRangeReader() *roaringsetrange.SegmentReader {
	var segmentCursor roaringsetrange.SegmentCursor
	if s.readFromMemory {
		segmentCursor = roaringsetrange.NewSegmentCursorMmap(s.contents[s.dataStartPos:s.dataEndPos])
	} else {
		sectionReader := io.NewSectionReader(s.contentFile, int64(s.dataStartPos), int64(s.dataEndPos))
		// since segment reader concurrenlty fetches next segment and merges bitmaps of previous segments
		// at least 2 buffers needs to be used by cursor not to overwrite data before they are consumed.
		segmentCursor = roaringsetrange.NewSegmentCursorPread(sectionReader, 2)
	}

	return roaringsetrange.NewSegmentReaderConcurrent(
		roaringsetrange.NewGaplessSegmentCursor(segmentCursor),
		concurrency.SROAR_MERGE)
}

func (s *segment) newRoaringSetRangeCursor() roaringsetrange.SegmentCursor {
	if s.readFromMemory {
		return roaringsetrange.NewSegmentCursorMmap(s.contents[s.dataStartPos:s.dataEndPos])
	}

	sectionReader := io.NewSectionReader(s.contentFile, int64(s.dataStartPos), int64(s.dataEndPos))
	// compactor does not work concurrently, next segment is fetched after previous one gets consumed,
	// therefore just one buffer is sufficient.
	return roaringsetrange.NewSegmentCursorPread(sectionReader, 1)
}
