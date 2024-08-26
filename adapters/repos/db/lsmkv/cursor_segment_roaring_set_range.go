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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

func (s *segment) newRoaringSetRangeCursor() *roaringsetrange.SegmentCursor {
	return roaringsetrange.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos])
}

func (sg *SegmentGroup) newRoaringSetRangeCursors() ([]roaringsetrange.InnerCursor, func()) {
	sg.maintenanceLock.RLock()

	cursors := make([]roaringsetrange.InnerCursor, len(sg.segments))
	for i, segment := range sg.segments {
		cursors[i] = segment.newRoaringSetRangeCursor()
	}

	return cursors, sg.maintenanceLock.RUnlock
}
