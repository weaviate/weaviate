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

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"

func (s *segment) newRoaringSetCursor() roaringset.InnerCursor {
	return roaringset.NewSegmentCursor(s.contents[s.dataStartPos:s.dataEndPos], s.index)
}

func (sg *SegmentGroup) newRoaringSetCursors() ([]roaringset.InnerCursor, func()) {
	sg.maintenanceLock.RLock()
	out := make([]roaringset.InnerCursor, len(sg.segments))

	for i, segment := range sg.segments {
		out[i] = segment.newRoaringSetCursor()
	}

	return out, sg.maintenanceLock.RUnlock
}
