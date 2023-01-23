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
