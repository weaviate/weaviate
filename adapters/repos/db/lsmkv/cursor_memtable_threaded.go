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

func (m *MemtableThreaded) newCollectionCursor() innerCursorCollection {
	if m.baseline != nil {
		return m.baseline.newCollectionCursor()
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:     ThreadedNewCollectionCursor,
			operationName: "ThreadedNewCollectionCursor",
		})
		return output.innerCursorCollection
	}
}

func (m *MemtableThreaded) newRoaringSetCursor() roaringset.InnerCursor {
	if m.baseline != nil {
		return m.baseline.newRoaringSetCursor()
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:     ThreadedNewRoaringSetCursor,
			operationName: "ThreadedNewRoaringSetCursor",
		})
		return output.innerCursorRoaringSet
	}
}

func (m *MemtableThreaded) newMapCursor() innerCursorMap {
	if m.baseline != nil {
		return m.baseline.newMapCursor()
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:     ThreadedNewMapCursor,
			operationName: "ThreadedNewMapCursor",
		})
		return output.innerCursorMap
	}
}

func (m *MemtableThreaded) newCursor() innerCursorReplace {
	if m.baseline != nil {
		return m.baseline.newCursor()
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:     ThreadedNewCursor,
			operationName: "ThreadedNewCursor",
		})
		return output.innerCursorReplace
	}
}
