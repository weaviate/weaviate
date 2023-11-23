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
	}
	return nil
}

func (m *MemtableThreaded) newRoaringSetCursor() roaringset.InnerCursor {
	if m.baseline != nil {
		return m.baseline.newRoaringSetCursor()
	}
	return nil
}

func (m *MemtableThreaded) newMapCursor() innerCursorMap {
	if m.baseline != nil {
		return m.baseline.newMapCursor()
	}
	return nil
}

func (m *MemtableThreaded) newCursor() innerCursorReplace {
	if m.baseline != nil {
		return m.baseline.newCursor()
	}
	return nil
}
