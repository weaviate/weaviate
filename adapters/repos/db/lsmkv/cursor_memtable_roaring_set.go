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

func (m *Memtable) newRoaringSetCursor() roaringset.InnerCursor {
	m.RLock()
	defer m.RUnlock()

	// Since FlattenInOrder makes deep copy of bst's nodes,
	// no further memtable's locking in required on cursor's methods
	return roaringset.NewBinarySearchTreeCursor(m.roaringSet)
}

// newSealedRoaringSetCursor reads this memtable without the copy
// [Memtable.newRoaringSetCursor] makes. It is safe only once the memtable is out
// of active use and its writers have drained; no lock here could make it so.
func (m *Memtable) newSealedRoaringSetCursor() roaringset.InnerCursor {
	return roaringset.NewBinarySearchTreeCursorNoCopy(m.roaringSet)
}
