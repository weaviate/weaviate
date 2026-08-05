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

// newSealedRoaringSetCursor reads this memtable without copying it, for callers
// that know it is no longer written to — swapped out of active use with its
// writers drained. The copy the cursor above makes is what a reader of a live
// memtable needs and what the flush path's condensing rides on; a reader of a
// sealed one pays it for nothing, and on a large memtable it dominates the read.
//
// Using this on a memtable that is still being written to would race.
func (m *Memtable) newSealedRoaringSetCursor() roaringset.InnerCursor {
	m.RLock()
	defer m.RUnlock()

	return roaringset.NewSealedBinarySearchTreeCursor(m.roaringSet)
}
