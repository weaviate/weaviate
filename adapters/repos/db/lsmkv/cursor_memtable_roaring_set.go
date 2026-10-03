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

	// Since flattenInOrder makes deep copies of the index's nodes,
	// no further memtable locking is required on cursor's methods
	return roaringset.NewFlattenedNodesCursor(m.roaringSet.flattenInOrder())
}
