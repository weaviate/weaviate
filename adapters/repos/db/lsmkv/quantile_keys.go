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
)

// QuantileKeys returns an approximation of the keys that make up the specified
// quantiles. This can be used to start parallel cursors at fairly evenly
// distributed positions in the segment.
//
// To understand the approximation, checkout
// [lsmkv.segmentindex.DiskTree.QuantileKeys] that runs on each segment.
//
// Some things to keep in mind:
//
//  1. It may return fewer keys than requested (including 0) if the segment
//     contains fewer entries
//  2. It may return keys that do not exist, for example because they are
//     tombstoned. This is acceptable, as a key does not have to exist to be used
//     as part of .Seek() in a cursor.
//  3. It will never return duplicates, to make sure all parallel cursors
//     return unique values.
func (b *Bucket) QuantileKeys(q int) [][]byte {
	if q <= 0 {
		return nil
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()
return [][]byte{}
}
