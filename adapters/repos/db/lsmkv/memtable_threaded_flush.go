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

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *MemtableThreaded) flush() error {
	if m.baseline != nil {
		return m.baseline.flush()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedFlush,
		}, true, "Flush")
		return output.error
	}
}

func (m *MemtableThreaded) getNodesRoaringSet() []*roaringset.BinarySearchNode {
	if m.baseline != nil {
		return m.baseline.RoaringSet().FlattenInOrder()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetFlattenInOrder,
		}, true, "RoaringSetFlattenInOrder")
		return output.nodes
	}
}
