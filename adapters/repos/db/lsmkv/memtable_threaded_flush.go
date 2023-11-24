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
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (m *MemtableThreaded) flush() error {
	if m.baseline != nil {
		return m.baseline.flush()
	}
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) flushDataReplace(f io.Writer) ([]segmentindex.Key, error) {
	if m.baseline != nil {
		return m.baseline.flushDataReplace(f)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) flushDataSet(f io.Writer) ([]segmentindex.Key, error) {
	if m.baseline != nil {
		return m.baseline.flushDataSet(f)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) flushDataMap(f io.Writer) ([]segmentindex.Key, error) {
	if m.baseline != nil {
		return m.baseline.flushDataMap(f)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) flushDataCollection(f io.Writer,
	flat []*binarySearchNodeMulti,
) ([]segmentindex.Key, error) {
	if m.baseline != nil {
		return m.baseline.flushDataCollection(f, flat)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) flushDataRoaringSet(f io.Writer) ([]segmentindex.Key, error) {
	if m.baseline != nil {
		return m.baseline.flushDataRoaringSet(f)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) getNodesRoaringSet() []*roaringset.BinarySearchNode {
	if m.baseline != nil {
		return m.baseline.RoaringSet().FlattenInOrder()
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:     ThreadedRoaringSetFlattenInOrder,
			operationName: "ThreadedRoaringSetFlattenInOrder",
		})
		return output.nodes
	}

}
