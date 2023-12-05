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
	"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func ThreadedRoaringSetRemoveOne(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetRemoveOne(request.key, request.value)}
}

func ThreadedRoaringSetAddOne(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetAddOne(request.key, request.value)}
}

func ThreadedRoaringSetAddList(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetAddList(request.key, request.values)}
}

func ThreadedRoaringSetRemoveList(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetRemoveList(request.key, request.values)}
}

func ThreadedRoaringSetAddBitmap(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetAddBitmap(request.key, request.bm)}
}

func ThreadedRoaringSetRemoveBitmap(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetRemoveBitmap(request.key, request.bm)}
}

func ThreadedRoaringSetGet(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	bitmap, err := m.roaringSetGet(request.key)
	return ThreadedMemtableResponse{error: err, bitmap: bitmap}
}

func ThreadedRoaringSetAddRemoveBitmaps(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.roaringSetAddRemoveBitmaps(request.key, request.additions, request.deletions)}
}

func ThreadedFlattenInOrderRoaringSet(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{nodesRoaring: m.RoaringSet().FlattenInOrder()}
}

func ThreadedFlattenInOrderKey(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{nodesKey: m.key.flattenInOrder()}
}

func ThreadedFlattenInOrderKeyMulti(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{nodesMulti: m.keyMulti.flattenInOrder()}
}

func ThreadedFlattenInOrderKeyMap(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{nodesMap: m.keyMap.flattenInOrder()}
}

func (m *MemtableThreaded) roaringSetAddOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddOne(key, value)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetAddOne,
			key:       key,
			value:     value,
		}, false, "RoaringSetAddOne")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddList(key, values)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetAddList,
			key:       key,
			values:    values,
		}, false, "RoaringSetAddList")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddBitmap(key, bm)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetAddBitmap,
			key:       key,
			bm:        bm,
		}, false, "RoaringSetAddBitmap")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveOne(key, value)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetRemoveOne,
			key:       key,
			value:     value,
		}, false, "RoaringSetRemoveOne")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveList(key, values)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetRemoveList,
			key:       key,
			values:    values,
		}, false, "RoaringSetRemoveList")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveBitmap(key, bm)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetRemoveBitmap,
			key:       key,
			bm:        bm,
		}, false, "RoaringSetRemoveBitmap")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddRemoveBitmaps(key, additions, deletions)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetAddRemoveBitmaps,
			key:       key,
			additions: additions,
			deletions: deletions,
		}, false, "RoaringSetAddRemoveBitmaps")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if m.baseline != nil {
		return m.baseline.roaringSetGet(key)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetGet,
			key:       key,
		}, true, "RoaringSetGet")
		return output.bitmap, output.error
	}
}

func mergeRoaringSets(metaNodes [][]*roaringset.BinarySearchNode) ([]*roaringset.BinarySearchNode, error) {
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*roaringset.BinarySearchNode, totalSize)
	mergedNodesIndex := 0

	for {
		var smallestNode *roaringset.BinarySearchNode
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index < len(metaNodes[i]) {
				if smallestNode == nil || bytes.Compare(metaNodes[i][index].Key, smallestNode.Key) < 0 {
					smallestNode = metaNodes[i][index]
					smallestNodeIndex = i
				} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].Key, smallestNode.Key) {
					smallestNode.Value.Additions.Or(metaNodes[i][index].Value.Additions)
					smallestNode.Value.Deletions.Or(metaNodes[i][index].Value.Deletions)
					indices[i]++
				}
			}
		}
		if smallestNode == nil {
			break
		}
		flat[mergedNodesIndex] = smallestNode
		mergedNodesIndex++
		indices[smallestNodeIndex]++
	}

	// fmt.Printf("Merged %d nodes into %d nodes\n", totalSize, mergedNodesIndex)
	return flat[:mergedNodesIndex], nil
}

func writeRoaringSet(flat []*roaringset.BinarySearchNode, f *bufio.Writer) ([]segmentindex.Key, error) {
	totalDataLength := totalPayloadSizeRoaringSet(flat)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0,
		Version:          0,
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSet,
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		sn, err := roaringset.NewSegmentNode(node.Key, node.Value.Additions,
			node.Value.Deletions)
		if err != nil {
			return nil, fmt.Errorf("create segment node: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(f, totalWritten)
		if err != nil {
			return nil, fmt.Errorf("write node %d: %w", i, err)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}
	return keys, nil
}

func (m *MemtableThreaded) flattenNodesRoaringSet() []*roaringset.BinarySearchNode {
	if m.baseline != nil {
		return m.baseline.RoaringSet().FlattenInOrder()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedFlattenInOrderRoaringSet,
		}, true, "FlattenInOrderRoaringSet")
		return output.nodesRoaring
	}
}

func (m *MemtableThreaded) flushRoaringSet() error {
	flat := m.flattenNodesRoaringSet()
	totalSize := len(flat)

	f, err := os.Create(m.path + ".db")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, int(float64(totalSize)*1.3))

	keys, err := writeRoaringSet(flat, w)
	if err != nil {
		return err
	}

	return m.writeIndex(keys, w, f)
}
