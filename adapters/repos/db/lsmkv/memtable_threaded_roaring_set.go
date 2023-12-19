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
	"bytes"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *MemtableMulti) roaringSetAddOne(key []byte, value uint64) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetAddOne(key, value)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetAddList(key []byte, values []uint64) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetAddList(key, values)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetAddBitmap(key, bm)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetRemoveOne(key []byte, value uint64) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetRemoveOne(key, value)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetRemoveList(key []byte, values []uint64) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetRemoveList(key, values)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetRemoveBitmap(key, bm)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.roaringSetAddRemoveBitmaps(key, additions, deletions)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		bitmap, err := m.roaringSetGet(key)
		return ThreadedMemtableResponse{error: err, bitmap: bitmap}
	}
	result := m.callSingleWorker(memtableOperation, key, true)
	return result.bitmap, result.error
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
			if index >= len(metaNodes[i]) {
				continue
			}

			if smallestNode == nil || bytes.Compare(metaNodes[i][index].Key, smallestNode.Key) < 0 {
				smallestNode = metaNodes[i][index]
				smallestNodeIndex = i
			} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].Key, smallestNode.Key) {
				smallestNode.Value.Additions.Or(metaNodes[i][index].Value.Additions)
				smallestNode.Value.Deletions.Or(metaNodes[i][index].Value.Deletions)
				indices[i]++
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

func (m *MemtableMulti) flattenNodesRoaringSet() []*roaringset.BinarySearchNode {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			nodesRoaring: m.RoaringSet().FlattenInOrder(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var nodes [][]*roaringset.BinarySearchNode
	for _, response := range results {
		nodes = append(nodes, response.nodesRoaring)
	}
	merged, err := mergeRoaringSets(nodes)
	if err != nil {
		panic(err)
	}
	return merged
}
