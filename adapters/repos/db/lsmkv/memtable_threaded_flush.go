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
)

func (m *MemtableMulti) closeRequestChannels() {
	for _, channel := range m.requestsChannels {
		close(channel)
	}
	m.wgWorkers.Wait()
}

func (m *MemtableMulti) flush() error {
	return flush(m)
}

func (m *MemtableMulti) flattenInOrderKey() []*binarySearchNode {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			nodesKey: m.flattenInOrderKey(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var nodes [][]*binarySearchNode
	for _, response := range results {
		nodes = append(nodes, response.nodesKey)
	}
	merged, err := mergeKeyNodes(nodes)
	if err != nil {
		panic(err)
	}
	return merged
}

func (m *MemtableMulti) flattenInOrderKeyMap() []*binarySearchNodeMap {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			nodesMap: m.flattenInOrderKeyMap(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var nodes [][]*binarySearchNodeMap
	for _, response := range results {
		nodes = append(nodes, response.nodesMap)
	}
	merged, err := mergeMapNodes(nodes)
	if err != nil {
		panic(err)
	}
	return merged
}

func (m *MemtableMulti) flattenInOrderKeyMulti() []*binarySearchNodeMulti {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			nodesMulti: m.flattenInOrderKeyMulti(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var nodes [][]*binarySearchNodeMulti
	for _, response := range results {
		nodes = append(nodes, response.nodesMulti)
	}
	merged, err := mergeMultiNodes(nodes)
	if err != nil {
		panic(err)
	}
	return merged
}

// TODO: this code is mostly duplicate for all node types, and it should be refactored with generics
func mergeKeyNodes(metaNodes [][]*binarySearchNode) ([]*binarySearchNode, error) {
	numBuckets, indices, flat := prepareIteration(metaNodes)
	mergedNodesIndex := 0

	for {
		var smallestNode *binarySearchNode
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index < len(metaNodes[i]) {
				if smallestNode == nil || bytes.Compare(metaNodes[i][index].key, smallestNode.key) < 0 {
					smallestNode = metaNodes[i][index]
					smallestNodeIndex = i
				} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].key, smallestNode.key) {
					// smallestNode.value = append(smallestNode.value, metaNodes[i][index].value...)
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

func mergeMapNodes(metaNodes [][]*binarySearchNodeMap) ([]*binarySearchNodeMap, error) {
	numBuckets, indices, flat := prepareIteration(metaNodes)
	mergedNodesIndex := 0

	for {
		var smallestNode *binarySearchNodeMap
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index < len(metaNodes[i]) {
				if smallestNode == nil || bytes.Compare(metaNodes[i][index].key, smallestNode.key) < 0 {
					smallestNode = metaNodes[i][index]
					smallestNodeIndex = i
				} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].key, smallestNode.key) {
					// smallestNode.values = append(smallestNode.values, metaNodes[i][index].values...)
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

func mergeMultiNodes(metaNodes [][]*binarySearchNodeMulti) ([]*binarySearchNodeMulti, error) {
	numBuckets, indices, flat := prepareIteration(metaNodes)
	mergedNodesIndex := 0

	for {
		var smallestNode *binarySearchNodeMulti
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index < len(metaNodes[i]) {
				if smallestNode == nil || bytes.Compare(metaNodes[i][index].key, smallestNode.key) < 0 {
					smallestNode = metaNodes[i][index]
					smallestNodeIndex = i
				} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].key, smallestNode.key) {
					// smallestNode.values = append(smallestNode.values, metaNodes[i][index].values...)
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

func prepareIteration[T any](metaNodes [][]*T) (int, []int, []*T) {
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*T, totalSize)
	return numBuckets, indices, flat
}
