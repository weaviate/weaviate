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
	merged, err := mergeNodes(nodes)
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
	merged, err := mergeNodes(nodes)
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
	merged, err := mergeNodes(nodes)
	if err != nil {
		panic(err)
	}
	return merged
}

type Keyer interface {
	Key() []byte
	IsNil() bool
}

func (n *binarySearchNode) Key() []byte {
	return n.key
}

func (n *binarySearchNodeMap) Key() []byte {
	return n.key
}

func (n *binarySearchNodeMulti) Key() []byte {
	return n.key
}

func mergeNodes[T Keyer](metaNodes [][]T) ([]T, error) {
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0

	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]T, totalSize)
	mergedNodesIndex := 0

	for {
		var smallestNode T
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index >= len(metaNodes[i]) {
				continue
			}
			if smallestNode.IsNil() || bytes.Compare(metaNodes[i][index].Key(), smallestNode.Key()) < 0 {
				smallestNode = metaNodes[i][index]
				smallestNodeIndex = i
			} else if !smallestNode.IsNil() && bytes.Equal(metaNodes[i][index].Key(), smallestNode.Key()) {
				// smallestNode.value = append(smallestNode.value, metaNodes[i][index].value...)
				indices[i]++
			}

		}
		if smallestNode.IsNil() {
			break
		}
		flat[mergedNodesIndex] = smallestNode
		mergedNodesIndex++
		indices[smallestNodeIndex]++
	}

	// fmt.Printf("Merged %d nodes into %d nodes\n", totalSize, mergedNodesIndex)
	return flat[:mergedNodesIndex], nil
}
