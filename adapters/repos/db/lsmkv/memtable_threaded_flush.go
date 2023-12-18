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
	"os"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (m *MemtableMulti) closeRequestChannels() {
	for _, channel := range m.requestsChannels {
		close(channel)
	}
	m.wgWorkers.Wait()
}

func (m *MemtableMulti) flush() error {
	var err error

	if err := m.CommitlogClose(); err != nil {
		return errors.Wrap(err, "close commit log file")
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.CommitlogDelete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}
		return nil
	}

	if m.strategy == StrategyRoaringSet {
		err = m.flushRoaringSet()
	} else if m.strategy == StrategyReplace {
		err = m.flushKey()
	} else if m.strategy == StrategyMapCollection {
		err = m.flushKeyMap()
	} else if m.strategy == StrategySetCollection {
		err = m.flushKeyMulti()
	} else {
		return errors.Errorf("unknown strategy %s on flush", m.strategy)
	}
	if err != nil {
		return err
	}
	return nil
}

func (m *MemtableMulti) flushKey() error {
	flat := m.flattenInOrderKey()
	totalSize := len(flat)

	f, err := os.Create(m.path + ".db")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, int(float64(totalSize)*1.3))

	keys, err := writeKey(flat, w, f, m.secondaryIndices)
	if err != nil {
		return err
	}

	return m.writeIndex(keys, w, f)
}

func (m *MemtableMulti) flushKeyMap() error {
	flat := m.flattenInOrderKeyMap()
	totalSize := len(flat)

	f, err := os.Create(m.path + ".db")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, int(float64(totalSize)*1.3))

	keys, err := writeKeyMap(flat, w, f, m.secondaryIndices, m.strategy)
	if err != nil {
		return err
	}

	return m.writeIndex(keys, w, f)
}

func (m *MemtableMulti) flushKeyMulti() error {
	flat := m.flattenInOrderKeyMulti()
	totalSize := len(flat)

	f, err := os.Create(m.path + ".db")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, int(float64(totalSize)*1.3))

	keys, err := writeKeyMulti(flat, w, f, m.secondaryIndices, m.strategy)
	if err != nil {
		return err
	}

	return m.writeIndex(keys, w, f)
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
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*binarySearchNode, totalSize)
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
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*binarySearchNodeMap, totalSize)
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
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*binarySearchNodeMulti, totalSize)
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

func writeKey(flat []*binarySearchNode, w *bufio.Writer, f *os.File, secondaryIndices uint16) ([]segmentindex.Key, error) {
	totalDataLength := totalKeyAndValueSize(flat)
	perObjectAdditions := len(flat) * (1 + 8 + 4 + int(secondaryIndices)*4) // 1 byte for the tombstone, 8 bytes value length encoding, 4 bytes key length encoding, + 4 bytes key encoding for every secondary index
	headerSize := segmentindex.HeaderSize
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + perObjectAdditions + headerSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategyReplace,
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize = int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		segNode := &segmentReplaceNode{
			offset:              totalWritten,
			tombstone:           node.tombstone,
			value:               node.value,
			primaryKey:          node.key,
			secondaryKeys:       node.secondaryKeys,
			secondaryIndexCount: secondaryIndices,
		}

		ki, err := segNode.KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func writeKeyMap(flat []*binarySearchNodeMap, w *bufio.Writer, f *os.File, secondaryIndices uint16, strategy string) ([]segmentindex.Key, error) {
	// by encoding each map pair we can force the same structure as for a
	// collection, which means we can reuse the same flushing logic
	asMulti := make([]*binarySearchNodeMulti, len(flat))
	for i, mapNode := range flat {
		asMulti[i] = &binarySearchNodeMulti{
			key:    mapNode.key,
			values: make([]value, len(mapNode.values)),
		}

		for j := range asMulti[i].values {
			enc, err := mapNode.values[j].Bytes()
			if err != nil {
				return nil, err
			}

			asMulti[i].values[j] = value{
				value:     enc,
				tombstone: mapNode.values[j].Tombstone,
			}
		}

	}
	return writeKeyMulti(asMulti, w, f, secondaryIndices, strategy)
}

func writeKeyMulti(flat []*binarySearchNodeMulti, w *bufio.Writer, f *os.File, secondaryIndices uint16, strategy string) ([]segmentindex.Key, error) {
	totalDataLength := totalValueSizeCollection(flat)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: secondaryIndices,
		Strategy:         SegmentStrategyFromString(strategy),
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		ki, err := (&segmentCollectionNode{
			values:     node.values,
			primaryKey: node.key,
			offset:     totalWritten,
		}).KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

// TODO: this code is a duplicate of the Memtable flush code; I've separated it for tests, in the future, it should be refactored so it can be called for both Memtable and MemtableThreaded
func (m *MemtableMulti) writeIndex(keys []segmentindex.Key, w *bufio.Writer, f *os.File) error {
	indices := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: m.secondaryIndices,
		ScratchSpacePath:    m.path + ".scratch.d",
	}

	if _, err := indices.WriteTo(w); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// only now that the file has been flushed is it safe to delete the commit log
	// TODO: there might be an interest in keeping the commit logs around for
	// longer as they might come in handy for replication
	err := m.CommitlogDelete()
	return err
}
