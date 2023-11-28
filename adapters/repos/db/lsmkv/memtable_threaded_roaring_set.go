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
	"fmt"
	"math/rand"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func ThreadedRoaringSetRemoveOne(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetRemoveOne(request.key, request.value)}
}

func ThreadedRoaringSetAddOne(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetAddOne(request.key, request.value)}
}

func ThreadedRoaringSetAddList(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetAddList(request.key, request.values)}
}

func ThreadedRoaringSetRemoveList(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetRemoveList(request.key, request.values)}
}

func ThreadedRoaringSetAddBitmap(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetAddBitmap(request.key, request.bm)}
}

func ThreadedRoaringSetRemoveBitmap(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetRemoveBitmap(request.key, request.bm)}
}

func ThreadedRoaringSetGet(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	bitmap, err := m.roaringSetGet(request.key)
	return ThreadedBitmapResponse{error: err, bitmap: bitmap}
}

func ThreadedRoaringSetAddRemoveBitmaps(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{error: m.roaringSetAddRemoveBitmaps(request.key, request.additions, request.deletions)}
}

func ThreadedRoaringSetFlattenInOrder(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{nodes: m.RoaringSet().FlattenInOrder()}
}

func (m *MemtableThreaded) roaringOperation(data ThreadedBitmapRequest, needOutput bool, operationName string) ThreadedBitmapResponse {
	if operationName == "ThreadedRoaringSetFlattenInOrder" {
		// send request to all workers
		var nodes [][]*roaringset.BinarySearchNode
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedBitmapResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			nodes = append(nodes, response.nodes)
		}
		merged, err := mergeRoaringSets(nodes)
		return ThreadedBitmapResponse{nodes: merged, error: err}
		// TODO: everything below this line is a terrible idea
	} else if operationName == "ThreadedNewCollectionCursor" {
		// send request to all workers
		var cursors []innerCursorCollection
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedBitmapResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorCollection)
		}
		return ThreadedBitmapResponse{innerCursorCollection: cursors[0]}
	} else if operationName == "ThreadedNewRoaringSetCursor" {
		// send request to all workers
		var cursors []roaringset.InnerCursor
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedBitmapResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorRoaringSet)
		}
		return ThreadedBitmapResponse{innerCursorRoaringSet: cursors[0]}
	} else if operationName == "ThreadedNewMapCursor" {
		// send request to all workers
		var cursors []innerCursorMap
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedBitmapResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorMap)
		}
		return ThreadedBitmapResponse{innerCursorMap: cursors[0]}
	} else if operationName == "ThreadedNewCursor" {
		// send request to all workers
		var cursors []innerCursorReplace
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedBitmapResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorReplace)
		}
		return ThreadedBitmapResponse{innerCursorReplace: cursors[0]}
	}

	key := data.key
	workerID := 0
	if m.workerAssignment == "single-channel" {
		workerID = 0
	} else if m.workerAssignment == "random" {
		workerID = rand.Intn(m.numWorkers)
	} else if m.workerAssignment == "hash" {
		workerID = threadHashKey(key, m.numWorkers)
	} else {
		panic("invalid worker assignment")
	}
	if needOutput {
		responseCh := make(chan ThreadedBitmapResponse)
		data.response = responseCh
		m.requestsChannels[workerID] <- data
		return <-responseCh
	} else {
		data.response = nil
		m.requestsChannels[workerID] <- data
		return ThreadedBitmapResponse{}
	}
}

func (m *MemtableThreaded) roaringSetAddOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddOne(key, value)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddOne,
			key:       key,
			value:     value,
		}, false, "ThreadedRoaringSetAddOne")
		return output.error
	}

}

func (m *MemtableThreaded) roaringSetAddList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddList(key, values)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddList,
			key:       key,
			values:    values,
		}, false, "ThreadedRoaringSetAddList")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddBitmap(key, bm)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddBitmap,
			key:       key,
			bm:        bm,
		}, false, "ThreadedRoaringSetAddBitmap")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveOne(key, value)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetRemoveOne,
			key:       key,
			value:     value,
		}, false, "ThreadedRoaringSetRemoveOne")
		return output.error
	}

}

func (m *MemtableThreaded) roaringSetRemoveList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveList(key, values)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetRemoveList,
			key:       key,
			values:    values,
		}, false, "ThreadedRoaringSetRemoveList")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveBitmap(key, bm)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetRemoveBitmap,
			key:       key,
			bm:        bm,
		}, false, "ThreadedRoaringSetRemoveBitmap")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddRemoveBitmaps(key, additions, deletions)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddRemoveBitmaps,
			key:       key,
			additions: additions,
			deletions: deletions,
		}, false, "ThreadedRoaringSetAddRemoveBitmaps")
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if m.baseline != nil {
		return m.baseline.roaringSetGet(key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetGet,
			key:       key,
		}, true, "ThreadedRoaringSetGet")
		return output.bitmap, output.error
	}
}

func (m *MemtableThreaded) roaringSetAdjustMeta(entriesChanged int) {
	if m.baseline != nil {
		m.baseline.roaringSetAdjustMeta(entriesChanged)
	} else {
		//TODO: implement
	}
}

func (m *MemtableThreaded) roaringSetAddCommitLog(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddCommitLog(key, additions, deletions)
	} else {
		//TODO: implement
		return nil
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

	fmt.Printf("Merged %d nodes into %d nodes\n", totalSize, mergedNodesIndex)
	return flat[:mergedNodesIndex], nil
}
