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
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
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

func ThreadedRoaringSetFlattenInOrder(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{nodes: m.RoaringSet().FlattenInOrder()}
}

func (m *MemtableThreaded) threadedOperation(data ThreadedMemtableRequest, needOutput bool, operationName string) ThreadedMemtableResponse {
	if operationName == "WriteWAL" {
		multiMemtableRequest(m, data, false)
		return ThreadedMemtableResponse{}
	} else if operationName == "Size" {
		responses := multiMemtableRequest(m, data, true)
		var maxSize uint64
		for _, response := range responses {
			if response.size > maxSize {
				maxSize = response.size
			}
		}
		return ThreadedMemtableResponse{size: maxSize}
	} else if operationName == "ActiveDuration" {
		responses := multiMemtableRequest(m, data, true)
		var maxDuration time.Duration
		for _, response := range responses {
			if response.duration > maxDuration {
				maxDuration = response.duration
			}
		}
		return ThreadedMemtableResponse{duration: maxDuration}
	} else if operationName == "IdleDuration" {
		responses := multiMemtableRequest(m, data, true)
		var maxDuration time.Duration
		for _, response := range responses {
			if response.duration > maxDuration {
				maxDuration = response.duration
			}
		}
		return ThreadedMemtableResponse{duration: maxDuration}
	} else if operationName == "countStats" {
		responses := multiMemtableRequest(m, data, true)
		countStats := &countStats{
			upsertKeys:     make([][]byte, 0),
			tombstonedKeys: make([][]byte, 0),
		}
		for _, response := range responses {
			countStats.upsertKeys = append(countStats.upsertKeys, response.countStats.upsertKeys...)
			countStats.tombstonedKeys = append(countStats.tombstonedKeys, response.countStats.tombstonedKeys...)
		}
		return ThreadedMemtableResponse{countStats: countStats}
	} else if operationName == "RoaringSetFlattenInOrder" {
		// send request to all workers
		var nodes [][]*roaringset.BinarySearchNode
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedMemtableResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			nodes = append(nodes, response.nodes)
		}
		merged, err := mergeRoaringSets(nodes)
		return ThreadedMemtableResponse{nodes: merged, error: err}
	} else if operationName == "NewCollectionCursor" {
		// send request to all workers
		var cursors []innerCursorCollection
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedMemtableResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorCollection)
		}
		set := &CursorSet{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorSet: set}
	} else if operationName == "NewRoaringSetCursor" {
		// send request to all workers
		var cursors []roaringset.InnerCursor
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedMemtableResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorRoaringSet)
		}
		set := roaringset.NewCombinedCursorLayer(cursors, false)
		return ThreadedMemtableResponse{cursorRoaringSet: set}
	} else if operationName == "NewMapCursor" {
		// send request to all workers
		var cursors []innerCursorMap
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedMemtableResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorMap)
		}
		mapCursor := &CursorMap{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorMap: mapCursor}
	} else if operationName == "NewCursor" {
		// send request to all workers
		var cursors []innerCursorReplace
		for _, channel := range m.requestsChannels {
			responseChannel := make(chan ThreadedMemtableResponse)
			data.response = responseChannel
			channel <- data
			response := <-responseChannel
			cursors = append(cursors, response.innerCursorReplace)
		}
		cursor := &CursorReplace{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorReplace: cursor}
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
		responseCh := make(chan ThreadedMemtableResponse)
		data.response = responseCh
		m.requestsChannels[workerID] <- data
		return <-responseCh
	} else {
		data.response = nil
		m.requestsChannels[workerID] <- data
		return ThreadedMemtableResponse{}
	}
}

func multiMemtableRequest(m *MemtableThreaded, data ThreadedMemtableRequest, getResponses bool) []ThreadedMemtableResponse {
	var responses []ThreadedMemtableResponse
	if getResponses {
		responses = make([]ThreadedMemtableResponse, m.numWorkers)
	}
	for _, channel := range m.requestsChannels {
		if getResponses {
			data.response = make(chan ThreadedMemtableResponse)
		}
		channel <- data
		if getResponses {
			response := <-data.response
			responses = append(responses, response)
		}
	}
	return responses
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

	fmt.Printf("Merged %d nodes into %d nodes\n", totalSize, mergedNodesIndex)
	return flat[:mergedNodesIndex], nil
}
