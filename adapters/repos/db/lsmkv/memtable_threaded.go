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
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type MemtableThreaded struct {
	baseline         *Memtable
	wgWorkers        sync.WaitGroup
	path             string
	numClients       int
	numWorkers       int
	requestsChannels []chan ThreadedBitmapRequest
	workerAssignment string
	commitLogger     *commitLogger
}

type ThreadedMemtableFunc func(*Memtable, ThreadedBitmapRequest) ThreadedBitmapResponse

type ThreadedBitmapRequest struct {
	operation ThreadedMemtableFunc
	//operationName  string
	key         []byte
	value       uint64
	valueBytes  []byte
	values      []uint64
	valuesValue []value
	bm          *sroar.Bitmap
	additions   *sroar.Bitmap
	deletions   *sroar.Bitmap
	response    chan ThreadedBitmapResponse
	pos         int
	opts        []SecondaryKeyOption
	pair        MapPair
}

type ThreadedBitmapResponse struct {
	error                 error
	bitmap                roaringset.BitmapLayer
	nodes                 []*roaringset.BinarySearchNode
	result                []byte
	values                []value
	mapNodes              []MapPair
	innerCursorCollection innerCursorCollection
	innerCursorMap        innerCursorMap
	innerCursorReplace    innerCursorReplace
	innerCursorRoaringSet roaringset.InnerCursor
}

func ThreadedGet(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	v, err := m.get(request.key)
	return ThreadedBitmapResponse{error: err, result: v}
}

func ThreadedGetBySecondary(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	v, err := m.getBySecondary(request.pos, request.key)
	return ThreadedBitmapResponse{error: err, result: v}
}

func ThreadedPut(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	err := m.put(request.key, request.valueBytes, request.opts...)
	return ThreadedBitmapResponse{error: err}
}

func ThreadedSetTombstone(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	err := m.setTombstone(request.key, request.opts...)
	return ThreadedBitmapResponse{error: err}
}

func ThreadedGetCollection(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	v, err := m.getCollection(request.key)
	return ThreadedBitmapResponse{error: err, values: v}
}

func ThreadedGetMap(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	v, err := m.getMap(request.key)
	return ThreadedBitmapResponse{error: err, mapNodes: v}
}

func ThreadedAppend(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	err := m.append(request.key, request.valuesValue)
	return ThreadedBitmapResponse{error: err}
}

func ThreadedAppendMapSorted(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	err := m.appendMapSorted(request.key, request.pair)
	return ThreadedBitmapResponse{error: err}
}

func ThreadedNewCollectionCursor(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{innerCursorCollection: m.newCollectionCursor()}
}

func ThreadedNewMapCursor(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{innerCursorMap: m.newMapCursor()}
}

func ThreadedNewCursor(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{innerCursorReplace: m.newCursor()}
}

func ThreadedNewRoaringSetCursor(m *Memtable, request ThreadedBitmapRequest) ThreadedBitmapResponse {
	return ThreadedBitmapResponse{innerCursorRoaringSet: m.newRoaringSetCursor()}
}

func threadWorker(id int, dirName string, requests <-chan ThreadedBitmapRequest, wg *sync.WaitGroup, strategy string) {
	defer wg.Done()
	// One bucket per worker, initialization is done in the worker thread
	m, err := newMemtable(dirName, strategy, 0, nil)
	if err != nil {
		fmt.Println("Error creating memtable", err)
		return
	}

	for req := range requests {
		//fmt.Println("Worker", id, "received request", req.operationName)
		if req.response != nil {
			req.response <- req.operation(m, req)
		} else {
			req.operation(m, req)
		}
	}

	fmt.Println("Worker", id, "size:")

}

func threadHashKey(key []byte, numWorkers int) int {
	// consider using different hash function like Murmur hash or other hash table friendly hash functions
	hasher := fnv.New32a()
	hasher.Write(key)
	return int(hasher.Sum32()) % numWorkers
}

func newMemtableThreaded(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics,
) (*MemtableThreaded, error) {
	workerAssignment := os.Getenv("MEMTABLE_WORKER_ASSIGNMENT")
	if len(workerAssignment) == 0 {
		workerAssignment = "hash"
	}

	return newMemtableThreadedDebug(path, strategy, secondaryIndices, metrics, workerAssignment)
}

func newMemtableThreadedDebug(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics, workerAssignment string,
) (*MemtableThreaded, error) {
	if workerAssignment == "baseline" { //|| strategy != StrategyRoaringSet {
		m_alt, err := newMemtable(path, strategy, secondaryIndices, metrics)

		if err != nil {
			return nil, err
		}

		m := &MemtableThreaded{
			baseline: m_alt,
		}
		return m, nil
	} else {

		cl, err := newCommitLogger(path)

		if err != nil {
			return nil, err
		}

		var wgWorkers sync.WaitGroup
		numWorkers := runtime.NumCPU()
		numChannels := numWorkers
		requestsChannels := make([]chan ThreadedBitmapRequest, numChannels)

		for i := 0; i < numChannels; i++ {
			requestsChannels[i] = make(chan ThreadedBitmapRequest)
		}

		// Start worker goroutines
		wgWorkers.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			dirName := path + fmt.Sprintf("_%d", i)
			go threadWorker(i, dirName, requestsChannels[i%numChannels], &wgWorkers, strategy)
		}

		m := &MemtableThreaded{
			wgWorkers:        wgWorkers,
			numClients:       0,
			numWorkers:       numWorkers,
			requestsChannels: requestsChannels,
			baseline:         nil,
			workerAssignment: workerAssignment,
			commitLogger:     cl,
		}

		return m, nil

	}

}

func (m *MemtableThreaded) get(key []byte) ([]byte, error) {
	if m.baseline != nil {
		return m.baseline.get(key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedGet,
			key:       key,
		}, true, "ThreadedGet")
		return output.result, output.error
	}
}

func (m *MemtableThreaded) getBySecondary(pos int, key []byte) ([]byte, error) {
	if m.baseline != nil {
		return m.baseline.getBySecondary(pos, key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedGetBySecondary,
			key:       key,
			pos:       pos,
		}, true, "ThreadedGetBySecondary")
		return output.result, output.error
	}
}

func (m *MemtableThreaded) put(key, value []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.put(key, value, opts...)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:  ThreadedPut,
			key:        key,
			valueBytes: value,
			opts:       opts,
		}, false, "ThreadedPut")
		return output.error
	}
}

func (m *MemtableThreaded) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.setTombstone(key, opts...)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedSetTombstone,
			key:       key,
			opts:      opts,
		}, false, "ThreadedSetTombstone")
		return output.error
	}
}

func (m *MemtableThreaded) getCollection(key []byte) ([]value, error) {
	if m.baseline != nil {
		return m.baseline.getCollection(key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedGetCollection,
			key:       key,
		}, true, "ThreadedGetCollection")
		return output.values, output.error
	}
}

func (m *MemtableThreaded) getMap(key []byte) ([]MapPair, error) {
	if m.baseline != nil {
		return m.baseline.getMap(key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedGetMap,
			key:       key,
		}, true, "ThreadedGetMap")
		return output.mapNodes, output.error
	}
}

func (m *MemtableThreaded) append(key []byte, values []value) error {
	if m.baseline != nil {
		return m.baseline.append(key, values)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation:   ThreadedAppend,
			key:         key,
			valuesValue: values,
		}, false, "ThreadedAppend")
		return output.error
	}
}

func (m *MemtableThreaded) appendMapSorted(key []byte, pair MapPair) error {
	if m.baseline != nil {
		return m.baseline.appendMapSorted(key, pair)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedAppendMapSorted,
			key:       key,
			pair:      pair,
		}, false, "ThreadedAppendMapSorted")
		return output.error
	}
}

func (m *MemtableThreaded) Size() uint64 {
	if m.baseline != nil {
		return m.baseline.Size()
	}
	return 0
}

func (m *MemtableThreaded) ActiveDuration() time.Duration {
	if m.baseline != nil {
		return m.baseline.ActiveDuration()
	}
	return 0
}

func (m *MemtableThreaded) IdleDuration() time.Duration {
	if m.baseline != nil {
		return m.baseline.IdleDuration()
	}
	return 0
}

func (m *MemtableThreaded) countStats() *countStats {
	if m.baseline != nil {
		return m.baseline.countStats()
	}
	return nil
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to safge unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (m *MemtableThreaded) writeWAL() error {
	if m.baseline != nil {
		return m.baseline.writeWAL()
	}
	//TODO: implement
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) Commitlog() *commitLogger {
	if m.baseline != nil {
		return m.baseline.Commitlog()
	}
	//TODO: implement
	return m.commitLogger
}

func (m *MemtableThreaded) Path() string {
	if m.baseline != nil {
		return m.baseline.Path()
	}
	return m.path
}

func (m *MemtableThreaded) SecondaryIndices() uint16 {
	if m.baseline != nil {
		return m.baseline.SecondaryIndices()
	}
	//TODO: implement
	return 0
}

func (m *MemtableThreaded) SetPath(path string) {
	if m.baseline != nil {
		m.baseline.SetPath(path)
	}
	m.path = path
}
