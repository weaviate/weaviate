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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type MemtableThreaded struct {
	baseline         *Memtable
	wgWorkers        sync.WaitGroup
	path             string
	numWorkers       int
	requestsChannels []chan ThreadedMemtableRequest
	workerAssignment string
	secondaryIndices uint16
	lastWrite        time.Time
	createdAt        time.Time
	metrics          *memtableMetrics
	commitLogger     *commitLogger
}

type ThreadedMemtableFunc func(*Memtable, ThreadedMemtableRequest) ThreadedMemtableResponse

type ThreadedMemtableRequest struct {
	operation   ThreadedMemtableFunc
	key         []byte
	value       uint64
	valueBytes  []byte
	values      []uint64
	valuesValue []value
	bm          *sroar.Bitmap
	additions   *sroar.Bitmap
	deletions   *sroar.Bitmap
	response    chan ThreadedMemtableResponse
	pos         int
	opts        []SecondaryKeyOption
	pair        MapPair
}

type ThreadedMemtableResponse struct {
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
	cursorSet             *CursorSet
	cursorMap             *CursorMap
	cursorReplace         *CursorReplace
	cursorRoaringSet      *roaringset.CombinedCursorLayer
	size                  uint64
	duration              time.Duration
	countStats            *countStats
}

func ThreadedGet(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	v, err := m.get(request.key)
	return ThreadedMemtableResponse{error: err, result: v}
}

func ThreadedGetBySecondary(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	v, err := m.getBySecondary(request.pos, request.key)
	return ThreadedMemtableResponse{error: err, result: v}
}

func ThreadedPut(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	err := m.put(request.key, request.valueBytes, request.opts...)
	return ThreadedMemtableResponse{error: err}
}

func ThreadedSetTombstone(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	err := m.setTombstone(request.key, request.opts...)
	return ThreadedMemtableResponse{error: err}
}

func ThreadedGetCollection(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	v, err := m.getCollection(request.key)
	return ThreadedMemtableResponse{error: err, values: v}
}

func ThreadedGetMap(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	v, err := m.getMap(request.key)
	return ThreadedMemtableResponse{error: err, mapNodes: v}
}

func ThreadedAppend(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	err := m.append(request.key, request.valuesValue)
	return ThreadedMemtableResponse{error: err}
}

func ThreadedAppendMapSorted(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	err := m.appendMapSorted(request.key, request.pair)
	return ThreadedMemtableResponse{error: err}
}

func ThreadedNewCollectionCursor(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{innerCursorCollection: m.newCollectionCursor()}
}

func ThreadedNewMapCursor(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{innerCursorMap: m.newMapCursor()}
}

func ThreadedNewCursor(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{innerCursorReplace: m.newCursor()}
}

func ThreadedNewRoaringSetCursor(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{innerCursorRoaringSet: m.newRoaringSetCursor()}
}

func ThreadedWriteWAL(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.writeWAL()}
}

func ThreadedSize(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{size: m.size}
}

func threadWorker(id int, dirName string, requests <-chan ThreadedMemtableRequest, wg *sync.WaitGroup, strategy string) {
	defer wg.Done()
	// One bucket per worker, initialization is done in the worker thread
	m, err := newMemtable(dirName, strategy, 0, nil)
	if err != nil {
		fmt.Println("Error creating memtable", err)
		return
	}

	for req := range requests {
		// fmt.Println("Worker", id, "received request", req.operationName)
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
	workerAssignment := os.Getenv("MEMTABLE_THREADED_WORKER_ASSIGNMENT")
	if len(workerAssignment) == 0 {
		workerAssignment = "hash"
	}
	enableForSet := map[string]bool{}
	enableFor := os.Getenv("MEMTABLE_THREADED_ENABLE_TYPES")
	if len(workerAssignment) != 0 {
		for _, t := range strings.Split(enableFor, ",") {
			enableForSet[t] = true
		}
	}

	return newMemtableThreadedDebug(path, strategy, secondaryIndices, metrics, workerAssignment, enableForSet)
}

func newMemtableThreadedDebug(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics, workerAssignment string, enableFor map[string]bool,
) (*MemtableThreaded, error) {
	if !enableFor[strategy] {
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
		requestsChannels := make([]chan ThreadedMemtableRequest, numChannels)

		for i := 0; i < numChannels; i++ {
			requestsChannels[i] = make(chan ThreadedMemtableRequest)
		}

		// Start worker goroutines
		wgWorkers.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			dirName := path + fmt.Sprintf("_%d", i)
			go threadWorker(i, dirName, requestsChannels[i%numChannels], &wgWorkers, strategy)
		}

		m := &MemtableThreaded{
			wgWorkers:        wgWorkers,
			path:             path,
			numWorkers:       numWorkers,
			requestsChannels: requestsChannels,
			secondaryIndices: secondaryIndices,
			lastWrite:        time.Now(),
			createdAt:        time.Now(),
			metrics:          newMemtableMetrics(metrics, filepath.Dir(path), strategy),
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
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedGet,
			key:       key,
		}, true, "Get")
		return output.result, output.error
	}
}

func (m *MemtableThreaded) getBySecondary(pos int, key []byte) ([]byte, error) {
	if m.baseline != nil {
		return m.baseline.getBySecondary(pos, key)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedGetBySecondary,
			key:       key,
			pos:       pos,
		}, true, "GetBySecondary")
		return output.result, output.error
	}
}

func (m *MemtableThreaded) put(key, value []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.put(key, value, opts...)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation:  ThreadedPut,
			key:        key,
			valueBytes: value,
			opts:       opts,
		}, false, "Put")
		return output.error
	}
}

func (m *MemtableThreaded) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.setTombstone(key, opts...)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSetTombstone,
			key:       key,
			opts:      opts,
		}, false, "SetTombstone")
		return output.error
	}
}

func (m *MemtableThreaded) getCollection(key []byte) ([]value, error) {
	if m.baseline != nil {
		return m.baseline.getCollection(key)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedGetCollection,
			key:       key,
		}, true, "GetCollection")
		return output.values, output.error
	}
}

func (m *MemtableThreaded) getMap(key []byte) ([]MapPair, error) {
	if m.baseline != nil {
		return m.baseline.getMap(key)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedGetMap,
			key:       key,
		}, true, "GetMap")
		return output.mapNodes, output.error
	}
}

func (m *MemtableThreaded) append(key []byte, values []value) error {
	if m.baseline != nil {
		return m.baseline.append(key, values)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation:   ThreadedAppend,
			key:         key,
			valuesValue: values,
		}, false, "Append")
		return output.error
	}
}

func (m *MemtableThreaded) appendMapSorted(key []byte, pair MapPair) error {
	if m.baseline != nil {
		return m.baseline.appendMapSorted(key, pair)
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedAppendMapSorted,
			key:       key,
			pair:      pair,
		}, false, "AppendMapSorted")
		return output.error
	}
}

func (m *MemtableThreaded) Size() uint64 {
	if m.baseline != nil {
		return m.baseline.Size()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSize,
		}, false, "Size")
		return output.size
	}
}

func (m *MemtableThreaded) ActiveDuration() time.Duration {
	if m.baseline != nil {
		return m.baseline.ActiveDuration()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSize,
		}, false, "ActiveDuration")
		return output.duration
	}
}

func (m *MemtableThreaded) IdleDuration() time.Duration {
	if m.baseline != nil {
		return m.baseline.IdleDuration()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSize,
		}, false, "IdleDuration")
		return output.duration
	}
}

func (m *MemtableThreaded) countStats() *countStats {
	if m.baseline != nil {
		return m.baseline.countStats()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSize,
		}, false, "countStats")
		return output.countStats
	}
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
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedWriteWAL,
		}, false, "WriteWAL")
		return output.error
	}
}

func (m *MemtableThreaded) Commitlog() *commitLogger {
	if m.baseline != nil {
		return m.baseline.Commitlog()
	}
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
	return m.secondaryIndices
}

func (m *MemtableThreaded) SetPath(path string) {
	if m.baseline != nil {
		m.baseline.SetPath(path)
	}
	m.path = path
}
