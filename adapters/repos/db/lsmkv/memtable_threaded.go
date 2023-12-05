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
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type DummyCommitlog struct{}

func (*DummyCommitlog) close() error {
	return nil
}

type MemtableThreaded struct {
	baseline         *Memtable
	wgWorkers        *sync.WaitGroup
	path             string
	numWorkers       int
	strategy         string
	requestsChannels []chan ThreadedMemtableRequest
	workerAssignment string
	secondaryIndices uint16
	lastWrite        time.Time
	createdAt        time.Time
	metrics          *memtableMetrics
	commitlog        *DummyCommitlog
}

type ThreadedMemtableFunc func(*Memtable, ThreadedMemtableRequest) ThreadedMemtableResponse

type ThreadedMemtableRequest struct {
	operation    ThreadedMemtableFunc
	key          []byte
	value        uint64
	valueBytes   []byte
	values       []uint64
	valuesValue  []value
	bm           *sroar.Bitmap
	additions    *sroar.Bitmap
	deletions    *sroar.Bitmap
	response     chan ThreadedMemtableResponse
	pos          int
	opts         []SecondaryKeyOption
	pair         MapPair
	bucketDir    string
	newBucketDir string
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

func ThreadedActiveDuration(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{duration: m.ActiveDuration()}
}

func ThreadedIdleDuration(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{duration: m.IdleDuration()}
}

func ThreadeCountStats(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{countStats: m.countStats()}
}

func ThreadedCommitlogSize(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{size: uint64(m.Commitlog().Size())}
}

func ThreadedCommitlogUpdatePath(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	updatePath := func(src string) string {
		return strings.Replace(src, request.bucketDir, request.newBucketDir, 1)
	}
	m.SetPath(updatePath(m.Path()))
	m.Commitlog().path = updatePath(m.Commitlog().path)
	return ThreadedMemtableResponse{}
}

func ThreadedCommitlogDelete(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	return ThreadedMemtableResponse{error: m.Commitlog().delete()}
}

func ThreadedFlush(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	err := m.flush()
	return ThreadedMemtableResponse{error: err}
}

func ThreadedCommitlogPause(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	m.Commitlog().pause()
	return ThreadedMemtableResponse{}
}

func ThreadedCommitlogUnpause(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	m.Commitlog().unpause()
	return ThreadedMemtableResponse{}
}

func ThreadedCommitlogFileSize(m *Memtable, request ThreadedMemtableRequest) ThreadedMemtableResponse {
	stat, err := m.Commitlog().file.Stat()
	if err != nil {
		return ThreadedMemtableResponse{error: err}
	}
	return ThreadedMemtableResponse{size: uint64(stat.Size())}
}

func threadWorker(id int, dirName string, secondaryIndices uint16, metrics *Metrics, requests <-chan ThreadedMemtableRequest, wg *sync.WaitGroup, strategy string) {
	defer wg.Done()
	// One bucket per worker, initialization is done in the worker thread
	m, err := newMemtable(dirName, strategy, secondaryIndices, metrics)
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
	// enableForSet := map[string]bool{StrategyMapCollection: true, StrategyRoaringSet: true, StrategyReplace: true}

	enableFor := os.Getenv("MEMTABLE_THREADED_ENABLE_TYPES")
	if len(enableFor) != 0 {
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

		var wgWorkers *sync.WaitGroup
		numWorkers := runtime.NumCPU()
		numChannels := numWorkers
		requestsChannels := make([]chan ThreadedMemtableRequest, numChannels)

		for i := 0; i < numChannels; i++ {
			requestsChannels[i] = make(chan ThreadedMemtableRequest)
		}

		// Start worker goroutines
		wgWorkers = &sync.WaitGroup{}
		wgWorkers.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			dirName := path + fmt.Sprintf("_%d", i)
			go threadWorker(i, dirName, secondaryIndices, metrics, requestsChannels[i%numChannels], wgWorkers, strategy)
		}

		m := &MemtableThreaded{
			wgWorkers:        wgWorkers,
			path:             path,
			numWorkers:       numWorkers,
			strategy:         strategy,
			requestsChannels: requestsChannels,
			secondaryIndices: secondaryIndices,
			lastWrite:        time.Now(),
			createdAt:        time.Now(),
			metrics:          newMemtableMetrics(metrics, filepath.Dir(path), strategy),
			baseline:         nil,
			workerAssignment: workerAssignment,
		}

		return m, nil

	}
}

func (m *MemtableThreaded) threadedOperation(data ThreadedMemtableRequest, needOutput bool, operationName string) ThreadedMemtableResponse {
	if operationName == "WriteWAL" || operationName == "Flush" || operationName == "CommitlogUpdatePath" || operationName == "CommitlogDelete" || operationName == "CommitlogPause" || operationName == "CommitlogUnpause" {
		multiMemtableRequest(m, data, true)
		return ThreadedMemtableResponse{}
	} else if operationName == "Size" || operationName == "CommitlogFileSize" {
		responses := multiMemtableRequest(m, data, true)
		var maxSize uint64
		for _, response := range responses {
			if response.size > maxSize {
				maxSize = response.size
			}
		}
		return ThreadedMemtableResponse{size: maxSize}
	} else if operationName == "IdleDuration" || operationName == "ActiveDuration" {
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
		responses := multiMemtableRequest(m, data, true)
		var nodes [][]*roaringset.BinarySearchNode
		for _, response := range responses {
			nodes = append(nodes, response.nodes)
		}
		merged, err := mergeRoaringSets(nodes)
		return ThreadedMemtableResponse{nodes: merged, error: err}
	} else if operationName == "NewCollectionCursor" {
		responses := multiMemtableRequest(m, data, true)
		var cursors []innerCursorCollection
		for _, response := range responses {
			cursors = append(cursors, response.innerCursorCollection)
		}
		set := &CursorSet{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorSet: set}
	} else if operationName == "NewRoaringSetCursor" {
		responses := multiMemtableRequest(m, data, true)
		var cursors []roaringset.InnerCursor
		for _, response := range responses {
			cursors = append(cursors, response.innerCursorRoaringSet)
		}
		set := roaringset.NewCombinedCursorLayer(cursors, false)
		return ThreadedMemtableResponse{cursorRoaringSet: set}
	} else if operationName == "NewMapCursor" {
		responses := multiMemtableRequest(m, data, true)
		var cursors []innerCursorMap
		for _, response := range responses {
			cursors = append(cursors, response.innerCursorMap)
		}
		mapCursor := &CursorMap{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorMap: mapCursor}
	} else if operationName == "NewCursor" {
		responses := multiMemtableRequest(m, data, true)
		var cursors []innerCursorReplace
		for _, response := range responses {
			cursors = append(cursors, response.innerCursorReplace)
		}
		cursor := &CursorReplace{
			unlock: func() {
			},
			innerCursors: cursors,
		}
		return ThreadedMemtableResponse{cursorReplace: cursor}
	} else if operationName == "GetBySecondary" {
		// The worker that has the data for the secondary key is one that was the primary key, not the one in the secondary hash key space
		// Makes Test_MemtableSecondaryKeyBug work
		responses := multiMemtableRequest(m, data, true)
		for _, response := range responses {
			if response.result != nil {
				return ThreadedMemtableResponse{result: response.result, error: response.error}
			}
		}
		return ThreadedMemtableResponse{error: responses[0].error}
	}
	return singleMemtableRequest(data, m, needOutput)
}

func singleMemtableRequest(data ThreadedMemtableRequest, m *MemtableThreaded, needOutput bool) ThreadedMemtableResponse {
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
	var responseChanels []chan ThreadedMemtableResponse
	if getResponses {
		responses = make([]ThreadedMemtableResponse, m.numWorkers)
		responseChanels = make([]chan ThreadedMemtableResponse, m.numWorkers)
		for i := 0; i < m.numWorkers; i++ {
			responseChanels[i] = make(chan ThreadedMemtableResponse)
		}
	}
	for i, channel := range m.requestsChannels {
		if getResponses {
			data.response = responseChanels[i]
		}
		channel <- data
	}
	if getResponses {
		for i, responseCh := range responseChanels {
			responses[i] = <-responseCh
		}
	}
	return responses
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

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
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
			operation: ThreadedActiveDuration,
		}, false, "ActiveDuration")
		return output.duration
	}
}

func (m *MemtableThreaded) IdleDuration() time.Duration {
	if m.baseline != nil {
		return m.baseline.IdleDuration()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedIdleDuration,
		}, false, "IdleDuration")
		return output.duration
	}
}

func (m *MemtableThreaded) countStats() *countStats {
	if m.baseline != nil {
		return m.baseline.countStats()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadeCountStats,
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

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
func (m *MemtableThreaded) CommitlogSize() int64 {
	if m.baseline != nil {
		return m.baseline.Commitlog().Size()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedSize,
		}, false, "CommitlogSize")
		return int64(output.size)
	}
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

func (m *MemtableThreaded) UpdatePath(bucketDir, newBucketDir string) {
	updatePath := func(src string) string {
		return strings.Replace(src, bucketDir, newBucketDir, 1)
	}
	if m.baseline != nil {
		m.baseline.SetPath(updatePath(m.baseline.Path()))
		m.baseline.Commitlog().path = updatePath(m.baseline.Commitlog().path)
	} else {
		m.threadedOperation(ThreadedMemtableRequest{
			operation:    ThreadedCommitlogUpdatePath,
			bucketDir:    bucketDir,
			newBucketDir: newBucketDir,
		}, false, "UpdatePath")
	}
}

func (m *MemtableThreaded) CommitlogPause() {
	if m.baseline != nil {
		m.baseline.Commitlog().pause()
	} else {
		m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedCommitlogPause,
		}, false, "CommitlogPause")
	}
}

func (m *MemtableThreaded) CommitlogUnpause() {
	if m.baseline != nil {
		m.baseline.Commitlog().unpause()
	} else {
		m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedCommitlogUnpause,
		}, false, "CommitlogUnpause")
	}
}

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
func (m *MemtableThreaded) CommitlogFileSize() (int64, error) {
	if m.baseline != nil {
		stat, err := m.baseline.Commitlog().file.Stat()
		if err != nil {
			return 0, err
		}
		return stat.Size(), nil
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedCommitlogFileSize,
		}, false, "CommitlogFileSize,")
		return int64(output.size), output.error
	}
}
