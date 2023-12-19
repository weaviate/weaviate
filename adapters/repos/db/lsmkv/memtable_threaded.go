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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

var (
	MEMTABLE_THREADED_SINGLE_CHANNEL = "single-channel"
	MEMTABLE_THREADED_BASELINE       = "baseline"
	MEMTABLE_THREADED_RANDOM         = "random"
	MEMTABLE_THREADED_HASH           = "hash"
)

type MemtableMulti struct {
	wgWorkers        *sync.WaitGroup
	path             string
	numWorkers       int
	strategy         string
	requestsChannels []chan memtableOperationChannel
	workerAssignment string
	secondaryIndices uint16
	lastWrite        time.Time
	createdAt        time.Time
	metrics          *memtableMetrics
}

type ThreadedMemtableResponse struct {
	error                 error
	bitmap                roaringset.BitmapLayer
	nodesRoaring          []*roaringset.BinarySearchNode
	nodesKey              []*binarySearchNode
	nodesMulti            []*binarySearchNodeMulti
	nodesMap              []*binarySearchNodeMap
	result                []byte
	values                []value
	mapNodes              []MapPair
	innerCursorCollection innerCursorCollection
	innerCursorMap        innerCursorMap
	innerCursorReplace    innerCursorReplace
	innerCursorRoaringSet roaringset.InnerCursor
	size                  uint64
	duration              time.Duration
	countStats            *countStats
}

type memtableOperation func(id int, m *MemtableSingle) ThreadedMemtableResponse

type memtableOperationChannel struct {
	callback memtableOperation
	response chan ThreadedMemtableResponse
}

func threadWorker(id int, dirName string, secondaryIndices uint16, metrics *Metrics, requests <-chan memtableOperationChannel, wg *sync.WaitGroup, strategy string) {
	defer wg.Done()
	// One bucket per worker, initialization is done in the worker thread
	m, err := newMemtableSingle(dirName, strategy, secondaryIndices, metrics)
	if err != nil {
		fmt.Println("Error creating memtable", err)
		return
	}

	for req := range requests {
		if req.response != nil {
			req.response <- req.callback(id, m)
		} else {
			req.callback(id, m)
		}
	}
}

func threadHashKey(key []byte, numWorkers int) int {
	// consider using different hash function like Murmur hash or other hash table friendly hash functions
	hasher := fnv.New32a()
	hasher.Write(key)
	return int(hasher.Sum32()) % numWorkers
}

func newMemtable(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics,
) (Memtable, error) {
	workerAssignment := os.Getenv("MEMTABLE_THREADED_WORKER_ASSIGNMENT")
	if len(workerAssignment) == 0 {
		workerAssignment = MEMTABLE_THREADED_HASH
	}
	enableForSet := map[string]bool{}
	// enableForSet := map[string]bool{StrategyMapCollection: true, StrategyRoaringSet: true, StrategyReplace: true, StrategySetCollection: true}

	enableFor := os.Getenv("MEMTABLE_THREADED_ENABLE_TYPES")
	if len(enableFor) != 0 {
		for _, t := range strings.Split(enableFor, ",") {
			enableForSet[t] = true
		}
	}

	numThreadsInt := runtime.NumCPU()
	numThreads := os.Getenv("MEMTABLE_THREADED_THREAD_COUNT")
	if len(numThreads) > 0 {
		numThreadsIntParsed, err := strconv.Atoi(numThreads)
		if err != nil {
			return nil, err
		}
		numThreadsInt = numThreadsIntParsed
	}

	bufferSizeInt := 10
	bufferSize := os.Getenv("MEMTABLE_THREADED_BUFFER_SIZE")
	if len(bufferSize) > 0 {
		bufferSizeIntParsed, err := strconv.Atoi(bufferSize)
		if err != nil {
			return nil, err
		}
		bufferSizeInt = bufferSizeIntParsed
	}

	if enableForSet[strategy] {
		return newMemtableThreaded(path, strategy, secondaryIndices, metrics, workerAssignment, numThreadsInt, bufferSizeInt)
	} else {
		return newMemtableSingle(path, strategy, secondaryIndices, metrics)
	}
}

func newMemtableThreaded(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics, workerAssignment string, numThreads int, bufferSize int,
) (*MemtableMulti, error) {
	var wgWorkers *sync.WaitGroup
	numWorkers := numThreads
	numChannels := numWorkers
	requestsChannels := make([]chan memtableOperationChannel, numChannels)

	for i := 0; i < numChannels; i++ {
		requestsChannels[i] = make(chan memtableOperationChannel, bufferSize)
	}

	// Start worker goroutines
	wgWorkers = &sync.WaitGroup{}
	wgWorkers.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		dirName := path + fmt.Sprintf("_%d", i)
		go threadWorker(i, dirName, secondaryIndices, metrics, requestsChannels[i], wgWorkers, strategy)
	}

	m := &MemtableMulti{
		wgWorkers:        wgWorkers,
		path:             path,
		numWorkers:       numWorkers,
		strategy:         strategy,
		requestsChannels: requestsChannels,
		secondaryIndices: secondaryIndices,
		lastWrite:        time.Now(),
		createdAt:        time.Now(),
		metrics:          newMemtableMetrics(metrics, filepath.Dir(path), strategy),
		workerAssignment: workerAssignment,
	}

	return m, nil
}

func (mm *MemtableMulti) callAllWorkers(operation memtableOperation, needOutput bool) []ThreadedMemtableResponse {
	// bottleneck?
	// operation is pushed to channels sequentially,
	// loop will wait with next elements until channel will be able to accept current one
	var responseChannels []chan ThreadedMemtableResponse
	if needOutput {
		responseChannels = make([]chan ThreadedMemtableResponse, mm.numWorkers)
		for i := range responseChannels {
			responseChannels[i] = make(chan ThreadedMemtableResponse)
		}
		for i, ch := range mm.requestsChannels {
			operationChannel := memtableOperationChannel{
				callback: operation,
				response: responseChannels[i],
			}
			ch <- operationChannel
		}
	} else {
		for _, ch := range mm.requestsChannels {
			operationChannel := memtableOperationChannel{
				callback: operation,
				response: nil,
			}
			ch <- operationChannel
		}
	}

	if needOutput {
		responses := make([]ThreadedMemtableResponse, mm.numWorkers)
		for i, ch := range responseChannels {
			responses[i] = <-ch
		}
		return responses
	}
	return nil
}

func (mm *MemtableMulti) callSingleWorker(operation memtableOperation, key []byte, needOutput bool) ThreadedMemtableResponse {
	// replace with deterministic translation key to id (hash etc)
	workerID := 0
	if mm.workerAssignment == MEMTABLE_THREADED_SINGLE_CHANNEL {
		workerID = 0
	} else if mm.workerAssignment == MEMTABLE_THREADED_RANDOM {
		workerID = rand.Intn(mm.numWorkers)
	} else if mm.workerAssignment == MEMTABLE_THREADED_HASH {
		workerID = threadHashKey(key, mm.numWorkers)
	} else {
		panic("invalid worker assignment")
	}

	if needOutput {
		responseCh := make(chan ThreadedMemtableResponse)
		operationChannel := memtableOperationChannel{
			callback: operation,
			response: responseCh,
		}
		mm.requestsChannels[workerID] <- operationChannel
		return <-responseCh
	} else {
		operationChannel := memtableOperationChannel{
			callback: operation,
			response: nil,
		}
		mm.requestsChannels[workerID] <- operationChannel
		return ThreadedMemtableResponse{}
	}
}

func (m *MemtableMulti) get(key []byte) ([]byte, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		value, err := m.get(key)
		return ThreadedMemtableResponse{result: value, error: err}
	}
	result := m.callSingleWorker(memtableOperation, key, true)
	return result.result, result.error
}

func (m *MemtableMulti) getBySecondary(pos int, key []byte) ([]byte, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		value, err := m.getBySecondary(pos, key)
		return ThreadedMemtableResponse{result: value, error: err}
	}
	results := m.callAllWorkers(memtableOperation, true)
	for _, response := range results {
		if response.result != nil {
			return response.result, response.error
		}
	}
	return nil, results[0].error
}

func (m *MemtableMulti) put(key, value []byte, opts ...SecondaryKeyOption) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.put(key, value, opts...)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.setTombstone(key, opts...)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) getCollection(key []byte) ([]value, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		values, err := m.getCollection(key)
		return ThreadedMemtableResponse{values: values, error: err}
	}
	result := m.callSingleWorker(memtableOperation, key, true)
	return result.values, result.error
}

func (m *MemtableMulti) getMap(key []byte) ([]MapPair, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		values, err := m.getMap(key)
		return ThreadedMemtableResponse{mapNodes: values, error: err}
	}
	result := m.callSingleWorker(memtableOperation, key, true)
	return result.mapNodes, result.error
}

func (m *MemtableMulti) append(key []byte, values []value) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.append(key, values)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

func (m *MemtableMulti) appendMapSorted(key []byte, pair MapPair) error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.appendMapSorted(key, pair)}
	}
	result := m.callSingleWorker(memtableOperation, key, false)
	return result.error
}

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
func (m *MemtableMulti) Size() uint64 {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{size: m.Size()}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var sumSize uint64
	for _, result := range results {
		sumSize += result.size
	}
	return sumSize
}

func (m *MemtableMulti) ActiveDuration() time.Duration {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{duration: m.ActiveDuration()}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var maxDuration time.Duration
	for _, result := range results {
		if result.duration > maxDuration {
			maxDuration = result.duration
		}
	}
	return maxDuration
}

func (m *MemtableMulti) IdleDuration() time.Duration {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{duration: m.IdleDuration()}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var maxDuration time.Duration
	for _, result := range results {
		if result.duration > maxDuration {
			maxDuration = result.duration
		}
	}
	return maxDuration
}

func (m *MemtableMulti) countStats() *countStats {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{countStats: m.countStats()}
	}
	results := m.callAllWorkers(memtableOperation, true)
	countStats := &countStats{
		upsertKeys:     make([][]byte, 0),
		tombstonedKeys: make([][]byte, 0),
	}
	for _, result := range results {
		countStats.upsertKeys = append(countStats.upsertKeys, result.countStats.upsertKeys...)
		countStats.tombstonedKeys = append(countStats.tombstonedKeys, result.countStats.tombstonedKeys...)
	}
	return countStats
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to safge unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (m *MemtableMulti) writeWAL() error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{error: m.writeWAL()}
	}
	results := m.callAllWorkers(memtableOperation, true)
	for _, result := range results {
		if result.error != nil {
			return result.error
		}
	}
	return nil
}

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
func (m *MemtableMulti) CommitlogSize() int64 {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{size: uint64(m.CommitlogSize())}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var sumSize uint64
	for _, result := range results {
		sumSize += result.size
	}
	return int64(sumSize)
}

func (m *MemtableMulti) CommitlogPath() string {
	// this one should always exists
	return m.path + fmt.Sprintf("_%d", 0)
}

func (m *MemtableMulti) Path() string {
	return m.path
}

func (m *MemtableMulti) SecondaryIndices() uint16 {
	return m.secondaryIndices
}

func (m *MemtableMulti) UpdatePath(bucketDir, newBucketDir string) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		m.UpdatePath(bucketDir, newBucketDir)
		return ThreadedMemtableResponse{}
	}
	m.callAllWorkers(memtableOperation, true)
}

func (m *MemtableMulti) CommitlogPause() {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		m.CommitlogPause()
		return ThreadedMemtableResponse{}
	}
	m.callAllWorkers(memtableOperation, true)
}

func (m *MemtableMulti) CommitlogUnpause() {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		m.CommitlogUnpause()
		return ThreadedMemtableResponse{}
	}
	m.callAllWorkers(memtableOperation, true)
}

func (m *MemtableMulti) CommitlogClose() error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		err := m.CommitlogClose()
		return ThreadedMemtableResponse{error: err}
	}
	results := m.callAllWorkers(memtableOperation, true)
	for _, result := range results {
		if result.error != nil {
			return result.error
		}
	}
	return nil
}

func (m *MemtableMulti) CommitlogDelete() error {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		err := m.CommitlogDelete()
		return ThreadedMemtableResponse{error: err}
	}
	results := m.callAllWorkers(memtableOperation, true)
	for _, result := range results {
		if result.error != nil {
			return result.error
		}
	}
	return nil
}

// TODO: the MemtableThreaded is reporting the max size of the individual Memtables for flushing, not the sum. Not sure what is the best way to handle this, as sometimes we may want the max and other times we may want the sum.
func (m *MemtableMulti) CommitlogFileSize() (int64, error) {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		size, err := m.CommitlogFileSize()
		return ThreadedMemtableResponse{size: uint64(size), error: err}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var sumSize uint64
	for _, result := range results {
		sumSize += result.size
	}
	return int64(sumSize), nil
}

func (m *MemtableMulti) Strategy() string {
	return m.strategy
}
