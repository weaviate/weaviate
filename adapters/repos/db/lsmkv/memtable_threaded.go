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
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type ThreadedMemtableFunc func(*Memtable, ThreadedBitmapRequest) ThreadedBitmapResponse

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

type ThreadedBitmapRequest struct {
	operation      ThreadedMemtableFunc
	operationName  string
	key            []byte
	value          uint64
	values         []uint64
	bm             *sroar.Bitmap
	additions      *sroar.Bitmap
	deletions      *sroar.Bitmap
	entriesChanged int
	response       chan ThreadedBitmapResponse
}

type ThreadedBitmapResponse struct {
	error  error
	bitmap roaringset.BitmapLayer
	nodes  []*roaringset.BinarySearchNode
}

func threadWorker(id int, dirName string, requests <-chan ThreadedBitmapRequest, wg *sync.WaitGroup) {
	defer wg.Done()
	// One bucket per worker, initialization is done in the worker thread
	m, err := newMemtable(dirName, StrategyRoaringSet, 0, nil)
	if err != nil {
		fmt.Println("Error creating memtable", err)
		return
	}

	for req := range requests {
		//fmt.Println("Worker", id, "received request", req.operationName)
		if req.operationName == "Flush" {
			break
		}
		req.response <- req.operation(m, req)
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
	return newMemtableThreadedDebug(path, strategy, secondaryIndices, metrics, "hash")
}

func newMemtableThreadedDebug(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics, workerAssignment string,
) (*MemtableThreaded, error) {
	if strategy != StrategyRoaringSet || workerAssignment == "baseline" {
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
			dirName := path
			go threadWorker(i, dirName, requestsChannels[i%numChannels], &wgWorkers)
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
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) getBySecondary(pos int, key []byte) ([]byte, error) {
	if m.baseline != nil {
		return m.baseline.getBySecondary(pos, key)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) put(key, value []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.put(key, value, opts...)
	}
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	if m.baseline != nil {
		return m.baseline.setTombstone(key, opts...)
	}
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) getCollection(key []byte) ([]value, error) {
	if m.baseline != nil {
		return m.baseline.getCollection(key)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) getMap(key []byte) ([]MapPair, error) {
	if m.baseline != nil {
		return m.baseline.getMap(key)
	}
	return nil, errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) append(key []byte, values []value) error {
	if m.baseline != nil {
		return m.baseline.append(key, values)
	}
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) appendMapSorted(key []byte, pair MapPair) error {
	if m.baseline != nil {
		return m.baseline.appendMapSorted(key, pair)
	}
	return errors.Errorf("baseline is nil")
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
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) Commitlog() *commitLogger {
	if m.baseline != nil {
		return m.baseline.Commitlog()
	}
	return m.commitLogger
}

func (m *MemtableThreaded) RoaringSet() *roaringset.BinarySearchTree {
	if m.baseline != nil {
		return m.baseline.RoaringSet()
	}
	return nil
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
	return 0
}

func (m *MemtableThreaded) SetPath(path string) {
	if m.baseline != nil {
		m.baseline.SetPath(path)
	}
	m.path = path
}
