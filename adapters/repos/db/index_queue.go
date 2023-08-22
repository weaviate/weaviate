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

package db

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

// IndexQueue is a persistent queue of vectors to index.
// It batches vectors together before sending them to the indexing worker.
// It persists the vectors on disk to ensure they are not lost in case of a crash.
// It is safe to use concurrently.
type IndexQueue struct {
	Index BatchIndexer

	logger logrus.FieldLogger

	maxQueueSize  int
	maxStaleTime  time.Duration
	retryInterval time.Duration

	// processCh is the channel used to send vectors to the indexing worker.
	processCh chan indexQueueJob

	// indexCh is the channel used to send vectors to the indexing worker.
	indexCh chan batch

	// if closed, prevents new vectors from being added to the queue.
	closed chan struct{}

	// tracks the number of in-flight pushes
	wg sync.WaitGroup

	// tracks the background workers
	workerWg sync.WaitGroup

	// walFile is the append-only file used to persist the pending vectors.
	walFile   *os.File
	walBuffer *bufio.Writer

	// queue of not-yet-indexed vectors
	queuedVectors struct {
		sync.RWMutex

		toIndex []indexQueueJob
	}

	// keeps track of the last time the queue was indexed
	staleTm *time.Ticker

	pqMaxPool *pqMaxPool
}

type IndexQueueOptions struct {
	// MaxQueueSize is the maximum number of vectors to queue
	// before sending them to the indexing worker.
	MaxQueueSize int

	// MaxStaleTime is the maximum time to wait before sending
	// the pending vectors to the indexing worker, regardless
	// of the queue size.
	MaxStaleTime time.Duration

	// Logger is the logger used by the queue.
	Logger logrus.FieldLogger

	// RetryInterval is the interval between retries when
	// indexing fails.
	RetryInterval time.Duration

	// IndexWorkerCount is the number of workers used to index
	// the vectors.
	IndexWorkerCount int
}

type BatchIndexer interface {
	AddBatch(id []uint64, vector [][]float32) error
	SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
}

type VectorLoader interface {
	vectorByIndexID(ctx context.Context, indexID uint64) ([]float32, error)
}

func NewIndexQueue(
	walPath string,
	index BatchIndexer,
	vectorLoader VectorLoader,
	opts IndexQueueOptions,
) (*IndexQueue, error) {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}

	if opts.MaxQueueSize == 0 {
		opts.MaxQueueSize = 10_000
	}

	if opts.MaxStaleTime == 0 {
		opts.MaxStaleTime = 10 * time.Second
	}

	if opts.RetryInterval == 0 {
		opts.RetryInterval = 1 * time.Second
	}

	if opts.IndexWorkerCount == 0 {
		// use the number of CPUs
		opts.IndexWorkerCount = runtime.GOMAXPROCS(0)
	}

	q := IndexQueue{
		Index:         index,
		maxQueueSize:  opts.MaxQueueSize,
		maxStaleTime:  opts.MaxStaleTime,
		retryInterval: opts.RetryInterval,
		processCh:     make(chan indexQueueJob),
		indexCh:       make(chan batch),
		closed:        make(chan struct{}),
		logger:        opts.Logger.WithField("component", "index_queue"),
		pqMaxPool:     newPqMaxPool(0),
	}

	q.queuedVectors.toIndex = make([]indexQueueJob, 0, q.maxQueueSize)

	var err error

	// open append-only file
	q.walFile, err = os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open wal file")
	}

	q.walBuffer = bufio.NewWriter(q.walFile)

	q.workerWg.Add(1)
	go func() {
		defer q.workerWg.Done()
		defer close(q.indexCh)

		q.processor()
	}()

	for i := 0; i < opts.IndexWorkerCount; i++ {
		q.workerWg.Add(1)
		go func() {
			defer q.workerWg.Done()

			q.indexer()
		}()
	}

	return &q, nil
}

// Close waits till the queue has ingested and persisted all pending vectors.
func (q *IndexQueue) Close() error {
	// check if the queue is closed
	select {
	case <-q.closed:
		return errors.New("index queue closed")
	default:
	}

	// prevent new jobs from being added
	close(q.closed)

	// wait for in-flight pushes to finish
	q.wg.Wait()

	// close the workers in cascade
	close(q.processCh)

	q.workerWg.Wait()

	err := q.walBuffer.Flush()
	if err != nil {
		_ = q.walFile.Close()
		return errors.Wrap(err, "failed to flush wal buffer")
	}

	// close the wal file
	return q.walFile.Close()
}

func (q *IndexQueue) loadWalFile(vectorLoader VectorLoader, walPath string) error {
	_, err := os.Stat(walPath)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to stat wal file")
	}

	if os.IsNotExist(err) {
		q.walFile, err = os.Create(walPath)
		if err != nil {
			return errors.Wrap(err, "failed to open wal file")
		}

		q.walBuffer = bufio.NewWriter(q.walFile)
		return nil
	}

	q.walFile, err = os.Open(walPath)
	if err != nil {
		return errors.Wrap(err, "failed to open wal file")
	}

	_, err = q.walFile.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek wal file")
	}

	rd := bufio.NewReader(q.walFile)

	// read the file by chunks
	buf := make([]byte, 8*1024)
	for {
		n, err := rd.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to read wal file")
		}

		// read the ids
		for i := 0; i < n; i += 8 {
			id := binary.BigEndian.Uint64(buf[i : i+8])

			vector, err := vectorLoader.vectorByIndexID(context.Background(), id)
			if err != nil {
				return errors.Wrap(err, "failed to load vector")
			}

			q.queuedVectors.toIndex = append(q.queuedVectors.toIndex, indexQueueJob{
				id:     id,
				vector: vector,
			})
		}
	}

	q.walBuffer = bufio.NewWriter(q.walFile)

	return nil
}

type indexQueueJob struct {
	id     uint64
	vector []float32
	done   chan error
}

// Push adds a vector to the persistent indexing queue.
// It waits until the vector is successfully persisted to the
// on-disk queue or sent to the indexing worker.
func (q *IndexQueue) Push(ctx context.Context, id uint64, vector []float32) error {
	// check if the queue is closed
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closed:
		return errors.New("index queue closed")
	default:
	}

	// count the number of in-flight pushes
	q.wg.Add(1)
	defer q.wg.Done()

	done := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.processCh <- indexQueueJob{
		id:     id,
		vector: vector,
		done:   done,
	}:
		return <-done
	}
}

// Search defer to the index and brute force the unindexed data
func (q *IndexQueue) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	ids, vectors := q.getQueuedVectors()

	indexedResults, distances, err := q.Index.SearchByVector(vector, k, allowList)
	if err != nil {
		return nil, nil, err
	}
	results := q.pqMaxPool.GetMax(k)
	defer q.pqMaxPool.Put(results)
	for i := range indexedResults {
		results.Insert(indexedResults[i], distances[i])
	}

	err = q.bruteForce(vector, k, ids, vectors, results, allowList)
	if err != nil {
		return nil, nil, err
	}

	if cap(ids) >= k {
		ids = ids[:k]
		distances = distances[:k]
	} else {
		ids = make([]uint64, k)
		distances = make([]float32, k)
	}

	for i := k - 1; i >= 0; i-- {
		element := results.Pop()
		ids[i] = element.ID
		distances[i] = element.Dist
	}
	return ids, distances, nil
}

func (q *IndexQueue) bruteForce(vector []float32, k int, ids []uint64, vectors [][]float32, results *priorityqueue.Queue, allowList helpers.AllowList) error {
	// actual brute force. Consider moving to a separate
	for i := range vectors {
		// skip filtered data
		if allowList != nil && allowList.Contains(ids[i]) {
			continue
		}

		dist, _, err := q.Index.DistanceBetweenVectors(vector, vectors[i])
		if err != nil {
			return err
		}

		if results.Len() < k || dist < results.Top().Dist {
			results.Insert(ids[i], dist)
			for results.Len() > k {
				results.Pop()
			}
		}
	}
	return nil
}

// This is the processor worker. Its job is to batch jobs together before
// sending them to the index.
// While the queue is not full or stale, it persists the jobs on disk.
// Once the queue is full or stale, it sends the jobs to the indexing worker.
// It batches concurrent jobs to reduce the number of disk writes.
func (q *IndexQueue) processor() {
	received := make([]indexQueueJob, 0, q.maxQueueSize)

	q.staleTm = time.NewTicker(q.maxStaleTime)

	for {
		received = received[:0]

		select {
		case j, ok := <-q.processCh:
			if !ok {
				// the channel is closed, abort.
				return
			}
			received = append(received, j)

			// loop over the channel to dequeue it until it's empty
			// or we have reached one of our thresholds
		LOOP:
			for {
				select {
				case jj := <-q.processCh:
					received = append(received, jj)
					if q.shouldIndex(received) {
						break LOOP
					}
				default:
					// the channel is empty, break out of the loop
					break LOOP
				}
			}

			// persist the new jobs on disk
			err := q.persist(received)
			if err != nil {
				q.notifyError(received, err)
				continue
			}

			// add the new jobs to the queue
			q.addToQueue(received)

			// notify the jobs that they have been processed
			// to avoid blocking the clients
			q.notifySuccess(received)

			// if the queue is not stale and not full
			// continue processing
			if !q.shouldIndex(nil) {
				continue
			}

			// the queue is full or stale, we need to index the vectors
			var b batch
			b.ids, b.vectors = q.getQueuedVectors()

			// send the batch to the indexing worker
			q.indexCh <- b

			// reset the on-disk queue
			err = q.reset()
			if err != nil {
				// TODO: if we can't reset the file
				// should we recreate it?
				q.logger.WithError(err).Error("failed to reset wal file")
				continue
			}
		case <-q.staleTm.C:
			// if the queue is stale, send the jobs to the indexing worker.
			if q.getQueueLen() == 0 {
				q.staleTm.Reset(q.maxStaleTime)
				continue
			}

			var b batch
			b.ids, b.vectors = q.getQueuedVectors()
			q.indexCh <- b

			// reset the queue
			err := q.reset()
			if err != nil {
				q.logger.WithError(err).Error("failed to reset wal file")
				continue
			}
		case <-q.closed:
			// if the queue is closed, do nothing.
			return
		}
	}
}

func (q *IndexQueue) indexer() {
	for b := range q.indexCh {
		q.indexVectors(&b)
	}
}

func (q *IndexQueue) shouldIndex(received []indexQueueJob) bool {
	// if the queue is not full, continue
	if len(received)+q.getQueueLen() >= q.maxQueueSize {
		return true
	}

	// if the queue is not stale, continue
	select {
	case <-q.staleTm.C:
		return true
	default:
		return false
	}
}

func (q *IndexQueue) addToQueue(jobs []indexQueueJob) {
	q.queuedVectors.Lock()
	q.queuedVectors.toIndex = append(q.queuedVectors.toIndex, jobs...)
	q.queuedVectors.Unlock()
}

func (q *IndexQueue) getQueuedVectors() ([]uint64, [][]float32) {
	q.queuedVectors.RLock()

	ids := make([]uint64, 0, len(q.queuedVectors.toIndex))
	vectors := make([][]float32, 0, len(q.queuedVectors.toIndex))

	for _, j := range q.queuedVectors.toIndex {
		ids = append(ids, j.id)
		vectors = append(vectors, j.vector)
	}

	q.queuedVectors.RUnlock()

	return ids, vectors
}

func (q *IndexQueue) getQueueLen() int {
	q.queuedVectors.RLock()
	defer q.queuedVectors.RUnlock()

	return len(q.queuedVectors.toIndex)
}

func (q *IndexQueue) persist(vectors []indexQueueJob) error {
	buf := make([]byte, 8)

	for _, jj := range vectors {
		// store the id only
		binary.BigEndian.PutUint64(buf, jj.id)
		_, err := q.walBuffer.Write(buf)
		if err != nil {
			jj.done <- err
			continue
		}
	}

	// write to the file
	err := q.walBuffer.Flush()
	if err != nil {
		return err
	}

	// ensure the data is persisted to disk
	err = q.walFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

type batch struct {
	ids     []uint64
	vectors [][]float32
}

func (q *IndexQueue) indexVectors(b *batch) error {
	for {
		err := q.Index.AddBatch(b.ids, b.vectors)
		if err == nil {
			break
		}

		q.logger.WithError(err).Infof("failed to index vectors, retrying in %s", q.retryInterval.String())

		t := time.NewTimer(q.retryInterval)
		select {
		case <-q.closed:
			// drain the timer
			if !t.Stop() {
				<-t.C
			}
			return errors.New("index queue closed")
		case <-t.C:
		}
	}

	return nil
}

func (q *IndexQueue) notifyError(jobs []indexQueueJob, err error) {
	for _, j := range jobs {
		j.done <- err
	}
}

func (q *IndexQueue) notifySuccess(jobs []indexQueueJob) {
	q.notifyError(jobs, nil)
}

func (q *IndexQueue) reset() error {
	// reset the queue
	q.queuedVectors.Lock()
	q.queuedVectors.toIndex = q.queuedVectors.toIndex[:0]
	q.queuedVectors.Unlock()

	// reset the stale timer
	q.staleTm.Reset(q.maxStaleTime)

	// reset the on-disk queue
	return q.resetFile()
}

func (q *IndexQueue) resetFile() error {
	_, err := q.walFile.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "failed to reset wal file position")
	}

	err = q.walFile.Truncate(0)
	if err != nil {
		return errors.Wrap(err, "failed to truncate wal file")
	}

	// reset the buffer
	q.walBuffer.Reset(q.walFile)

	return nil
}

type pqMaxPool struct {
	pool *sync.Pool
}

func newPqMaxPool(defaultCap int) *pqMaxPool {
	return &pqMaxPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return priorityqueue.NewMax(defaultCap)
			},
		},
	}
}

func (pqh *pqMaxPool) GetMax(capacity int) *priorityqueue.Queue {
	pq := pqh.pool.Get().(*priorityqueue.Queue)
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMaxPool) Put(pq *priorityqueue.Queue) {
	pqh.pool.Put(pq)
}
