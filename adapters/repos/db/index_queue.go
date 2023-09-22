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
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/storobj"
)

// IndexQueue is an in-memory queue of vectors to index.
// It batches vectors together before sending them to the indexing workers.
// It is safe to use concurrently.
type IndexQueue struct {
	Index BatchIndexer

	IndexQueueOptions

	// processCh is the channel used to send vectors to the indexing worker.
	processCh chan []vectorDescriptor

	// indexCh is the channel used to send vectors to the indexing worker.
	indexCh chan *chunk

	// context used to close pending tasks
	// if canceled, prevents new vectors from being added to the queue.
	ctx      context.Context
	cancelFn context.CancelFunc

	// tracks the background workers
	wg sync.WaitGroup

	// queue of not-yet-indexed vectors
	queue *vectorQueue

	pqMaxPool *pqMaxPool

	bufPool sync.Pool

	lastIndexedCursor *minIndexedCursor
}

type vectorDescriptor struct {
	id     uint64
	vector []float32
}

type IndexQueueOptions struct {
	// BatchSize is the number of vectors to batch together
	// before sending them to the indexing worker.
	BatchSize int

	// IndexInterval is the maximum time to wait before sending
	// the pending vectors to the indexing worker.
	IndexInterval time.Duration

	// Logger is the logger used by the queue.
	Logger logrus.FieldLogger

	// RetryInterval is the interval between retries when
	// indexing fails.
	RetryInterval time.Duration

	// IndexWorkerCount is the number of workers used to index
	// the vectors.
	IndexWorkerCount int

	// Maximum number of vectors to use for brute force search
	// when vectors are not indexed.
	BruteForceSearchLimit int
}

type BatchIndexer interface {
	AddBatch(id []uint64, vector [][]float32) error
	SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
	ContainsNode(id uint64) bool
	Delete(id ...uint64) error
}

func NewIndexQueue(
	shardID string,
	rootPath string,
	index BatchIndexer,
	opts IndexQueueOptions,
) (*IndexQueue, error) {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.WithField("component", "index_queue")

	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}

	if opts.IndexInterval == 0 {
		opts.IndexInterval = 1 * time.Second
	}

	if opts.RetryInterval == 0 {
		opts.RetryInterval = 1 * time.Second
	}

	if opts.IndexWorkerCount == 0 {
		opts.IndexWorkerCount = runtime.GOMAXPROCS(0) - 1
	}

	if opts.BruteForceSearchLimit == 0 {
		opts.BruteForceSearchLimit = 100_000
	}

	q := IndexQueue{
		IndexQueueOptions: opts,
		Index:             index,
		queue:             newVectorQueue(opts.BatchSize),
		processCh:         make(chan []vectorDescriptor),
		indexCh:           make(chan *chunk),
		pqMaxPool:         newPqMaxPool(0),
		bufPool: sync.Pool{
			New: func() any {
				buf := make([]vectorDescriptor, 0, 10*opts.BatchSize)
				return &buf
			},
		},
	}

	var err error

	q.lastIndexedCursor, err = newMinIndexedCursor(shardID, rootPath)
	if err != nil {
		return nil, errors.Wrap(err, "initialize min indexed cursor")
	}

	q.ctx, q.cancelFn = context.WithCancel(context.Background())

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()

		q.enqueuer()
	}()

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		defer close(q.indexCh)

		q.indexer()
	}()

	for i := 0; i < opts.IndexWorkerCount; i++ {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()

			q.worker()
		}()
	}

	return &q, nil
}

// Close immediately closes the queue and waits for workers to finish their current tasks.
// Any pending vectors are discarded.
func (q *IndexQueue) Close() error {
	// check if the queue is closed
	select {
	case <-q.ctx.Done():
		return errors.New("index queue closed")
	default:
	}

	// prevent new jobs from being added
	q.cancelFn()

	// close the workers in cascade
	close(q.processCh)

	q.wg.Wait()

	return q.lastIndexedCursor.Close()
}

// Push adds a list of vectors to the queue.
func (q *IndexQueue) Push(ctx context.Context, vectors ...vectorDescriptor) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.ctx.Done():
		return errors.New("index queue closed")
	case q.processCh <- vectors:
		return nil
	}
}

// Size returns the number of vectors waiting to be indexed.
func (q *IndexQueue) Size() int64 {
	var count int64
	q.queue.fullChunks.Lock()
	e := q.queue.fullChunks.list.Front()
	for e != nil {
		c := e.Value.(*chunk)
		count += int64(c.cursor)

		e = e.Next()
	}
	q.queue.fullChunks.Unlock()

	q.queue.curBatch.Lock()
	count += int64(q.queue.curBatch.c.cursor)
	q.queue.curBatch.Unlock()

	return count
}

// Delete marks the given vectors as deleted.
// This method can be called even if the async indexing is disabled.
func (q *IndexQueue) Delete(ids ...uint64) error {
	if !asyncEnabled() {
		return q.Index.Delete(ids...)
	}

	remaining := make([]uint64, 0, len(ids))
	indexed := make([]uint64, 0, len(ids))

	for _, id := range ids {
		if q.Index.ContainsNode(id) {
			indexed = append(indexed, id)

			// is it already marked as deleted in the queue?
			if q.queue.IsDeleted(id) {
				q.queue.ResetDeleted(id)
			}

			continue
		}

		remaining = append(remaining, id)
	}

	err := q.Index.Delete(indexed...)
	if err != nil {
		return errors.Wrap(err, "delete node from index")
	}

	q.queue.Delete(remaining)

	return nil
}

// Push adds a list of vectors to the queue.
func (q *IndexQueue) PreloadShard(ctx context.Context, shard *Shard) error {
	if !asyncEnabled() {
		return nil
	}

	// load non-indexed vectors and add them to the queue
	lastID := q.lastIndexedCursor.Get()
	if lastID == 0 {
		return nil
	}

	start := time.Now()

	maxDocID := shard.counter.Get()

	var counter int

	buf := make([]byte, 8)
	for i := lastID; i < maxDocID; i++ {
		binary.LittleEndian.PutUint64(buf, i)

		v, err := shard.store.Bucket(helpers.ObjectsBucketLSM).GetBySecondary(0, buf)
		if err != nil {
			return errors.Wrap(err, "get last indexed object")
		}
		if v == nil {
			continue
		}
		obj, err := storobj.FromBinary(v)
		if err != nil {
			return errors.Wrap(err, "unmarshal last indexed object")
		}
		id := obj.DocID()
		if shard.vectorIndex.ContainsNode(id) {
			continue
		}
		if len(obj.Vector) == 0 {
			continue
		}
		counter++

		desc := vectorDescriptor{
			id:     id,
			vector: obj.Vector,
		}
		err = q.Push(ctx, desc)
		if err != nil {
			return err
		}
	}

	q.Logger.
		WithField("id", lastID).
		WithField("maxDocID", maxDocID).
		WithField("count", counter).
		WithField("took", time.Since(start)).
		Debug("enqueued vectors from last indexed cursor")

	return nil
}

func (q *IndexQueue) enqueuer() {
	for batch := range q.processCh {
		q.queue.Add(batch)
	}
}

func (q *IndexQueue) indexer() {
	t := time.NewTicker(q.IndexInterval)

	for {
		select {
		case <-t.C:
			err := q.pushToWorkers()
			if err != nil {
				q.Logger.WithError(err).Error("failed to index vectors")
				return
			}
		case <-q.ctx.Done():
			// stop the ticker
			t.Stop()
			return
		}
	}
}

func (q *IndexQueue) pushToWorkers() error {
	chunks := q.queue.borrowAllChunks()
	for _, c := range chunks {
		select {
		case <-q.ctx.Done():
			return errors.New("index queue closed")
		case q.indexCh <- c:
		}
	}

	return nil
}

func (q *IndexQueue) worker() {
	var ids []uint64
	var vectors [][]float32
	var deleted []uint64

	for b := range q.indexCh {
		for i := range b.data[:b.cursor] {
			if q.queue.IsDeleted(b.data[i].id) {
				deleted = append(deleted, b.data[i].id)
			} else {
				ids = append(ids, b.data[i].id)
				vectors = append(vectors, b.data[i].vector)
			}
		}

		if err := q.indexVectors(ids, vectors); err != nil {
			q.Logger.WithError(err).Error("failed to index vectors")
			return
		}

		// update the on-disk cursor that tracks the minimum indexed id
		// with a value that is guaranteed to be lower than the last lowest indexed id.
		q.queue.fullChunks.Lock()
		cl := q.queue.fullChunks.list.Len()
		q.queue.fullChunks.Unlock()
		// Determine a safe value to use as the new cursor.
		// The value doesn't have to be accurate but it must guarantee
		// that it is lower than the last indexed id.
		if len(ids) > 0 {
			var minID uint64
			for _, id := range ids {
				if minID == 0 || id < minID {
					minID = id
				}
			}

			delta := uint64(cl * q.BatchSize)
			// cap the delta to 10k vectors
			if delta > 10_000 {
				delta = 10_000
			}
			var safeID uint64
			if minID > delta {
				safeID = minID - delta
			} else {
				safeID = 0
			}

			q.lastIndexedCursor.Update(safeID)
		}

		// put the chunk back in the pool
		q.queue.releaseChunk(b)

		// remove the deleted ids from the mask
		if len(deleted) > 0 {
			q.queue.ResetDeleted(deleted...)
		}

		ids = ids[:0]
		vectors = vectors[:0]
		deleted = deleted[:0]
	}
}

func (q *IndexQueue) indexVectors(ids []uint64, vectors [][]float32) error {
	for {
		err := q.Index.AddBatch(ids, vectors)
		if err == nil {
			return nil
		}

		q.Logger.WithError(err).Infof("failed to index vectors, retrying in %s", q.RetryInterval.String())

		t := time.NewTimer(q.RetryInterval)
		select {
		case <-q.ctx.Done():
			// drain the timer
			if !t.Stop() {
				<-t.C
			}
			return errors.New("index queue closed")
		case <-t.C:
		}
	}
}

// SearchByVector performs the search through the index first, then uses brute force to
// query unindexed vectors.
func (q *IndexQueue) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	indexedResults, distances, err := q.Index.SearchByVector(vector, k, allowList)
	if err != nil {
		return nil, nil, err
	}

	if !asyncEnabled() {
		return indexedResults, distances, nil
	}

	results := q.pqMaxPool.GetMax(k)
	defer q.pqMaxPool.Put(results)
	for i := range indexedResults {
		results.Insert(indexedResults[i], distances[i])
	}

	buf := q.bufPool.Get().(*[]vectorDescriptor)
	snapshot := q.queue.AppendSnapshot((*buf)[:0], q.BruteForceSearchLimit)

	ids := make([]uint64, len(snapshot))
	vectors := make([][]float32, len(snapshot))
	for i, v := range snapshot {
		ids[i] = v.id
		vectors[i] = v.vector
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
		if results.Len() == 0 {
			break
		}
		element := results.Pop()
		ids[i] = element.ID
		distances[i] = element.Dist
	}

	q.bufPool.Put(&snapshot)

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

type vectorQueue struct {
	batchSize int
	pool      sync.Pool
	curBatch  struct {
		sync.Mutex

		c *chunk
	}
	fullChunks struct {
		sync.Mutex

		list *list.List
	}
	deleted struct {
		sync.RWMutex

		m map[uint64]struct{}
	}
}

func newVectorQueue(batchSize int) *vectorQueue {
	q := vectorQueue{
		batchSize: batchSize,
		pool: sync.Pool{
			New: func() any {
				return &chunk{
					data: make([]vectorDescriptor, batchSize),
				}
			},
		},
	}

	q.fullChunks.list = list.New()
	q.curBatch.c = q.getFreeChunk()
	q.deleted.m = make(map[uint64]struct{})

	return &q
}

func (q *vectorQueue) getFreeChunk() *chunk {
	return q.pool.Get().(*chunk)
}

func (q *vectorQueue) Add(vectors []vectorDescriptor) {
	var full []*chunk

	q.curBatch.Lock()
	f := q.ensureHasSpace()
	if f != nil {
		full = append(full, f)
	}

	for len(vectors) != 0 {
		curBatch := q.curBatch.c
		n := copy(curBatch.data[curBatch.cursor:], vectors)
		curBatch.cursor += n

		vectors = vectors[n:]

		f := q.ensureHasSpace()
		if f != nil {
			full = append(full, f)
		}
	}
	q.curBatch.Unlock()

	if len(full) > 0 {
		q.fullChunks.Lock()
		for _, f := range full {
			f.elem = q.fullChunks.list.PushBack(f)
		}
		q.fullChunks.Unlock()
	}
}

func (q *vectorQueue) ensureHasSpace() *chunk {
	if q.curBatch.c.cursor < q.batchSize {
		return nil
	}

	c := q.curBatch.c
	q.curBatch.c = q.getFreeChunk()
	return c
}

func (q *vectorQueue) borrowAllChunks() []*chunk {
	q.fullChunks.Lock()

	var chunks []*chunk
	e := q.fullChunks.list.Front()
	for e != nil {
		c := e.Value.(*chunk)
		if !c.borrowed {
			c.borrowed = true
			chunks = append(chunks, c)
		}

		e = e.Next()
	}

	q.fullChunks.Unlock()

	return chunks
}

func (q *vectorQueue) releaseChunk(c *chunk) {
	q.fullChunks.Lock()
	q.fullChunks.list.Remove(c.elem)
	q.fullChunks.Unlock()

	c.borrowed = false
	c.cursor = 0
	c.elem = nil

	q.pool.Put(c)
}

func (q *vectorQueue) AppendSnapshot(buf []vectorDescriptor, limit int) []vectorDescriptor {
	q.fullChunks.Lock()
	e := q.fullChunks.list.Front()
	var count int
	for e != nil && count < limit {
		c := e.Value.(*chunk)
		for i := 0; i < c.cursor; i++ {
			if !q.IsDeleted(c.data[i].id) {
				buf = append(buf, c.data[i])
				count++
			}
		}

		e = e.Next()
	}
	q.fullChunks.Unlock()

	if count >= limit {
		return buf
	}

	q.curBatch.Lock()
	for i := 0; i < q.curBatch.c.cursor && count < limit; i++ {
		if !q.IsDeleted(q.curBatch.c.data[i].id) {
			buf = append(buf, q.curBatch.c.data[i])
			count++
		}
	}
	q.curBatch.Unlock()

	return buf
}

func (q *vectorQueue) Delete(ids []uint64) {
	q.deleted.Lock()
	for _, id := range ids {
		q.deleted.m[id] = struct{}{}
	}
	q.deleted.Unlock()
}

func (q *vectorQueue) IsDeleted(id uint64) bool {
	q.deleted.RLock()
	_, ok := q.deleted.m[id]
	q.deleted.RUnlock()
	return ok
}

func (q *vectorQueue) ResetDeleted(id ...uint64) {
	q.deleted.Lock()
	for _, id := range id {
		delete(q.deleted.m, id)
	}
	q.deleted.Unlock()
}

type chunk struct {
	cursor   int
	borrowed bool
	data     []vectorDescriptor
	elem     *list.Element
}

type minIndexedCursor struct {
	sync.Mutex
	id uint64
	f  *os.File

	flushCh chan struct{}
}

func newMinIndexedCursor(shardID string, rootPath string) (*minIndexedCursor, error) {
	fileName := fmt.Sprintf("%s/%s.minindexed", rootPath, shardID)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	c := minIndexedCursor{
		f:       f,
		flushCh: make(chan struct{}, 1),
	}

	if stat.Size() > 0 {
		// the file has existed before, we need to initialize with its content
		err = binary.Read(f, binary.LittleEndian, &c.id)
		if err != nil {
			return nil, errors.Wrap(err, "read initial count from file")
		}
	}

	go func() {
		for range c.flushCh {
			err = c.Flush()
			if err != nil {
				logrus.WithError(err).Error("flush min indexed cursor")
			}
		}
	}()

	return &c, nil
}

func (c *minIndexedCursor) Close() error {
	close(c.flushCh)

	return c.f.Close()
}

func (c *minIndexedCursor) Get() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.id
}

func (c *minIndexedCursor) Update(id uint64) {
	c.Lock()
	defer c.Unlock()
	if id <= c.id {
		return
	}
	c.id = id

	select {
	case c.flushCh <- struct{}{}:
	default:
	}
}

func (c *minIndexedCursor) Flush() error {
	c.Lock()
	defer c.Unlock()

	c.f.Seek(0, 0)
	err := binary.Write(c.f, binary.LittleEndian, &c.id)
	if err != nil {
		return errors.Wrap(err, "increase cursor on disk")
	}
	c.f.Seek(0, 0)

	return nil
}
