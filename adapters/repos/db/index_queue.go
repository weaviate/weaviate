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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

// IndexQueue is an in-memory queue of vectors to index.
// It batches vectors together before sending them to the indexing workers.
// It is safe to use concurrently.
type IndexQueue struct {
	Shard   shardStatusUpdater
	Index   batchIndexer
	shardID string

	IndexQueueOptions

	// indexCh is the channel used to send vectors to the shared indexing workers.
	indexCh chan job

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

	checkpoints *indexcheckpoint.Checkpoints
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

	// Max time a vector can stay in the queue before being indexed.
	StaleTimeout time.Duration

	// Logger is the logger used by the queue.
	Logger logrus.FieldLogger

	// Maximum number of vectors to use for brute force search
	// when vectors are not indexed.
	BruteForceSearchLimit int
}

type batchIndexer interface {
	AddBatch(id []uint64, vector [][]float32) error
	SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
	ContainsNode(id uint64) bool
	Delete(id ...uint64) error
}
type shardStatusUpdater interface {
	compareAndSwapStatus(old, new string) (storagestate.Status, error)
}

func NewIndexQueue(
	shardID string,
	shard shardStatusUpdater,
	index batchIndexer,
	centralJobQueue chan job,
	checkpoints *indexcheckpoint.Checkpoints,
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

	if opts.BruteForceSearchLimit == 0 {
		opts.BruteForceSearchLimit = 100_000
	}

	if opts.StaleTimeout == 0 {
		opts.StaleTimeout = 1 * time.Minute
	}

	q := IndexQueue{
		shardID:           shardID,
		IndexQueueOptions: opts,
		Shard:             shard,
		Index:             index,
		indexCh:           centralJobQueue,
		pqMaxPool:         newPqMaxPool(0),
		checkpoints:       checkpoints,
		bufPool: sync.Pool{
			New: func() any {
				buf := make([]vectorDescriptor, 0, opts.BatchSize)
				return &buf
			},
		},
	}

	q.queue = newVectorQueue(&q)

	q.ctx, q.cancelFn = context.WithCancel(context.Background())

	if !asyncEnabled() {
		return &q, nil
	}

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()

		q.indexer()
	}()

	return &q, nil
}

// Close immediately closes the queue and waits for workers to finish their current tasks.
// Any pending vectors are discarded.
func (q *IndexQueue) Close() error {
	// check if the queue is closed
	if q.ctx.Err() != nil {
		return nil
	}

	// prevent new jobs from being added
	q.cancelFn()

	q.wg.Wait()

	// loop over the chunks of the queue
	// wait for the done chan to be closed
	// then return
	q.queue.wait()

	return nil
}

// Push adds a list of vectors to the queue.
func (q *IndexQueue) Push(ctx context.Context, vectors ...vectorDescriptor) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if q.ctx.Err() != nil {
		return errors.New("index queue closed")
	}

	q.queue.Add(vectors)
	return nil
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
	if q.queue.curBatch.c != nil {
		count += int64(q.queue.curBatch.c.cursor)
	}
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
	checkpoint, err := q.checkpoints.Get(q.shardID)
	if err != nil {
		return errors.Wrap(err, "get last indexed id")
	}
	if checkpoint == 0 {
		return nil
	}

	start := time.Now()

	maxDocID := shard.counter.Get()

	var counter int

	buf := make([]byte, 8)
	for i := checkpoint; i < maxDocID; i++ {
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
		WithField("checkpoint", checkpoint).
		WithField("last_stored_id", maxDocID).
		WithField("count", counter).
		WithField("took", time.Since(start)).
		WithField("shard_id", q.shardID).
		Debug("enqueued vectors from last indexed checkpoint")

	return nil
}

// Drop removes all persisted data related to the queue.
// It closes the queue if not already.
// It does not remove the index.
// It should be called only when the index is dropped.
func (q *IndexQueue) Drop() error {
	_ = q.Close()

	if q.checkpoints != nil {
		return q.checkpoints.Delete(q.shardID)
	}

	return nil
}

func (q *IndexQueue) indexer() {
	t := time.NewTicker(q.IndexInterval)

	for {
		select {
		case <-t.C:
			if q.Size() == 0 {
				_, _ = q.Shard.compareAndSwapStatus(storagestate.StatusIndexing.String(), storagestate.StatusReady.String())
				continue
			}
			status, err := q.Shard.compareAndSwapStatus(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
			if status != storagestate.StatusIndexing || err != nil {
				q.Logger.WithField("status", status).WithError(err).Error("failed to set shard status to indexing")
				continue
			}
			err = q.pushToWorkers()
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
	for i, c := range chunks {
		select {
		case <-q.ctx.Done():
			// release unsent borrowed chunks
			for _, c := range chunks[i:] {
				q.queue.releaseChunk(c)
			}

			return errors.New("index queue closed")
		case q.indexCh <- job{
			chunk:   c,
			indexer: q.Index,
			queue:   q.queue,
			ctx:     q.ctx,
		}:
		}
	}

	return nil
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
	IndexQueue *IndexQueue
	pool       sync.Pool
	curBatch   struct {
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

func newVectorQueue(iq *IndexQueue) *vectorQueue {
	q := vectorQueue{
		IndexQueue: iq,
		pool: sync.Pool{
			New: func() any {
				return &chunk{
					data: make([]vectorDescriptor, iq.BatchSize),
				}
			},
		},
	}

	q.fullChunks.list = list.New()
	q.deleted.m = make(map[uint64]struct{})

	return &q
}

func (q *vectorQueue) getFreeChunk() *chunk {
	c := q.pool.Get().(*chunk)
	c.indexed = make(chan struct{})
	return c
}

func (q *vectorQueue) wait() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		// get first non-closed channel
		var ch chan struct{}

		q.fullChunks.Lock()
		e := q.fullChunks.list.Front()
	LOOP:
		for e != nil {
			c := e.Value.(*chunk)
			if c.borrowed {
				select {
				case <-c.indexed:
				default:
					ch = c.indexed
					break LOOP
				}
			}

			e = e.Next()
		}
		q.fullChunks.Unlock()

		if ch == nil {
			return
		}

		select {
		case <-ch:
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
	}
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
	if q.curBatch.c == nil {
		q.curBatch.c = q.getFreeChunk()
	}

	if q.curBatch.c.cursor == 0 {
		now := time.Now()
		q.curBatch.c.createdAt = &now
	}

	if q.curBatch.c.cursor < q.IndexQueue.BatchSize {
		return nil
	}

	c := q.curBatch.c
	q.curBatch.c = q.getFreeChunk()
	now := time.Now()
	q.curBatch.c.createdAt = &now
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

	q.curBatch.Lock()
	if q.curBatch.c != nil && time.Since(*q.curBatch.c.createdAt) > q.IndexQueue.StaleTimeout && q.curBatch.c.cursor > 0 {
		q.curBatch.c.borrowed = true
		chunks = append(chunks, q.curBatch.c)
		q.curBatch.c = nil
	}
	q.curBatch.Unlock()

	return chunks
}

func (q *vectorQueue) releaseChunk(c *chunk) {
	close(c.indexed)

	if c.elem != nil {
		q.fullChunks.Lock()
		q.fullChunks.list.Remove(c.elem)
		q.fullChunks.Unlock()
	}

	c.borrowed = false
	c.cursor = 0
	c.elem = nil
	c.createdAt = nil
	c.indexed = nil

	q.pool.Put(c)
}

func (q *vectorQueue) persistCheckpoint(ids []uint64) {
	if len(ids) == 0 {
		return
	}

	// update the on-disk checkpoint that tracks the minimum indexed id
	// with a value that is guaranteed to be lower than the last lowest indexed id.
	q.fullChunks.Lock()
	cl := q.fullChunks.list.Len()
	q.fullChunks.Unlock()
	// Determine a safe value to use as the new checkpoint.
	// The value doesn't have to be accurate but it must guarantee
	// that it is lower than the last indexed id.
	var minID uint64
	for _, id := range ids {
		if minID == 0 || id < minID {
			minID = id
		}
	}

	delta := uint64(cl * q.IndexQueue.BatchSize)
	// cap the delta to 10k vectors
	if delta > 10_000 {
		delta = 10_000
	}
	var checkpoint uint64
	if minID > delta {
		checkpoint = minID - delta
	} else {
		checkpoint = 0
	}

	err := q.IndexQueue.checkpoints.Update(q.IndexQueue.shardID, checkpoint)
	if err != nil {
		q.IndexQueue.Logger.WithError(err).Error("update checkpoint")
	}
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
	cursor    int
	borrowed  bool
	data      []vectorDescriptor
	elem      *list.Element
	createdAt *time.Time
	indexed   chan struct{}
}
