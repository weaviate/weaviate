//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"container/list"
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

	// keeps track of the last call to Push()
	lastPushed atomic.Pointer[time.Time]

	pqMaxPool *pqMaxPool

	checkpoints *indexcheckpoint.Checkpoints

	paused atomic.Bool
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
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
	ContainsNode(id uint64) bool
	Delete(id ...uint64) error
	DistancerProvider() distancer.Provider
}

type compressedIndexer interface {
	Compressed() bool
	AlreadyIndexed() uint64
	TurnOnCompression(callback func()) error
	ShouldCompress() (bool, int)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q.queue.wait(ctx)

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

	// store the time of the last push
	now := time.Now()
	q.lastPushed.Store(&now)

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

// PreloadShard goes through the LSM store from the last checkpoint
// and enqueues any unindexed vector.
func (q *IndexQueue) PreloadShard(shard ShardLike) error {
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

	maxDocID := shard.Counter().Get()

	var counter int

	ctx := context.Background()

	buf := make([]byte, 8)
	for i := checkpoint; i < maxDocID; i++ {
		binary.LittleEndian.PutUint64(buf, i)

		v, err := shard.Store().Bucket(helpers.ObjectsBucketLSM).GetBySecondary(0, buf)
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
		if shard.VectorIndex().ContainsNode(id) {
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

	workerNb := runtime.GOMAXPROCS(0) - 1

	for {
		select {
		case <-t.C:
			if q.Size() == 0 {
				_, _ = q.Shard.compareAndSwapStatus(storagestate.StatusIndexing.String(), storagestate.StatusReady.String())
				continue
			}
			if q.paused.Load() {
				continue
			}
			status, err := q.Shard.compareAndSwapStatus(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
			if status != storagestate.StatusIndexing || err != nil {
				q.Logger.WithField("status", status).WithError(err).Warn("failed to set shard status to 'indexing', trying again in " + q.IndexInterval.String())
				continue
			}

			lastPushed := q.lastPushed.Load()
			if lastPushed == nil || time.Since(*lastPushed) > time.Second {
				// send at most 2 times the number of workers in one go,
				// then wait for the next tick in case more vectors
				// are added to the queue
				q.pushToWorkers(2*workerNb, false)
			} else {
				// send only one batch at a time and wait for it to be indexed
				// to avoid competing for resources with the Push() method.
				// This ensures the resources are used for queueing vectors in priority,
				// then for indexing them.
				q.pushToWorkers(1, true)
			}
			q.checkCompressionSettings()
		case <-q.ctx.Done():
			// stop the ticker
			t.Stop()
			return
		}
	}
}

func (q *IndexQueue) pushToWorkers(max int, wait bool) {
	chunks := q.queue.borrowChunks(max)
	for i, c := range chunks {
		select {
		case <-q.ctx.Done():
			// release unsent borrowed chunks
			for _, c := range chunks[i:] {
				q.queue.releaseChunk(c)
			}

			return
		case q.indexCh <- job{
			chunk:   c,
			indexer: q.Index,
			queue:   q.queue,
			ctx:     q.ctx,
		}:
		}
	}

	if wait {
		q.queue.wait(q.ctx)
	}
}

// SearchByVector performs the search through the index first, then uses brute force to
// query unindexed vectors.
func (q *IndexQueue) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return q.search(vector, -1, k, allowList)
}

// SearchByVectorDistance performs the search through the index first, then uses brute force to
// query unindexed vectors.
func (q *IndexQueue) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return q.search(vector, dist, int(maxLimit), allowList)
}

func (q *IndexQueue) search(vector []float32, dist float32, maxLimit int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	var indexedResults []uint64
	var distances []float32
	var err error
	if dist == -1 {
		indexedResults, distances, err = q.Index.SearchByVector(vector, maxLimit, allowList)
	} else {
		indexedResults, distances, err = q.Index.SearchByVectorDistance(vector, dist, int64(maxLimit), allowList)
	}
	if err != nil {
		return nil, nil, err
	}

	if !asyncEnabled() {
		return indexedResults, distances, nil
	}

	if q.Index.DistancerProvider().Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}

	var results *priorityqueue.Queue[any]
	var seen map[uint64]struct{}

	err = q.queue.Iterate(allowList, func(objects []vectorDescriptor) error {
		if results == nil {
			results = q.pqMaxPool.GetMax(maxLimit)
			seen = make(map[uint64]struct{}, len(indexedResults))
			for i := range indexedResults {
				seen[indexedResults[i]] = struct{}{}
				results.Insert(indexedResults[i], distances[i])
			}
		}

		return q.bruteForce(vector, objects, maxLimit, results, allowList, dist, seen)
	})
	if results != nil {
		defer q.pqMaxPool.Put(results)
	}
	if err != nil {
		return nil, nil, err
	}
	if results == nil {
		return indexedResults, distances, nil
	}

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())

	i := results.Len() - 1
	for results.Len() > 0 {
		element := results.Pop()
		ids[i] = element.ID
		dists[i] = element.Dist
		i--
	}

	return ids, dists, nil
}

func (q *IndexQueue) checkCompressionSettings() {
	ci, ok := q.Index.(compressedIndexer)
	if !ok {
		return
	}

	shouldCompress, shouldCompressAt := ci.ShouldCompress()
	if !shouldCompress || ci.Compressed() {
		return
	}

	if ci.AlreadyIndexed() > uint64(shouldCompressAt) {
		q.pauseIndexing()
		err := ci.TurnOnCompression(q.resumeIndexing)
		if err != nil {
			q.Logger.WithError(err).Error("failed to turn on compression")
		}
	}
}

// pause indexing and wait for the workers to finish their current tasks
// related to this queue.
func (q *IndexQueue) pauseIndexing() {
	q.Logger.Debug("pausing indexing, waiting for the current tasks to finish")
	q.paused.Store(true)
	q.queue.wait(q.ctx)
	q.Logger.Debug("indexing paused")
}

// resume indexing
func (q *IndexQueue) resumeIndexing() {
	q.paused.Store(false)
	q.Logger.Debug("indexing resumed")
}

func (q *IndexQueue) bruteForce(vector []float32, snapshot []vectorDescriptor, k int,
	results *priorityqueue.Queue[any], allowList helpers.AllowList,
	maxDistance float32, seen map[uint64]struct{},
) error {
	for i := range snapshot {
		// skip indexed data
		if _, ok := seen[snapshot[i].id]; ok {
			continue
		}

		// skip filtered data
		if allowList != nil && !allowList.Contains(snapshot[i].id) {
			continue
		}

		v := snapshot[i].vector
		if q.Index.DistancerProvider().Type() == "cosine-dot" {
			// cosine-dot requires normalized vectors, as the dot product and cosine
			// similarity are only identical if the vector is normalized
			v = distancer.Normalize(v)
		}

		dist, _, err := q.Index.DistanceBetweenVectors(vector, v)
		if err != nil {
			return err
		}

		// skip vectors that are too far away
		if maxDistance > 0 && dist > maxDistance {
			continue
		}

		if k < 0 || results.Len() < k || dist < results.Top().Dist {
			results.Insert(snapshot[i].id, dist)
			if k > 0 {
				for results.Len() > k {
					results.Pop()
				}
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
				return priorityqueue.NewMax[any](defaultCap)
			},
		},
	}
}

func (pqh *pqMaxPool) GetMax(capacity int) *priorityqueue.Queue[any] {
	pq := pqh.pool.Get().(*priorityqueue.Queue[any])
	if pq.Cap() < capacity {
		pq.ResetCap(capacity)
	} else {
		pq.Reset()
	}

	return pq
}

func (pqh *pqMaxPool) Put(pq *priorityqueue.Queue[any]) {
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
				buf := make([]vectorDescriptor, iq.BatchSize)
				return &buf
			},
		},
	}

	q.fullChunks.list = list.New()
	q.deleted.m = make(map[uint64]struct{})

	return &q
}

func (q *vectorQueue) getBuffer() []vectorDescriptor {
	buff := *(q.pool.Get().(*[]vectorDescriptor))
	return buff[:q.IndexQueue.BatchSize]
}

func (q *vectorQueue) getFreeChunk() *chunk {
	c := chunk{
		data: q.getBuffer(),
	}
	c.indexed = make(chan struct{})
	return &c
}

func (q *vectorQueue) wait(ctx context.Context) {
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

func (q *vectorQueue) borrowChunks(max int) []*chunk {
	if max <= 0 {
		max = math.MaxInt64
	}

	q.fullChunks.Lock()
	var chunks []*chunk
	e := q.fullChunks.list.Front()
	count := 0
	for e != nil && count < max {
		c := e.Value.(*chunk)
		if !c.borrowed {
			count++
			c.borrowed = true
			chunks = append(chunks, c)
		}

		e = e.Next()
	}
	q.fullChunks.Unlock()

	if count < max {
		var incompleteChunk *chunk
		q.curBatch.Lock()
		if q.curBatch.c != nil && time.Since(*q.curBatch.c.createdAt) > q.IndexQueue.StaleTimeout && q.curBatch.c.cursor > 0 {
			q.curBatch.c.borrowed = true
			chunks = append(chunks, q.curBatch.c)
			incompleteChunk = q.curBatch.c
			q.curBatch.c = nil
		}
		q.curBatch.Unlock()

		// add the incomplete chunk to the full chunks list
		if incompleteChunk != nil {
			q.fullChunks.Lock()
			q.fullChunks.list.PushBack(incompleteChunk)
			q.fullChunks.Unlock()
		}
	}

	return chunks
}

func (q *vectorQueue) releaseChunk(c *chunk) {
	if c == nil {
		return
	}

	if c.indexed != nil {
		close(c.indexed)
	}

	q.fullChunks.Lock()
	if c.elem != nil {
		q.fullChunks.list.Remove(c.elem)
	}

	// reset the chunk to notify the search
	// that it was released
	c.borrowed = false
	c.cursor = 0
	c.elem = nil
	c.createdAt = nil
	c.indexed = nil
	data := c.data
	c.data = nil

	q.fullChunks.Unlock()

	if len(data) == q.IndexQueue.BatchSize {
		q.pool.Put(&data)
	}
}

// persistCheckpoint update the on-disk checkpoint that tracks the last indexed id
// optimistically. It is not guaranteed to be accurate but it is guaranteed to be lower
// than any vector in the queue.
// To calculate the checkpoint, we use the lowest id in the current batch
// minus the number of vectors in the queue (delta), which is capped at 10k vectors.
// The calculation looks like this:
// checkpoint = min(ids) - max(queueSize, 10_000)
func (q *vectorQueue) persistCheckpoint(ids []uint64) {
	if len(ids) == 0 {
		return
	}

	q.fullChunks.Lock()
	cl := q.fullChunks.list.Len()
	q.fullChunks.Unlock()

	// get the lowest id in the current batch
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

// Iterate through all chunks in the queue and call the given function.
// Deleted vectors are skipped, and if an allowlist is provided, only vectors
// in the allowlist are returned.
func (q *vectorQueue) Iterate(allowlist helpers.AllowList, fn func(objects []vectorDescriptor) error) error {
	buf := q.getBuffer()
	defer q.pool.Put(&buf)

	var count int

	// since chunks can get released concurrently,
	// we first get the pointers to all chunks.
	// then iterate over them.
	// This will not give us the latest data, but
	// will prevent us from losing access to the rest
	// of the linked list if an intermediate chunk is released.
	var elems []*list.Element
	q.fullChunks.Lock()
	e := q.fullChunks.list.Front()
	for e != nil {
		elems = append(elems, e)
		e = e.Next()
	}
	q.fullChunks.Unlock()

	for i := 0; i < len(elems); i++ {
		// we need to lock the list to prevent the chunk from being released
		q.fullChunks.Lock()
		c := elems[i].Value.(*chunk)
		if c.data == nil {
			// the chunk was released in the meantime,
			// skip it
			q.fullChunks.Unlock()
			continue
		}

		buf = buf[:0]
		for i := 0; i < c.cursor; i++ {
			if allowlist != nil && !allowlist.Contains(c.data[i].id) {
				continue
			}

			if q.IsDeleted(c.data[i].id) {
				continue
			}

			buf = append(buf, c.data[i])
			count++
			if count >= q.IndexQueue.BruteForceSearchLimit {
				break
			}
		}
		q.fullChunks.Unlock()

		if len(buf) == 0 {
			continue
		}

		err := fn(buf)
		if err != nil {
			return err
		}

		if count >= q.IndexQueue.BruteForceSearchLimit {
			break
		}
	}

	if count >= q.IndexQueue.BruteForceSearchLimit {
		return nil
	}

	buf = buf[:0]
	q.curBatch.Lock()
	if q.curBatch.c != nil {
		for i := 0; i < q.curBatch.c.cursor; i++ {
			c := q.curBatch.c

			if allowlist != nil && !allowlist.Contains(c.data[i].id) {
				continue
			}

			if q.IsDeleted(c.data[i].id) {
				continue
			}

			buf = append(buf, c.data[i])
			count++

			if count >= q.IndexQueue.BruteForceSearchLimit {
				break
			}
		}
	}
	q.curBatch.Unlock()

	if len(buf) == 0 {
		return nil
	}

	err := fn(buf)
	if err != nil {
		return err
	}

	return nil
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
