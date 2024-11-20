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
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/internal/monitoring"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// IndexQueue is an in-memory queue of vectors to index.
// It batches vectors together before sending them to the indexing workers.
// It is safe to use concurrently.
type IndexQueue struct {
	Shard        shardStatusUpdater
	index        batchIndexer
	shardID      string
	targetVector string

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

	Checkpoints *indexcheckpoint.Checkpoints

	paused atomic.Bool

	// prevents replacing the index while
	// the queue is dequeuing vectors
	indexLock sync.RWMutex

	metrics *IndexQueueMetrics

	// tracks the jobs that are currently being processed
	processingJobs *common.SharedGauge

	// tracks the last time vectors were added to the queue
	lastPushed atomic.Pointer[time.Time]

	// tracks the dimensions of the vectors in the queue
	dims atomic.Int32
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

	// Maximum number of chunks to send to the workers in a single tick.
	MaxChunksPerTick int

	// Enable throttling of the indexing process.
	Throttle bool
}

type batchIndexer interface {
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	DistanceBetweenVectors(x, y []float32) (float32, error)
	ContainsNode(id uint64) bool
	Delete(id ...uint64) error
	DistancerProvider() distancer.Provider
	AlreadyIndexed() uint64
	ValidateBeforeInsert(vector []float32) error
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
}

type shardStatusUpdater interface {
	compareAndSwapStatusIndexingAndReady(old, new string) (storagestate.Status, error)
	Name() string
}

func NewIndexQueue(
	className string,
	shardID string,
	targetVector string,
	shard shardStatusUpdater,
	index batchIndexer,
	centralJobQueue chan job,
	checkpoints *indexcheckpoint.Checkpoints,
	opts IndexQueueOptions,
	promMetrics *monitoring.PrometheusMetrics,
) (*IndexQueue, error) {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.
		WithField("component", "index_queue").
		WithField("shard_id", shardID)

	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}
	if v, _ := strconv.Atoi(os.Getenv("ASYNC_BATCH_SIZE")); v > 0 {
		opts.BatchSize = v
	}

	if opts.IndexInterval == 0 {
		opts.IndexInterval = 1 * time.Second
	}
	if v, _ := time.ParseDuration(os.Getenv("ASYNC_INDEX_INTERVAL")); v > 0 {
		opts.IndexInterval = v
	}

	if opts.BruteForceSearchLimit == 0 {
		opts.BruteForceSearchLimit = 100_000
	}
	if v, err := strconv.Atoi(os.Getenv("ASYNC_BRUTE_FORCE_SEARCH_LIMIT")); err == nil && v >= 0 {
		opts.BruteForceSearchLimit = v
	}

	if opts.StaleTimeout == 0 {
		opts.StaleTimeout = 5 * time.Second
	}
	if v, _ := time.ParseDuration(os.Getenv("ASYNC_STALE_TIMEOUT")); v > 0 {
		opts.StaleTimeout = v
	}

	if opts.MaxChunksPerTick == 0 {
		opts.MaxChunksPerTick = runtime.GOMAXPROCS(0) - 1
	}
	if v, _ := strconv.Atoi(os.Getenv("ASYNC_MAX_CHUNKS_PER_TICK")); v > 0 {
		opts.MaxChunksPerTick = v
	}
	if entcfg.Enabled(os.Getenv("ASYNC_THROTTLING")) {
		opts.Throttle = true
	}

	q := IndexQueue{
		shardID:           shardID,
		targetVector:      targetVector,
		IndexQueueOptions: opts,
		Shard:             shard,
		index:             index,
		indexCh:           centralJobQueue,
		pqMaxPool:         newPqMaxPool(0),
		Checkpoints:       checkpoints,
		metrics:           NewIndexQueueMetrics(opts.Logger, promMetrics, className, shard.Name(), targetVector),
		processingJobs:    common.NewSharedGauge(),
	}

	q.queue = newVectorQueue(&q)

	q.ctx, q.cancelFn = context.WithCancel(context.Background())

	if !asyncEnabled() {
		return &q, nil
	}

	q.wg.Add(1)
	f := func() {
		defer q.wg.Done()

		q.indexer()
	}
	enterrors.GoWrapper(f, q.Logger)

	return &q, nil
}

// Close immediately closes the queue and waits for workers to finish their current tasks.
// Any pending vectors are discarded.
func (q *IndexQueue) Close() error {
	if q == nil {
		// queue was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	// check if the queue is closed
	if q.ctx.Err() != nil {
		return nil
	}

	// prevent new jobs from being added
	q.cancelFn()

	q.wg.Wait()

	// wait for the workers to finish their current tasks
	q.processingJobs.Wait()

	q.Logger.Debug("index queue closed")

	return nil
}

// Reset the queue with the given VectorIndex.
// - discard any pending vectors
// - reset the checkpoint to 0
// Requires the queue to be paused.
// It does not resume the queue.
func (q *IndexQueue) ResetWith(v batchIndexer) error {
	q.indexLock.Lock()
	defer q.indexLock.Unlock()

	q.PauseIndexing()

	q.index = v
	err := q.Checkpoints.Update(q.shardID, q.targetVector, 0)
	if err != nil {
		return errors.Wrap(err, "update checkpoint")
	}

	q.lastPushed.Store(nil)
	q.dims.Store(0)
	q.queue.Reset()
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
	q.indexLock.RLock()
	defer q.indexLock.RUnlock()

	now := time.Now()
	defer q.metrics.Push(now, len(vectors))

	q.lastPushed.Store(&now)

	// ensure the vector length is the same
	for i := range vectors {
		if len(vectors[i].vector) == 0 {
			return errors.Errorf("vector is empty")
		}

		// delegate the validation to the index
		err := q.index.ValidateBeforeInsert(vectors[i].vector)
		if err != nil {
			return errors.WithStack(err)
		}

		// if the index is still empty, ensure the first batch is consistent
		// by keeping track of the dimensions of the vectors.
		if q.dims.CompareAndSwap(0, int32(len(vectors[i].vector))) {
			continue
		}

		if q.dims.Load() != int32(len(vectors[i].vector)) {
			return errors.Errorf("inconsistent vector lengths: %d != %d", len(vectors[i].vector), q.dims.Load())
		}
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

	q.metrics.Size(count)
	return count
}

// Deletes the vectors from the index synchronously
// if they are already indexed, otherwise it marks them as deleted in the queue.
// If async indexing is disabled, it calls the index delete method directly.
func (q *IndexQueue) Delete(ids ...uint64) error {
	if len(ids) == 0 {
		return nil
	}

	start := time.Now()
	defer q.metrics.Delete(start, len(ids))

	if !asyncEnabled() {
		return q.index.Delete(ids...)
	}

	q.indexLock.RLock()
	defer q.indexLock.RUnlock()

	for i := range ids {
		if q.index.ContainsNode(ids[i]) {
			err := q.index.Delete(ids[i])
			if err != nil {
				q.Logger.WithError(err).Error("failed to delete vector from index")
			}
		} else {
			q.queue.Delete(ids[i])
		}
	}

	return nil
}

// Drop removes all persisted data related to the queue.
// It closes the queue if not already.
// It does not remove the index.
// It should be called only when the index is dropped.
func (q *IndexQueue) Drop() error {
	_ = q.Close()

	if q.Checkpoints != nil {
		return q.Checkpoints.Delete(q.shardID, q.targetVector)
	}

	q.Logger.Debug("index queue dropped")

	return nil
}

func (q *IndexQueue) indexer() {
	t := time.NewTicker(q.IndexInterval)

	maxPerTick := q.MaxChunksPerTick

	for {
		select {
		case <-t.C:
			if q.paused.Load() {
				continue
			}

			q.indexLock.RLock()
			if q.checkCompressionSettings() {
				q.indexLock.RUnlock()
				continue
			}

			// cleanup deleted vectors from the index
			err := q.cleanupDeleted()
			if err != nil {
				q.Logger.WithError(err).Error("cleanup deleted vectors")
			}

			if q.Size() == 0 {
				_, _ = q.Shard.compareAndSwapStatusIndexingAndReady(storagestate.StatusIndexing.String(), storagestate.StatusReady.String())
				q.indexLock.RUnlock()
				continue
			}

			status, err := q.Shard.compareAndSwapStatusIndexingAndReady(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
			if status != storagestate.StatusIndexing || err != nil {
				q.Logger.WithField("status", status).WithError(err).Warn("failed to set shard status to 'indexing', trying again in " + q.IndexInterval.String())
				q.indexLock.RUnlock()
				continue
			}

			lastPushed := q.lastPushed.Load()
			var vectorsSent int64
			if !q.Throttle || lastPushed == nil || time.Since(*lastPushed) > time.Second {
				// send at most maxPerTick chunks at a time, without waiting for them to be indexed
				vectorsSent = q.pushToWorkers(maxPerTick, false)
			} else {
				// send only one batch at a time and wait for it to be indexed
				// to avoid competing for resources with the inverted index.
				// This ensures the resources are used for storing / queueing vectors in priority.
				vectorsSent = q.pushToWorkers(1, true)
			}

			q.logStats(vectorsSent)
			q.indexLock.RUnlock()
		case <-q.ctx.Done():
			// stop the ticker
			t.Stop()
			return
		}
	}
}

func (q *IndexQueue) pushToWorkers(max int, wait bool) int64 {
	chunks := q.queue.dequeue(max)
	if len(chunks) == 0 {
		return 0
	}

	var minID uint64
	var count int64

	for i := range chunks {
		c := chunks[i]
		ids := make([]uint64, 0, c.cursor)
		vectors := make([][]float32, 0, c.cursor)
		var deleted []uint64

		for j := range c.data[:c.cursor] {
			if minID == 0 || c.data[j].id < minID {
				minID = c.data[j].id
			}

			if q.queue.IsDeleted(c.data[j].id) {
				deleted = append(deleted, c.data[j].id)
			} else {
				ids = append(ids, c.data[j].id)
				vectors = append(vectors, c.data[j].vector)
			}
		}

		q.queue.ResetDeleted(deleted...)

		if len(ids) == 0 {
			q.Logger.Debug("all vectors in the chunk are deleted. skipping")
			continue
		}

		q.processingJobs.Incr()

		count += int64(len(ids))

		select {
		case <-q.ctx.Done():
			q.processingJobs.Decr()
			return count
		case q.indexCh <- job{
			indexer: q.index,
			ctx:     q.ctx,
			ids:     ids,
			vectors: vectors,
			done:    q.processingJobs.Decr,
		}:
		}
	}

	q.queue.persistCheckpoint(minID)

	if wait {
		q.processingJobs.Wait()
	}

	return count
}

func (q *IndexQueue) cleanupDeleted() error {
	q.queue.deleted.Lock()
	defer q.queue.deleted.Unlock()

	for id := range q.queue.deleted.m {
		if q.index.ContainsNode(id) {
			err := q.index.Delete(id)
			if err != nil {
				return errors.Wrapf(err, "delete vector %d from index", id)
			}

			delete(q.queue.deleted.m, id)
		}
	}

	return nil
}

func (q *IndexQueue) logStats(vectorsSent int64) {
	q.metrics.VectorsDequeued(vectorsSent)

	qSize := q.Size()
	q.Logger.
		WithField("queue_size", qSize).
		WithField("vectors_sent", vectorsSent).
		Debug("queue stats")
}

// SearchByVector performs the search through the index first, then uses brute force to
// query unindexed vectors.
func (q *IndexQueue) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return q.search(ctx, vector, -1, k, allowList)
}

// SearchByVectorDistance performs the search through the index first, then uses brute force to
// query unindexed vectors.
func (q *IndexQueue) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return q.search(ctx, vector, dist, int(maxLimit), allowList)
}

func (q *IndexQueue) search(ctx context.Context, vector []float32, dist float32, maxLimit int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	start := time.Now()
	defer q.metrics.Search(start)

	q.indexLock.RLock()
	defer q.indexLock.RUnlock()

	var indexedResults []uint64
	var distances []float32
	var err error
	if dist == -1 {
		indexedResults, distances, err = q.index.SearchByVector(ctx, vector, maxLimit, allowList)
	} else {
		indexedResults, distances, err = q.index.SearchByVectorDistance(ctx, vector, dist, int64(maxLimit), allowList)
	}
	if err != nil {
		return nil, nil, err
	}

	// Skip merging brute force results if async indexing disabled or brute force search limit is 0
	if !asyncEnabled() || q.BruteForceSearchLimit == 0 {
		return indexedResults, distances, nil
	}

	if q.index.DistancerProvider().Type() == "cosine-dot" {
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

func (q *IndexQueue) checkCompressionSettings() bool {
	ci, ok := q.index.(upgradableIndexer)
	if !ok {
		return false
	}

	shouldUpgrade, shouldUpgradeAt := ci.ShouldUpgrade()
	if !shouldUpgrade || ci.Upgraded() {
		return false
	}

	if q.index.AlreadyIndexed() > uint64(shouldUpgradeAt) {
		q.PauseIndexing()
		err := ci.Upgrade(q.ResumeIndexing)
		if err != nil {
			q.Logger.WithError(err).Error("failed to upgrade")
		}

		return true
	}

	return false
}

// pause indexing and wait for the workers to finish their current tasks
// related to this queue.
func (q *IndexQueue) PauseIndexing() {
	if !q.paused.CompareAndSwap(false, true) {
		q.Logger.Warn("attempted to pause indexing, but it is already paused")
		return
	}
	q.Logger.Debug("pausing indexing, waiting for the current tasks to finish")
	q.processingJobs.Wait()
	q.Logger.Debug("indexing paused")
	q.metrics.Paused()
}

// Waits for the workers to finish their current indexing tasks.
// It does not pause the queue.
// This method can potentially block for a long time
// if the queue is receiving vectors.
func (q *IndexQueue) Wait() {
	q.processingJobs.Wait()
}

// resume indexing
func (q *IndexQueue) ResumeIndexing() {
	if !q.paused.CompareAndSwap(true, false) {
		q.Logger.Warn("attempted to resume indexing, but it is already running")
		return
	}
	q.Logger.Debug("indexing resumed")
	q.metrics.Resumed()
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
		if q.index.DistancerProvider().Type() == "cosine-dot" {
			// cosine-dot requires normalized vectors, as the dot product and cosine
			// similarity are only identical if the vector is normalized
			v = distancer.Normalize(v)
		}

		dist, err := q.index.DistanceBetweenVectors(vector, v)
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
	}

	q.fullChunks.list = list.New()
	q.deleted.m = make(map[uint64]struct{})

	return &q
}

func (q *vectorQueue) getBuffer() []vectorDescriptor {
	return make([]vectorDescriptor, q.IndexQueue.BatchSize)
}

func (q *vectorQueue) getFreeChunk() *chunk {
	c := chunk{
		data: q.getBuffer(),
	}
	return &c
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
		for i := range full {
			q.fullChunks.list.PushBack(full[i])
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

func (q *vectorQueue) dequeue(max int) []*chunk {
	if max <= 0 {
		max = math.MaxInt64
	}

	q.fullChunks.Lock()
	var chunks []*chunk
	e := q.fullChunks.list.Front()
	count := 0
	for e != nil && count < max {
		next := e.Next()
		c := q.fullChunks.list.Remove(e).(*chunk)
		chunks = append(chunks, c)
		count++

		e = next
	}
	q.fullChunks.Unlock()

	if count < max {
		q.curBatch.Lock()
		if q.curBatch.c != nil && time.Since(*q.curBatch.c.createdAt) > q.IndexQueue.StaleTimeout && q.curBatch.c.cursor > 0 {
			q.IndexQueue.metrics.Stale()
			chunks = append(chunks, q.curBatch.c)
			q.curBatch.c = nil
		}
		q.curBatch.Unlock()
	}

	return chunks
}

// persistCheckpoint update the on-disk checkpoint that tracks the last indexed id
// optimistically. It is not guaranteed to be accurate but it is guaranteed to be lower
// than any vector in the queue.
// To calculate the checkpoint, we use the lowest id in the current batch
// minus the number of vectors in the queue (delta), which is capped at 10k vectors.
// The calculation looks like this:
// checkpoint = min(ids) - max(queueSize, 10_000)
func (q *vectorQueue) persistCheckpoint(minID uint64) {
	q.fullChunks.Lock()
	cl := q.fullChunks.list.Len()
	q.fullChunks.Unlock()

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

	err := q.IndexQueue.Checkpoints.UpdateIfNewer(q.IndexQueue.shardID, q.IndexQueue.targetVector, checkpoint)
	if err != nil {
		q.IndexQueue.Logger.WithError(err).Warn("checkpoint not updated")
	}
}

// Iterate through all chunks in the queue and call the given function.
// Deleted vectors are skipped, and if an allowlist is provided, only vectors
// in the allowlist are returned.
func (q *vectorQueue) Iterate(allowlist helpers.AllowList, fn func(objects []vectorDescriptor) error) error {
	buf := q.getBuffer()

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

func (q *vectorQueue) Delete(ids ...uint64) {
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

func (q *vectorQueue) ResetDeleted(ids ...uint64) {
	q.deleted.Lock()
	for _, id := range ids {
		delete(q.deleted.m, id)
	}
	q.deleted.Unlock()
}

func (q *vectorQueue) Reset() {
	q.fullChunks.Lock()
	q.fullChunks.list = list.New()
	q.fullChunks.Unlock()

	q.curBatch.Lock()
	if q.curBatch.c != nil {
		q.curBatch.c = nil
	}
	q.curBatch.Unlock()

	q.deleted.Lock()
	q.deleted.m = make(map[uint64]struct{})
	q.deleted.Unlock()
}

type chunk struct {
	cursor    int
	data      []vectorDescriptor
	createdAt *time.Time
}
