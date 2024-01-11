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
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func startWorker(t testing.TB, retryInterval ...time.Duration) chan job {
	t.Helper()
	ch := make(chan job)
	t.Cleanup(func() {
		close(ch)
	})

	itv := time.Millisecond
	if len(retryInterval) > 0 {
		itv = retryInterval[0]
	}

	go func() {
		logger := logrus.New()
		logger.Level = logrus.ErrorLevel
		asyncWorker(ch, logger, itv)
	}()

	return ch
}

func newCheckpointManager(t testing.TB) *indexcheckpoint.Checkpoints {
	t.Helper()

	return newCheckpointManagerWithDir(t, t.TempDir())
}

func newCheckpointManagerWithDir(t testing.TB, dir string) *indexcheckpoint.Checkpoints {
	t.Helper()

	c, err := indexcheckpoint.New(dir, logrus.New())
	require.NoError(t, err)

	return c
}

func pushVector(t testing.TB, ctx context.Context, q *IndexQueue, id uint64, vector []float32) {
	err := q.Push(ctx, vectorDescriptor{
		id:     id,
		vector: vector,
	})
	require.NoError(t, err)
}

func randVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	return vec
}

func TestIndexQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	os.Setenv("ASYNC_INDEXING", "true")
	defer os.Unsetenv("ASYNC_INDEXING")

	writeIDs := func(q *IndexQueue, from, to uint64) {
		vectors := make([]vectorDescriptor, 0, to-from)
		for i := from; i < to; i++ {
			vectors = append(vectors, vectorDescriptor{
				id:     i,
				vector: []float32{1, 2, 3},
			})
		}
		err := q.Push(ctx, vectors...)
		require.NoError(t, err)
	}

	getLastUpdate := func(q *IndexQueue) time.Time {
		fi, err := os.Stat(q.checkpoints.Filename())
		require.NoError(t, err)
		return fi.ModTime()
	}

	waitForUpdate := func(q *IndexQueue) func(timeout ...time.Duration) bool {
		lastUpdate := getLastUpdate(q)

		return func(timeout ...time.Duration) bool {
			start := time.Now()

			if len(timeout) == 0 {
				timeout = []time.Duration{500 * time.Millisecond}
			}
			for {
				cur := getLastUpdate(q)
				if cur.Equal(lastUpdate) {
					if time.Since(start) > timeout[0] {
						return false
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}

				lastUpdate = cur
				return true
			}
		}
	}

	t.Run("pushes to indexer if batch is full", func(t *testing.T) {
		var idx mockBatchIndexer
		idsCh := make(chan []uint64, 1)
		idx.addBatchFn = func(ids []uint64, vector [][]float32) error {
			idsCh <- ids
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize: 2,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		select {
		case <-idsCh:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}

		pushVector(t, ctx, q, 2, []float32{4, 5, 6})
		ids := <-idsCh

		require.Equal(t, []uint64{1, 2}, ids)
	})

	t.Run("doesn't index if batch is not null", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			called <- struct{}{}
			return nil
		}
		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     100,
			IndexInterval: time.Microsecond,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		select {
		case <-called:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}

		pushVector(t, ctx, q, 2, []float32{4, 5, 6})

		select {
		case <-called:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("retry on indexing error", func(t *testing.T) {
		var idx mockBatchIndexer
		i := int32(0)
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			if atomic.AddInt32(&i, 1) < 3 {
				return fmt.Errorf("indexing error: %d", i)
			}

			close(called)

			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize: 1,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		<-called
	})

	t.Run("merges results from queries", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(called)
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     3,
			IndexInterval: 100 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		pushVector(t, ctx, q, 2, []float32{4, 5, 6})
		pushVector(t, ctx, q, 3, []float32{7, 8, 9})
		pushVector(t, ctx, q, 4, []float32{1, 2, 3})

		<-called

		time.Sleep(500 * time.Millisecond)
		res, _, err := q.SearchByVector([]float32{1, 2, 3}, 2, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 4}, res)
	})

	t.Run("search with empty index", func(t *testing.T) {
		var idx mockBatchIndexer

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize: 6,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := 0; i < 10; i++ {
			pushVector(t, ctx, q, uint64(i+1), []float32{float32(i) + 1, float32(i) + 2, float32(i) + 3})
		}

		res, _, err := q.SearchByVector([]float32{1, 2, 3}, 2, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, res)
	})

	t.Run("queue size", func(t *testing.T) {
		var idx mockBatchIndexer
		closeCh := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			<-closeCh
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize: 5,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 101; i++ {
			pushVector(t, ctx, q, i+1, []float32{1, 2, 3})
		}

		time.Sleep(100 * time.Millisecond)
		require.EqualValues(t, 101, q.Size())
		close(closeCh)
	})

	t.Run("deletion", func(t *testing.T) {
		var idx mockBatchIndexer
		var count int32
		indexingDone := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			if atomic.AddInt32(&count, 1) == 5 {
				close(indexingDone)
			}

			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     4,
			IndexInterval: 100 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 20; i++ {
			pushVector(t, ctx, q, i, []float32{1, 2, 3})
		}

		err = q.Delete(5, 10, 15)
		require.NoError(t, err)

		wait := waitForUpdate(q)
		<-indexingDone

		// wait for the checkpoint file to be written to disk
		wait()

		// check what has been indexed
		require.Equal(t, []uint64{0, 1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19}, idx.IDs())

		// the "deleted" mask should be empty
		q.queue.deleted.Lock()
		require.Empty(t, q.queue.deleted.m)
		q.queue.deleted.Unlock()

		// now delete something that's already indexed
		err = q.Delete(0, 4, 8)
		require.NoError(t, err)

		// the "deleted" mask should still be empty
		q.queue.deleted.Lock()
		require.Empty(t, q.queue.deleted.m)
		q.queue.deleted.Unlock()

		// check what's in the index
		require.Equal(t, []uint64{1, 2, 3, 6, 7, 9, 11, 12, 13, 14, 16, 17, 18, 19}, idx.IDs())

		// delete something that's not indexed yet
		err = q.Delete(20, 21, 22)
		require.NoError(t, err)

		// the "deleted" mask should contain the deleted ids
		q.queue.deleted.Lock()
		var ids []int
		for id := range q.queue.deleted.m {
			ids = append(ids, int(id))
		}
		q.queue.deleted.Unlock()
		sort.Ints(ids)
		require.Equal(t, []int{20, 21, 22}, ids)
	})

	t.Run("brute force upper limit", func(t *testing.T) {
		var idx mockBatchIndexer

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:             1000,
			BruteForceSearchLimit: 2,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		pushVector(t, ctx, q, 2, []float32{4, 5, 6})
		pushVector(t, ctx, q, 3, []float32{7, 8, 9})
		pushVector(t, ctx, q, 4, []float32{1, 2, 3})

		res, _, err := q.SearchByVector([]float32{7, 8, 9}, 2, nil)
		require.NoError(t, err)
		// despite having 4 vectors in the queue
		// only the first two are used for brute force search
		require.Equal(t, []uint64{2, 1}, res)
	})

	t.Run("stores a safe checkpoint", func(t *testing.T) {
		var idx mockBatchIndexer

		dir := t.TempDir()
		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManagerWithDir(t, dir), IndexQueueOptions{
			BatchSize:     5,
			IndexInterval: time.Hour,
		})
		require.NoError(t, err)
		defer q.Close()

		wait := waitForUpdate(q)
		writeIDs(q, 5, 7)  // [5, 6]
		writeIDs(q, 9, 13) // [5, 6, 9, 10, 11], [12]
		writeIDs(q, 0, 5)  // [5, 6, 9, 10, 11], [12, 0, 1, 2, 3], [4]
		time.Sleep(100 * time.Millisecond)
		before, err := q.checkpoints.Get("1")
		require.NoError(t, err)
		q.pushToWorkers(-1, false)
		// the checkpoint should be: 0, then 0
		// the cursor should not be updated
		wait(100 * time.Millisecond)
		after, err := q.checkpoints.Get("1")
		require.NoError(t, err)
		require.Equal(t, before, after)

		writeIDs(q, 15, 25) // [4, 15, 16, 17, 18], [19, 20, 21, 22, 23], [24]
		writeIDs(q, 30, 40) // [4, 15, 16, 17, 18], [19, 20, 21, 22, 23], [24, 30, 31, 32, 33], [34, 35, 36, 37, 38], [39]
		time.Sleep(100 * time.Millisecond)
		// the checkpoint should be: 0, then 4, then 14, then 29
		q.pushToWorkers(-1, false)
		// 0
		wait()
		// 4
		wait()
		// 14
		wait()
		// 29
		wait()
		v, err := q.checkpoints.Get("1")
		require.NoError(t, err)
		require.Equal(t, 29, int(v))
	})

	t.Run("stale vectors", func(t *testing.T) {
		var idx mockBatchIndexer
		closeCh := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(closeCh)
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     5,
			StaleTimeout:  100 * time.Millisecond,
			IndexInterval: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 3; i++ {
			pushVector(t, ctx, q, i+1, []float32{1, 2, 3})
		}

		select {
		case <-closeCh:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("should have been indexed after 100ms")
		}

		require.EqualValues(t, []uint64{1, 2, 3}, idx.IDs())
	})

	t.Run("updates the shard state", func(t *testing.T) {
		var idx mockBatchIndexer
		indexed := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(indexed)
			return nil
		}

		updated := make(chan string)
		shard := mockShard{
			compareAndSwapStatusFn: func(old, new string) (storagestate.Status, error) {
				updated <- new
				return storagestate.Status(new), nil
			},
		}

		q, err := NewIndexQueue("1", &shard, &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     2,
			IndexInterval: 100 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 2; i++ {
			pushVector(t, ctx, q, i+1, []float32{1, 2, 3})
		}

		select {
		case newState := <-updated:
			require.Equal(t, storagestate.StatusIndexing.String(), newState)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("shard state should have been updated after 100ms")
		}

		select {
		case <-indexed:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("should have been indexed after 100ms")
		}

		select {
		case newState := <-updated:
			require.Equal(t, storagestate.StatusReady.String(), newState)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("shard state should have been updated after 100ms")
		}
	})

	t.Run("close waits for indexing to be done", func(t *testing.T) {
		var idx mockBatchIndexer
		var count int
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			<-time.After(10 * time.Millisecond)
			count++
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize: 5,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 100; i++ {
			pushVector(t, ctx, q, i+1, []float32{1, 2, 3})
		}

		q.pushToWorkers(-1, false)
		q.Close()

		require.EqualValues(t, 20, count)
	})

	t.Run("cos: normalized the query vector", func(t *testing.T) {
		var idx mockBatchIndexer
		idx.distancerProvider = distancer.NewCosineDistanceProvider()

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     7, // 300 is not divisible by 7
			IndexInterval: 100 * time.Second,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 300; i++ {
			pushVector(t, ctx, q, i+1, randVector(1536))
		}

		q.pushToWorkers(-1, false)

		_, distances, err := q.SearchByVector(randVector(1536), 10, nil)
		require.NoError(t, err)

		// all distances should be between 0 and 1
		for _, dist := range distances {
			require.True(t, dist >= 0 && dist <= 1)
		}
	})

	t.Run("pause/resume indexing", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			called <- struct{}{}
			// simulate work
			<-time.After(100 * time.Millisecond)
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     2,
			IndexInterval: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		pushVector(t, ctx, q, 2, []float32{4, 5, 6})

		// batch indexed
		<-called

		// pause indexing: this will block until the batch is indexed
		q.pauseIndexing()

		// add more vectors
		pushVector(t, ctx, q, 3, []float32{7, 8, 9})
		pushVector(t, ctx, q, 4, []float32{1, 2, 3})

		// wait enough time to make sure the indexing is not happening
		<-time.After(200 * time.Millisecond)

		select {
		case <-called:
			t.Fatal("should not have been called")
		default:
		}

		// resume indexing
		q.resumeIndexing()

		// wait for the indexing to be done
		<-called
	})

	t.Run("compression", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.shouldCompress = true
		idx.threshold = 4
		idx.alreadyIndexed = 6

		release := make(chan struct{})
		idx.onCompressionTurnedOn = func(callback func()) error {
			go func() {
				<-release
				callback()
			}()

			close(called)
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     2,
			IndexInterval: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, []float32{1, 2, 3})
		pushVector(t, ctx, q, 2, []float32{4, 5, 6})

		// compression requested
		<-called

		// indexing should be paused
		require.True(t, q.paused.Load())

		// release the compression
		idx.compressed = true
		close(release)

		// indexing should be resumed eventually
		time.Sleep(100 * time.Millisecond)
		require.False(t, q.paused.Load())

		indexed := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(indexed)
			return nil
		}

		// add more vectors
		pushVector(t, ctx, q, 3, []float32{7, 8, 9})
		pushVector(t, ctx, q, 4, []float32{1, 2, 3})

		// indexing should happen
		<-indexed
	})

	t.Run("compression does not occur at the indexing if async is enabled", func(t *testing.T) {
		vectors := [][]float32{{0, 1, 3, 4, 5, 6}, {0, 1, 3, 4, 5, 6}, {0, 1, 3, 4, 5, 6}}
		distancer := distancer.NewL2SquaredProvider()
		uc := ent.UserConfig{}
		uc.MaxConnections = 112
		uc.EFConstruction = 112
		uc.EF = 10
		uc.VectorCacheMaxObjects = 10e12
		index, _ := hnsw.New(
			hnsw.Config{
				RootPath:              t.TempDir(),
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
					copy(container.Slice, vectors[int(id)])
					return container.Slice, nil
				},
			}, uc,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), newDummyStore(t))
		defer index.Shutdown(context.Background())

		q, err := NewIndexQueue("1", new(mockShard), index, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     2,
			IndexInterval: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		uc.PQ = ent.PQConfig{Enabled: true, Encoder: ent.PQEncoder{Type: "please break...", Distribution: "normal"}}
		err = index.UpdateUserConfig(uc, func() {})
		require.Nil(t, err)
	})

	t.Run("sending batch with deleted ids to worker", func(t *testing.T) {
		var idx mockBatchIndexer
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			t.Fatal("should not have been called")
			return nil
		}

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     2,
			IndexInterval: 100 * time.Second,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 0, []float32{1, 2, 3})
		pushVector(t, ctx, q, 1, []float32{1, 2, 3})

		err = q.Delete(0, 1)
		require.NoError(t, err)

		q.pushToWorkers(-1, true)
	})

	t.Run("release twice", func(t *testing.T) {
		var idx mockBatchIndexer

		q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(t), newCheckpointManager(t), IndexQueueOptions{
			BatchSize:     10,
			IndexInterval: time.Hour, // do not index automatically
		})
		require.NoError(t, err)

		for i := uint64(0); i < 35; i++ {
			pushVector(t, ctx, q, i+1, []float32{1, 2, 3})
		}

		chunks := q.queue.borrowChunks(10)
		require.Equal(t, 3, len(chunks))

		// release once
		for _, chunk := range chunks {
			q.queue.releaseChunk(chunk)
		}

		// release again
		for _, chunk := range chunks {
			q.queue.releaseChunk(chunk)
		}
	})
}

func BenchmarkPush(b *testing.B) {
	var idx mockBatchIndexer

	idx.addBatchFn = func(id []uint64, vector [][]float32) error {
		time.Sleep(1 * time.Second)
		return nil
	}

	q, err := NewIndexQueue("1", new(mockShard), &idx, startWorker(b), newCheckpointManager(b), IndexQueueOptions{
		BatchSize:     1000,
		IndexInterval: 1 * time.Millisecond,
	})
	require.NoError(b, err)
	defer q.Close()

	vecs := make([]vectorDescriptor, 100)
	for j := range vecs {
		vecs[j] = vectorDescriptor{
			id:     uint64(j),
			vector: []float32{1, 2, 3},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			err = q.Push(context.Background(), vecs...)
			require.NoError(b, err)
		}
	}
}

type mockShard struct {
	compareAndSwapStatusFn func(old, new string) (storagestate.Status, error)
}

func (m *mockShard) compareAndSwapStatus(old, new string) (storagestate.Status, error) {
	if m.compareAndSwapStatusFn == nil {
		return storagestate.Status(new), nil
	}

	return m.compareAndSwapStatusFn(old, new)
}

type mockBatchIndexer struct {
	sync.Mutex
	addBatchFn            func(id []uint64, vector [][]float32) error
	vectors               map[uint64][]float32
	containsNodeFn        func(id uint64) bool
	deleteFn              func(ids ...uint64) error
	distancerProvider     distancer.Provider
	shouldCompress        bool
	threshold             int
	compressed            bool
	alreadyIndexed        uint64
	onCompressionTurnedOn func(func()) error
}

func (m *mockBatchIndexer) AddBatch(ctx context.Context, ids []uint64, vector [][]float32) (err error) {
	m.Lock()
	defer m.Unlock()

	if m.addBatchFn != nil {
		err = m.addBatchFn(ids, vector)
	}

	if m.vectors == nil {
		m.vectors = make(map[uint64][]float32)
	}

	for i, id := range ids {
		m.vectors[id] = vector[i]
	}

	return
}

func (m *mockBatchIndexer) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	m.Lock()
	defer m.Unlock()

	results := newPqMaxPool(k).GetMax(k)

	if m.DistancerProvider().Type() == "cosine-dot" {
		vector = distancer.Normalize(vector)
	}

	for id, v := range m.vectors {
		// skip filtered data
		if allowList != nil && allowList.Contains(id) {
			continue
		}

		if m.DistancerProvider().Type() == "cosine-dot" {
			v = distancer.Normalize(v)
		}

		dist, _, err := m.DistanceBetweenVectors(vector, v)
		if err != nil {
			return nil, nil, err
		}

		if results.Len() < k || dist < results.Top().Dist {
			results.Insert(id, dist)
			for results.Len() > k {
				results.Pop()
			}
		}
	}
	var ids []uint64
	var distances []float32

	for i := k - 1; i >= 0; i-- {
		if results.Len() == 0 {
			break
		}
		element := results.Pop()
		ids = append(ids, element.ID)
		distances = append(distances, element.Dist)
	}

	return ids, distances, nil
}

func (m *mockBatchIndexer) SearchByVectorDistance(vector []float32, maxDistance float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	m.Lock()
	defer m.Unlock()

	results := newPqMaxPool(int(maxLimit)).GetMax(int(maxLimit))

	if m.DistancerProvider().Type() == "cosine-dot" {
		vector = distancer.Normalize(vector)
	}

	for id, v := range m.vectors {
		// skip filtered data
		if allowList != nil && allowList.Contains(id) {
			continue
		}

		if m.DistancerProvider().Type() == "cosine-dot" {
			v = distancer.Normalize(v)
		}

		dist, _, err := m.DistanceBetweenVectors(vector, v)
		if err != nil {
			return nil, nil, err
		}

		if dist > maxDistance {
			continue
		}

		if results.Len() < int(maxLimit) || dist < results.Top().Dist {
			results.Insert(id, dist)
			for results.Len() > int(maxLimit) {
				results.Pop()
			}
		}
	}
	var ids []uint64
	var distances []float32

	for i := maxLimit - 1; i >= 0; i-- {
		if results.Len() == 0 {
			break
		}
		element := results.Pop()
		ids = append(ids, element.ID)
		distances = append(distances, element.Dist)
	}

	return ids, distances, nil
}

func (m *mockBatchIndexer) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	res := float32(0)
	for i := range x {
		diff := x[i] - y[i]
		res += diff * diff
	}
	return res, true, nil
}

func (m *mockBatchIndexer) ContainsNode(id uint64) bool {
	m.Lock()
	defer m.Unlock()
	if m.containsNodeFn != nil {
		return m.containsNodeFn(id)
	}

	_, ok := m.vectors[id]
	return ok
}

func (m *mockBatchIndexer) Delete(ids ...uint64) error {
	m.Lock()
	defer m.Unlock()
	if m.deleteFn != nil {
		return m.deleteFn(ids...)
	}

	for _, id := range ids {
		delete(m.vectors, id)
	}

	return nil
}

func (m *mockBatchIndexer) IDs() []uint64 {
	m.Lock()
	defer m.Unlock()

	ids := make([]uint64, 0, len(m.vectors))
	for id := range m.vectors {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids
}

func (m *mockBatchIndexer) DistancerProvider() distancer.Provider {
	if m.distancerProvider == nil {
		return distancer.NewL2SquaredProvider()
	}

	return m.distancerProvider
}

func (m *mockBatchIndexer) ShouldCompress() (bool, int) {
	return m.shouldCompress, m.threshold
}

func (m *mockBatchIndexer) Compressed() bool {
	return m.compressed
}

func (m *mockBatchIndexer) AlreadyIndexed() uint64 {
	return m.alreadyIndexed
}

func (m *mockBatchIndexer) TurnOnCompression(callback func()) error {
	if m.onCompressionTurnedOn != nil {
		return m.onCompressionTurnedOn(callback)
	}

	return nil
}

func newDummyStore(t *testing.T) *lsmkv.Store {
	logger, _ := test.NewNullLogger()
	storeDir := t.TempDir()
	store, err := lsmkv.New(storeDir, storeDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	return store
}
