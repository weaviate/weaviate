//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestHFreshShutdownPreservesReplicas(t *testing.T) {
	cfg, uc := makeHFreshConfig(t)
	uc.MaxPostingSizeKB = 8
	uc.Distance = "l2-squared"
	cfg.DistanceProvider = distancer.NewL2SquaredProvider()
	cfg.Centroids.HNSWConfig.DistanceProvider = cfg.DistanceProvider
	t.Cleanup(func() { require.NoError(t, cfg.Scheduler.Close(context.Background())) })
	storeDir := t.TempDir()
	store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)

	splitStarted, reassignStarted := make(chan struct{}), make(chan struct{})
	splitRelease, reassignRelease := make(chan struct{}), make(chan struct{})
	var splitOnce, reassignOnce, releaseSplit, releaseReassign sync.Once
	unblockSplit := func() { releaseSplit.Do(func() { close(splitRelease) }) }
	unblockReassign := func() { releaseReassign.Do(func() { close(reassignRelease) }) }
	var armed atomic.Bool
	vectors := map[uint64][]float32{0: {0, 0, 0, 0}}
	cfg.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
		if armed.Load() {
			if id == 0 {
				reassignOnce.Do(func() { close(reassignStarted); <-reassignRelease })
			} else {
				splitOnce.Do(func() { close(splitStarted); <-splitRelease })
			}
		}
		return vectors[id], nil
	}
	h := makeHFreshWithConfig(t, store, cfg, uc)
	t.Cleanup(func() {
		unblockSplit()
		unblockReassign()
		h.Shutdown(context.Background())
		store.Shutdown(context.Background())
	})
	for _, q := range []*queue.DiskQueue{h.taskQueue.analyzeQueue.DiskQueue, h.taskQueue.splitQueue, h.taskQueue.reassignQueue, h.taskQueue.mergeQueue} {
		require.NoError(t, q.Pause(t.Context()))
	}
	require.NoError(t, h.Add(t.Context(), 0, vectors[0]))
	var oldID uint64
	for id := range h.PostingMap.Iter() {
		oldID = id
	}
	require.NotZero(t, oldID)
	// Four live replicas, after the old posting was retired by maintenance.
	for _, center := range [][]float32{{1, 0, 0, 0}, {-1, 0, 0, 0}, {0, 1, 0, 0}, {0, -1, 0, 0}} {
		id, err := h.IDs.Next()
		require.NoError(t, err)
		require.NoError(t, h.Centroids.Insert(id, &Centroid{Uncompressed: center}))
		p := Posting{NewVector(0, VectorVersion(1), h.quantizer.CompressedBytes(h.quantizer.Encode(vectors[0])))}
		require.NoError(t, h.PostingStore.Put(t.Context(), id, p))
		require.NoError(t, h.setPostingVectorIDs(t.Context(), id, p))
	}
	require.NoError(t, h.Centroids.MarkAsDeleted(oldID))
	require.NoError(t, h.PostingStore.Put(t.Context(), oldID, Posting{}))
	require.NoError(t, h.setPostingVectorIDs(t.Context(), oldID, Posting{}))

	// An oversized, unrelated posting keeps the split worker busy. Its children
	// also need splits, exercising the split -> split handoff on shutdown.
	splitID, err := h.IDs.Next()
	require.NoError(t, err)
	center := []float32{100, 100, 0, 0}
	require.NoError(t, h.Centroids.Insert(splitID, &Centroid{Uncompressed: center}))
	p := make(Posting, 3*int(h.maxPostingSize))
	for i := range p {
		v := []float32{100, 100, 0, 0}
		v[i%2] += float32(i%17) / 10
		id := uint64(i + 1)
		vectors[id] = v
		p[i] = NewVector(id, VectorVersion(1), h.quantizer.CompressedBytes(h.quantizer.Encode(v)))
	}
	require.NoError(t, h.PostingStore.Put(t.Context(), splitID, p))
	require.NoError(t, h.setPostingVectorIDs(t.Context(), splitID, p))
	h.taskQueue.analyzeQueue.Resume()
	require.NoError(t, h.taskQueue.Flush())
	h.scheduler.Schedule(t.Context())
	require.NoError(t, h.taskQueue.analyzeQueue.Wait(t.Context()))
	require.Equal(t, 4, liveShutdownReplicas(t, h, 0))

	armed.Store(true)
	require.NoError(t, h.taskQueue.EnqueueSplit(splitID))
	require.NoError(t, h.taskQueue.EnqueueReassign(oldID, 0))
	require.NoError(t, h.taskQueue.Flush())
	h.taskQueue.splitQueue.Resume()
	h.taskQueue.reassignQueue.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, started := range []chan struct{}{splitStarted, reassignStarted} {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("maintenance worker did not start")
		}
	}
	done := make(chan error, 1)
	registered := h.scheduler.QueueCount()
	enterrors.GoWrapper(func() { done <- h.Shutdown(ctx) }, cfg.Logger)
	require.Eventually(t, func() bool {
		return h.scheduler.IsQueuePaused(h.taskQueue.analyzeQueue.ID()) || h.scheduler.QueueCount() < registered
	}, time.Second, time.Millisecond)
	unblockReassign()
	require.NoError(t, h.taskQueue.reassignQueue.Wait(ctx))
	unblockSplit()
	require.NoError(t, <-done)
	armed.Store(false)
	require.NoError(t, store.Shutdown(t.Context()))
	store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)
	h = makeHFreshWithConfig(t, store, cfg, uc)
	h.waitForMaintenance(t)
	// Search can enqueue merges; keep the reopened snapshot stable while
	// observing versions and postings separately.
	for _, q := range []*queue.DiskQueue{h.taskQueue.analyzeQueue.DiskQueue, h.taskQueue.splitQueue, h.taskQueue.reassignQueue, h.taskQueue.mergeQueue} {
		require.NoError(t, q.Pause(t.Context()))
	}
	found, _, err := h.SearchByVector(t.Context(), vectors[0], 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, found, "shutdown must preserve searchability after reopen")
	require.Equal(t, 4, liveShutdownReplicas(t, h, 0), "shutdown must preserve all live replicas after reopen")
	for id := range vectors {
		require.Positive(t, liveShutdownReplicas(t, h, id), "shutdown must preserve the vectors of an in-flight split")
	}
}

func liveShutdownReplicas(t *testing.T, h *HFresh, id uint64) int {
	t.Helper()
	version, err := h.VersionMap.Get(t.Context(), id)
	require.NoError(t, err)
	count := 0
	for postingID := range h.PostingMap.Iter() {
		p, err := h.PostingStore.Get(t.Context(), postingID)
		require.NoError(t, err)
		for _, v := range p {
			if v.ID() == id && v.Version() == version {
				count++
			}
		}
	}
	return count
}

// A deadline is not permission to tear down resources still held by a worker.
func TestHFreshShutdownDeadlineCanBeRetried(t *testing.T) {
	for _, drop := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("drop=%t/deadline=%t", drop, deadline), func(t *testing.T) {
				cfg, uc := makeHFreshConfig(t)
				t.Cleanup(func() { cfg.Scheduler.Close(context.Background()) })
				var centroidClosed atomic.Bool
				original := cfg.Centroids.HNSWConfig.MakeCommitLoggerThunk
				cfg.Centroids.HNSWConfig.MakeCommitLoggerThunk = func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
					l, err := original(opts...)
					return &shutdownCommitLogger{CommitLogger: l, closed: &centroidClosed}, err
				}
				store := testinghelpers.NewDummyStore(t)
				h := makeHFreshWithConfig(t, store, cfg, uc)
				ids := NewBucketStore(h.PostingMap.bucket.bucket)
				_, err := h.IDs.Next()
				require.NoError(t, err)
				reserved, err := ids.Load()
				require.NoError(t, err)

				started, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(func() { unblock(); h.Shutdown(context.Background()); store.Shutdown(context.Background()) })
				q, err := queue.NewDiskQueue(queue.DiskQueueOptions{
					ID: h.taskQueue.splitQueue.ID(), Dir: t.TempDir(), Logger: cfg.Logger, Scheduler: cfg.Scheduler,
					TaskDecoder: shutdownTaskDecoder{execute: func(ctx context.Context) error { close(started); <-release; return ctx.Err() }},
				})
				require.NoError(t, err)
				require.NoError(t, q.Init())
				require.NoError(t, h.taskQueue.splitQueue.Close(t.Context()))
				h.taskQueue.splitQueue = q
				cfg.Scheduler.RegisterQueue(q)
				require.NoError(t, q.Push([]byte{1}))
				require.NoError(t, q.Flush())
				<-started
				ctx, cancel := context.WithCancel(context.Background())
				want := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				defer cancel()
				closeIndex := h.Shutdown
				if drop {
					closeIndex = h.Drop
				}
				err = closeIndex(ctx)
				require.ErrorIs(t, err, want, "shutdown must report an undrained worker")
				require.False(t, centroidClosed.Load(), "shutdown must keep centroids open while workers run")
				retained, err := ids.Load()
				require.NoError(t, err)
				require.Equal(t, reserved, retained, "shutdown must not flush the ID sequence while workers run")
				retryEntered := make(chan struct{})
				cancelIndex := h.cancel
				h.cancel = func() { cancelIndex(); close(retryEntered) }
				retried := make(chan error, 1)
				enterrors.GoWrapper(func() { retried <- closeIndex(context.Background()) }, cfg.Logger)
				t.Cleanup(func() {
					unblock()
					require.NoError(t, <-retried, "shutdown must allow retrying the drain")
					require.True(t, centroidClosed.Load())
				})
				<-retryEntered
				// A second call must not wait behind a worker held by the first drain.
				repeated := make(chan error, 2)
				enterrors.GoWrapper(func() { repeated <- h.Shutdown(ctx) }, cfg.Logger)
				enterrors.GoWrapper(func() { repeated <- h.Drop(ctx) }, cfg.Logger)
				for range 2 {
					select {
					case err := <-repeated:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(time.Second):
						t.Fatal("concurrent teardown must not wait behind an active drain")
					}
				}
			})
		}
	}
}

type shutdownCommitLogger struct {
	hnsw.CommitLogger
	closed *atomic.Bool
}

func (l *shutdownCommitLogger) Shutdown(ctx context.Context) error {
	l.closed.Store(true)
	return l.CommitLogger.Shutdown(ctx)
}

func TestHFreshShutdownControls(t *testing.T) {
	for _, inserted := range []bool{false, true} {
		t.Run(fmt.Sprint("inserted=", inserted), func(t *testing.T) {
			cfg, uc := makeHFreshConfig(t)
			t.Cleanup(func() { cfg.Scheduler.Close(context.Background()) })
			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(context.Background())
			h := makeHFreshWithConfig(t, store, cfg, uc)
			if inserted {
				require.NoError(t, h.Add(t.Context(), 0, []float32{1, 0, 0, 0}))
				h.waitForMaintenance(t)
			}
			require.NoError(t, h.Shutdown(context.Background()))
			require.ErrorIs(t, h.Shutdown(context.Background()), context.Canceled)
			require.NoError(t, h.Drop(context.Background()))
			require.NoError(t, h.Drop(context.Background()))
		})
	}
	h := &HFresh{}
	require.NoError(t, h.Shutdown(context.Background()))
	require.NoError(t, h.Drop(context.Background()))
}

func TestHFreshRetriesInterruptedCentroidShutdown(t *testing.T) {
	for _, commitlog := range []bool{false, true} {
		for _, drop := range []bool{false, true} {
			t.Run(fmt.Sprintf("commitlog=%t/drop=%t", commitlog, drop), func(t *testing.T) {
				cfg, uc := makeHFreshConfig(t)
				defer cfg.Scheduler.Close(context.Background())
				started, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				callbacks := &heldShutdownCallbacks{
					CycleCallbackGroup: cyclemanager.NewCallbackGroup("held-centroid", cfg.Logger, 1),
					started:            started, release: release,
				}
				if commitlog {
					cfg.Centroids.HNSWConfig.MakeCommitLoggerThunk = func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
						return hnsw.NewCommitLogger(cfg.RootPath, "centroids", cfg.Logger, callbacks, opts...)
					}
				} else {
					cfg.TombstoneCallbacks = callbacks
				}
				store := testinghelpers.NewDummyStore(t)
				createObjectsBucket(t, store)
				h, err := New(cfg, uc, store)
				require.NoError(t, err)
				done := make(chan struct{})
				enterrors.GoWrapper(func() { callbacks.CycleCallback(func() bool { return false }); close(done) }, cfg.Logger)
				defer func() { unblock(); <-done; h.Shutdown(context.Background()); store.Shutdown(context.Background()) }()
				<-started
				closeIndex := h.Shutdown
				if drop {
					closeIndex = h.Drop
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				err = closeIndex(ctx)
				require.ErrorIs(t, err, context.DeadlineExceeded, "centroid drain interruption must be reported")
				ctx, cancelAgain := context.WithCancel(context.Background())
				cancelAgain()
				require.ErrorIs(t, h.Drop(ctx), context.Canceled, "drop must retain an interrupted centroid drain")
				unblock()
				<-done
				require.NoError(t, closeIndex(context.Background()), "centroid shutdown must be retryable after its callback finishes")
				require.NoError(t, h.Drop(context.Background()))
			})
		}
	}
}

type heldShutdownCallbacks struct {
	cyclemanager.CycleCallbackGroup
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *heldShutdownCallbacks) Register(id string, _ cyclemanager.CycleCallback, opts ...cyclemanager.RegisterOption) cyclemanager.CycleCallbackCtrl {
	return g.CycleCallbackGroup.Register(id, func(cyclemanager.ShouldAbortCallback) bool {
		g.once.Do(func() { close(g.started); <-g.release })
		return false
	}, opts...)
}
