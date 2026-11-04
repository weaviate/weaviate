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

package hnsw

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type bootstrapFixture struct {
	index   *hnsw
	vectors [][]float32
	gone    sync.Map
	ctx     context.Context
	cfg     Config
	uc      ent.UserConfig
	store   *lsmkv.Store
	logs    *test.Hook
	// onAddNode runs when a node is logged, i.e. the moment it joins the graph
	onAddNode func(id uint64)
}

// hookedCommitLogger reports every node that joins the graph
type hookedCommitLogger struct {
	CommitLogger
	onAddNode func(id uint64)
}

func (l *hookedCommitLogger) AddNode(id uint64, level uint16) error {
	l.onAddNode(id)
	return l.CommitLogger.AddNode(id, level)
}

func newBootstrapFixture(t *testing.T, compressed bool) *bootstrapFixture {
	ctx := context.Background()
	f := &bootstrapFixture{ctx: ctx}
	f.vectors, _ = testinghelpers.RandomVecsFixedSeed(8, 1, 16)
	logger, logs := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	f.logs = logs
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })
	dir := t.TempDir()
	indexID := "entrypoint-bootstrap"

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.VectorCacheMaxObjects = 100000
	if compressed {
		uc.RQ = ent.RQConfig{Enabled: true, Bits: 8}
	} else {
		uc.RQ.Enabled = false
		uc.SkipDefaultQuantization = true
	}

	f.cfg = Config{
		RootPath: dir,
		ID:       indexID,
		Logger:   logger,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			commitLog, err := NewCommitLogger(dir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
			if err != nil {
				return nil, err
			}
			return &hookedCommitLogger{CommitLogger: commitLog, onAddNode: func(id uint64) {
				if f.onAddNode != nil {
					f.onAddNode(id)
				}
			}}, nil
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if _, gone := f.gone.Load(id); gone {
				return nil, storobj.NewErrNotFoundf(id, "deleted from object store")
			}
			return f.vectors[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(f.vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
	}
	f.uc = uc
	f.store = store
	f.open(t)

	for i := range 3 {
		require.NoError(t, f.index.Add(ctx, uint64(i), f.vectors[i]))
	}
	require.Equal(t, compressed, f.index.Compressed())
	return f
}

func (f *bootstrapFixture) open(t *testing.T) {
	index, err := New(f.cfg, f.uc, cyclemanager.NewCallbackGroupNoop(), f.store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(f.ctx) })
	index.PostStartup(f.ctx)
	index.randFunc = func() float64 { return 1 }
	f.index = index
}

func (f *bootstrapFixture) restart(t *testing.T) {
	require.NoError(t, f.index.Flush())
	require.NoError(t, f.index.Shutdown(f.ctx))
	f.open(t)
}

func (f *bootstrapFixture) deleteFromObjectStore(id uint64) {
	f.gone.Store(id, struct{}{})
	if f.index.Compressed() {
		f.index.compressor.Delete(f.ctx, id)
	} else {
		f.index.cache.Delete(f.ctx, id)
	}
}

// leaveGhost leaves a node without links in the graph, like an insert that
// failed after joining it, or one from a version that joined before picking an
// entrypoint
func (f *bootstrapFixture) leaveGhost(t *testing.T, id uint64) {
	node := &vertex{level: 0}
	node.connections.GrowLayersTo(0)
	require.NoError(t, f.index.publishNode(id, node, f.vectors[id]))
	require.NotNil(t, f.index.nodeByID(id))
	require.False(t, f.index.hasTombstone(id))
}

func (f *bootstrapFixture) nonEntrypointNodes() []uint64 {
	ep := f.index.getEntrypoint()
	var ids []uint64
	for i := range uint64(3) {
		if i != ep {
			ids = append(ids, i)
		}
	}
	return ids
}

func (f *bootstrapFixture) requireReachable(t *testing.T, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		found, _, err := f.index.SearchByVector(ctx, f.vectors[id], len(f.vectors), nil)
		cancel()
		require.NoError(t, err)
		require.ElementsMatch(t, ids, found, "search from %d", id)
	}
}

func (f *bootstrapFixture) startAdd(ctx context.Context, id uint64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.index.Add(ctx, id, f.vectors[id]) }()
	return done
}

// waits counts how often an insert of id started waiting for an entrypoint
func (f *bootstrapFixture) waits(id uint64) int {
	n := 0
	for _, e := range f.logs.AllEntries() {
		if e.Data["action"] == "hnsw_insert" && e.Data["id"] == id {
			n++
		}
	}
	return n
}

// waitUntilWaiting blocks until the insert of id started waiting for the n-th time
func (f *bootstrapFixture) waitUntilWaiting(t *testing.T, id uint64, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return f.waits(id) >= n },
		10*time.Second, time.Millisecond, "insert of %d should be waiting for an entrypoint", id)
}

// requireNotJoined checks that waiting inserts are not part of the graph
func (f *bootstrapFixture) requireNotJoined(t *testing.T, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		require.Nil(t, f.index.nodeByID(id), "waiting insert %d joined the graph", id)
	}
}

func (f *bootstrapFixture) requireDone(t *testing.T, dones ...<-chan error) {
	t.Helper()
	for _, done := range dones {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("insert did not finish after maintenance was released")
		}
	}
}

func (f *bootstrapFixture) requireNoSelfLinks(t *testing.T) {
	t.Helper()
	for i := range uint64(len(f.vectors)) {
		node := f.index.nodeByID(i)
		if node == nil {
			continue
		}
		node.Lock()
		for level := uint8(0); level < node.connections.Layers(); level++ {
			require.NotContains(t, node.connections.GetLayer(level), i, "node %d links to itself at level %d", i, level)
		}
		node.Unlock()
	}
}

func (f *bootstrapFixture) addWithin(t *testing.T, id uint64, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.index.Add(f.ctx, id, f.vectors[id]) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatalf("insert of %d did not finish within %s", id, timeout)
	}
}

func TestEntrypoint_BootstrapLastLiveNode(t *testing.T) {
	variants := []struct {
		name       string
		compressed bool
	}{
		{name: "uncompressed", compressed: false},
		{name: "rq8", compressed: true},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Run("every live node taken by cleanup after the node joined", func(t *testing.T) {
				// the entrypoint was usable when the insert picked it, then
				// cleanup took every live node under maintenance before the
				// insert connected; the insert must wait, not fail
				f := newBootstrapFixture(t, variant.compressed)
				live := []*vertex{f.index.nodeByID(0), f.index.nodeByID(1), f.index.nodeByID(2)}
				var once sync.Once
				f.onAddNode = func(id uint64) {
					if id != 3 {
						return
					}
					once.Do(func() {
						for _, node := range live {
							node.markAsMaintenance()
						}
						go func() {
							time.Sleep(30 * time.Millisecond)
							for _, node := range live {
								node.unmarkAsMaintenance()
							}
						}()
					})
				}

				f.addWithin(t, 3, 10*time.Second)
				require.GreaterOrEqual(t, f.waits(3), 1, "insert should have waited for maintenance to end")
				f.requireReachable(t, 0, 1, 2, 3)
				f.requireNoSelfLinks(t)
			})

			t.Run("ghost promoted by deleting the last live nodes", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				f.leaveGhost(t, 3)

				require.NoError(t, f.index.Delete(0, 1, 2))
				require.Equal(t, uint64(3), f.index.getEntrypoint())

				f.addWithin(t, 3, 10*time.Second)
				f.requireReachable(t, 3)

				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				f.requireReachable(t, 3)

				f.addWithin(t, 4, 10*time.Second)
				f.requireReachable(t, 3, 4)
			})

			t.Run("bootstrapped entrypoint survives restart", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				f.deleteFromObjectStore(f.index.getEntrypoint())
				f.addWithin(t, 3, 10*time.Second)
				require.Equal(t, uint64(3), f.index.getEntrypoint())

				f.restart(t)
				require.Equal(t, uint64(3), f.index.getEntrypoint())
				f.requireReachable(t, 3)
				f.addWithin(t, 4, 10*time.Second)
				f.requireReachable(t, 3, 4)

				f.restart(t)
				f.requireReachable(t, 3, 4)
			})

			t.Run("bootstrap publishes a usable entrypoint", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				f.leaveGhost(t, 3)
				require.NoError(t, f.index.Delete(0, 1, 2))
				node := f.index.nodeByID(3)
				node.markAsMaintenance()

				ok, err := f.index.bootstrapEntrypoint(3, node, f.vectors[3])
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, uint64(3), f.index.getEntrypoint())
				require.False(t, node.isUnderMaintenance())
			})

			t.Run("ghost retried after the entrypoint lost its object", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				f.leaveGhost(t, 3)

				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				f.deleteFromObjectStore(f.index.getEntrypoint())

				f.addWithin(t, 3, 10*time.Second)
				f.requireReachable(t, 3)

				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				f.addWithin(t, 4, 10*time.Second)
				f.requireReachable(t, 3, 4)
			})

			t.Run("first insert after the entrypoint lost its object", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				f.deleteFromObjectStore(f.index.getEntrypoint())

				f.addWithin(t, 3, 10*time.Second)
				f.requireReachable(t, 3)

				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				f.addWithin(t, 4, 10*time.Second)
				f.requireReachable(t, 3, 4)
			})

			t.Run("only other live node is held by cleanup", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				err := addWithDeadline(f.index, 3, f.vectors[3])
				require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
				require.True(t, enterrors.IsTransient(err))
				require.Equal(t, ep, f.index.getEntrypoint())

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.addWithin(t, 3, 10*time.Second)
				f.requireReachable(t, ep, 3)
			})

			t.Run("insert waits for the node held by cleanup", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				done := make(chan error, 1)
				go func() { done <- f.index.Add(f.ctx, 3, f.vectors[3]) }()
				select {
				case err := <-done:
					t.Fatalf("insert finished while cleanup held the only live node: %v", err)
				case <-time.After(100 * time.Millisecond):
				}

				f.index.nodeByID(ep).unmarkAsMaintenance()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("insert did not finish after cleanup released the node")
				}
				f.requireReachable(t, ep, 3)
			})

			t.Run("waiting insert stops on shutdown", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()
				defer f.index.nodeByID(ep).unmarkAsMaintenance()

				done := make(chan error, 1)
				go func() { done <- f.index.Add(f.ctx, 3, f.vectors[3]) }()
				f.waitUntilWaiting(t, 3, 1)

				// the first step of Shutdown; the full Shutdown runs in cleanup
				f.index.shutdownCtxCancel()
				select {
				case err := <-done:
					require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
					require.ErrorIs(t, err, context.Canceled)
					require.True(t, enterrors.IsTransient(err))
				case <-time.After(10 * time.Second):
					t.Fatal("waiting insert did not stop on shutdown")
				}
			})

			t.Run("concurrent inserts do not strand the node held by cleanup", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				done3 := f.startAdd(f.ctx, 3)
				f.waitUntilWaiting(t, 3, 1)
				done4 := f.startAdd(f.ctx, 4)
				f.waitUntilWaiting(t, 4, 1)
				f.requireNotJoined(t, 3, 4)
				require.Equal(t, ep, f.index.getEntrypoint())

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireDone(t, done3, done4)
				f.requireReachable(t, ep, 3, 4)
				f.requireNoSelfLinks(t)
				f.restart(t)
				f.requireReachable(t, ep, 3, 4)
			})

			t.Run("waiting insert does not block deletes", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				done3 := f.startAdd(f.ctx, 3)
				f.waitUntilWaiting(t, 3, 1)

				deleted := make(chan error, 1)
				go func() { deleted <- f.index.Delete(5) }()
				select {
				case err := <-deleted:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("delete blocked behind a waiting insert")
				}

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireDone(t, done3)
				f.requireReachable(t, ep, 3)
			})

			t.Run("waiting inserts stay out of the graph", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				var dones []<-chan error
				for id := uint64(3); id < uint64(len(f.vectors)); id++ {
					dones = append(dones, f.startAdd(f.ctx, id))
				}
				for id := uint64(3); id < uint64(len(f.vectors)); id++ {
					f.waitUntilWaiting(t, id, 1)
				}
				f.requireNotJoined(t, 3, 4, 5, 6, 7)

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireDone(t, dones...)
				f.requireReachable(t, ep, 3, 4, 5, 6, 7)
				f.requireNoSelfLinks(t)
				f.restart(t)
				f.requireReachable(t, ep, 3, 4, 5, 6, 7)
			})

			t.Run("insert retried after its deadline keeps every insert reachable", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				deadline, cancel := context.WithCancel(f.ctx)
				timedOut := f.startAdd(deadline, 3)
				f.waitUntilWaiting(t, 3, 1)
				done4 := f.startAdd(f.ctx, 4)
				f.waitUntilWaiting(t, 4, 1)

				cancel()
				select {
				case err := <-timedOut:
					require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
					require.True(t, enterrors.IsTransient(err))
				case <-time.After(10 * time.Second):
					t.Fatal("insert of 3 did not stop when its context ended")
				}
				// the failed insert left nothing behind
				f.requireNotJoined(t, 3)

				retry := f.startAdd(f.ctx, 3)
				f.waitUntilWaiting(t, 3, 2)
				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireDone(t, retry, done4)
				f.requireReachable(t, ep, 3, 4)
				f.requireNoSelfLinks(t)
				f.restart(t)
				f.requireReachable(t, ep, 3, 4)
			})
		})
	}
}
