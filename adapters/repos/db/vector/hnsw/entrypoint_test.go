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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type bootstrapFixture struct {
	index     *hnsw
	vectors   [][]float32
	gone      sync.Map
	ctx       context.Context
	cfg       Config
	uc        ent.UserConfig
	store     *lsmkv.Store
	commitLog *hookedCommitLogger
	onAddNode func(id uint64)
	hookMu    sync.Mutex
	// onVector runs when the object store is asked for id's vector, outside
	// every index lock; evict the id from the cache first to make that happen.
	// The vector is read after the hook, so a hook may still remove it.
	onVector func(id uint64)
}

type hookedCommitLogger struct {
	CommitLogger
	onAddNode          func(id uint64)
	setEntrypointCalls atomic.Int32
	failReplaceLinks   atomic.Bool
	// dropWrites loses entrypoint changes and tombstones, like a crash before
	// the log reached disk
	dropWrites atomic.Bool
	// dropReset loses the reset of an emptied graph, like a crash right after
	// the last tombstone reached disk
	dropReset atomic.Bool
}

func (l *hookedCommitLogger) Reset() error {
	if l.dropReset.Load() {
		return nil
	}
	return l.CommitLogger.Reset()
}

func (l *hookedCommitLogger) AddTombstone(id uint64) error {
	if l.dropWrites.Load() {
		return nil
	}
	return l.CommitLogger.AddTombstone(id)
}

func (l *hookedCommitLogger) ReplaceLinksAtLevel(id uint64, level int, links []uint64) error {
	if l.failReplaceLinks.Load() {
		return fmt.Errorf("injected commit log error")
	}
	return l.CommitLogger.ReplaceLinksAtLevel(id, level, links)
}

func (l *hookedCommitLogger) AddNode(id uint64, level uint16) error {
	l.onAddNode(id)
	return l.CommitLogger.AddNode(id, level)
}

func (l *hookedCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	l.setEntrypointCalls.Add(1)
	if l.dropWrites.Load() {
		return nil
	}
	return l.CommitLogger.SetEntryPointWithMaxLayer(id, level)
}

func newBootstrapFixture(t *testing.T, compressed bool) *bootstrapFixture {
	return newBootstrapFixtureWith(t, compressed, 32, 3)
}

// newBootstrapFixtureWith opens an index with the given maximum connections
// and inserts the first nodes, all on level 0
func newBootstrapFixtureWith(t *testing.T, compressed bool, maxConnections, nodes int) *bootstrapFixture {
	ctx := context.Background()
	f := &bootstrapFixture{ctx: ctx}
	f.vectors, _ = testinghelpers.RandomVecsFixedSeed(64, 1, 16)
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })
	dir := t.TempDir()
	indexID := "entrypoint-bootstrap"

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = maxConnections
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
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			commitLog, err := NewCommitLogger(dir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
			if err != nil {
				return nil, err
			}
			f.commitLog = &hookedCommitLogger{CommitLogger: commitLog, onAddNode: func(id uint64) {
				if f.onAddNode != nil {
					f.onAddNode(id)
				}
			}}
			return f.commitLog, nil
		},
		DistanceProvider: distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			f.hookMu.Lock()
			hook := f.onVector
			f.hookMu.Unlock()
			if hook != nil {
				hook(id)
			}
			// after the hook, so a hook can make this very read fail
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

	for i := range nodes {
		require.NoError(t, f.index.Add(ctx, uint64(i), f.vectors[i]))
	}
	if nodes > 0 {
		require.Equal(t, compressed, f.index.Compressed())
	}
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

// restart reopens the index and waits for the cache prefill, which reads the
// vectors through the object store hook in the background
func (f *bootstrapFixture) restart(t *testing.T) {
	require.NoError(t, f.index.Flush())
	require.NoError(t, f.index.Shutdown(f.ctx))
	f.open(t)
	f.index.prefillWg.Wait()
}

// setVectorHook installs a hook that runs once, the first time the object
// store is asked for target's vector
func (f *bootstrapFixture) setVectorHook(target uint64, hook func()) {
	f.hookMu.Lock()
	defer f.hookMu.Unlock()
	f.onVector = func(id uint64) {
		if id != target {
			return
		}
		f.hookMu.Lock()
		f.onVector = nil
		f.hookMu.Unlock()
		hook()
	}
}

func (f *bootstrapFixture) evict(id uint64) {
	if f.index.Compressed() {
		f.index.compressor.Delete(f.ctx, id)
	} else {
		f.index.cache.Delete(f.ctx, id)
	}
}

func (f *bootstrapFixture) deleteFromObjectStore(id uint64) {
	f.gone.Store(id, struct{}{})
	f.evict(id)
}

// leaveGhost adds a node without links, like an insert that failed after joining
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

func (f *bootstrapFixture) requireDone(t *testing.T, dones ...<-chan error) {
	t.Helper()
	for _, done := range dones {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("insert did not finish")
		}
	}
}

func (f *bootstrapFixture) layer(id uint64, level int) []uint64 {
	node := f.index.nodeByID(id)
	if node == nil {
		return nil
	}
	node.Lock()
	defer node.Unlock()
	return node.connections.GetLayer(uint8(level))
}

func (f *bootstrapFixture) requireLayersWellFormed(t *testing.T) {
	t.Helper()
	requireLayersWellFormed(t, f.index)
}

// requireLayersWellFormed checks every node's layers: within capacity, no
// duplicate links, no self links
func requireLayersWellFormed(t *testing.T, index *hnsw) {
	t.Helper()
	index.RLock()
	size := len(index.nodes)
	index.RUnlock()
	for i := range uint64(size) {
		node := index.nodeByID(i)
		if node == nil {
			continue
		}
		node.Lock()
		layers := node.connections.GetAllLayers()
		node.Unlock()
		for level, links := range layers {
			capacity := index.maximumConnections
			if level == 0 {
				capacity = index.maximumConnectionsLayerZero
			}
			require.LessOrEqual(t, len(links), capacity, "node %d level %d over capacity: %v", i, level, links)
			seen := make(map[uint64]struct{}, len(links))
			for _, link := range links {
				require.NotEqual(t, i, link, "node %d links to itself at level %d", i, level)
				_, dup := seen[link]
				require.False(t, dup, "node %d has duplicate link %d at level %d: %v", i, link, level, links)
				seen[link] = struct{}{}
			}
		}
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
				// every live node is taken under maintenance the moment the node
				// joins and stays so until the insert is done
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
					})
				}

				f.addWithin(t, 3, 10*time.Second)
				for _, node := range live {
					node.unmarkAsMaintenance()
				}
				f.requireReachable(t, 0, 1, 2, 3)
				f.requireLayersWellFormed(t)
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
				// the held node is usable as the entrypoint right away: nothing
				// waits, the entrypoint is not churned
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()
				persisted := f.commitLog.setEntrypointCalls.Load()

				f.addWithin(t, 3, 10*time.Second)
				require.Equal(t, ep, f.index.getEntrypoint())
				require.Equal(t, persisted, f.commitLog.setEntrypointCalls.Load(), "entrypoint must not be re-persisted")
				f.requireReachable(t, ep, 3)

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireReachable(t, ep, 3)
				f.requireLayersWellFormed(t)
			})

			t.Run("concurrent inserts while cleanup holds the only live node", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				require.NoError(t, f.index.Delete(f.nonEntrypointNodes()...))
				ep := f.index.getEntrypoint()
				f.index.nodeByID(ep).markAsMaintenance()

				var dones []<-chan error
				for id := uint64(3); id < 8; id++ {
					dones = append(dones, f.startAdd(f.ctx, id))
				}
				f.requireDone(t, dones...)
				require.Equal(t, ep, f.index.getEntrypoint())
				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.requireReachable(t, ep, 3, 4, 5, 6, 7)
				f.requireLayersWellFormed(t)
				f.restart(t)
				f.requireReachable(t, ep, 3, 4, 5, 6, 7)
			})

			t.Run("every node under maintenance", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				for i := range uint64(3) {
					f.index.nodeByID(i).markAsMaintenance()
				}
				ep := f.index.getEntrypoint()

				f.addWithin(t, 3, 10*time.Second)
				require.Equal(t, ep, f.index.getEntrypoint())
				for i := range uint64(3) {
					f.index.nodeByID(i).unmarkAsMaintenance()
				}
				f.requireReachable(t, 0, 1, 2, 3)
				f.requireLayersWellFormed(t)
			})

			t.Run("entrypoint held by cleanup and the rest gone", func(t *testing.T) {
				// the global entrypoint is being rewired, the node the repair would
				// pick lost its object, the last one is held by a cleanup worker
				f := newBootstrapFixture(t, variant.compressed)
				ep := f.index.getEntrypoint()
				others := f.nonEntrypointNodes()
				f.index.nodeByID(ep).markAsMaintenance()
				f.deleteFromObjectStore(others[0])
				f.index.nodeByID(others[1]).markAsMaintenance()

				f.addWithin(t, 3, 10*time.Second)
				require.Equal(t, ep, f.index.getEntrypoint())
				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.index.nodeByID(others[1]).unmarkAsMaintenance()
				found, _, err := f.index.SearchByVector(f.ctx, f.vectors[3], 4, nil)
				require.NoError(t, err)
				require.Contains(t, found, uint64(3))
				require.Contains(t, found, ep)
				require.Contains(t, found, others[1])
			})

			t.Run("only other live node is being inserted and has no links yet", func(t *testing.T) {
				// a delete made the inserting node the entrypoint: it is used as
				// the last resort rather than waited for
				f := newBootstrapFixture(t, variant.compressed)
				node := &vertex{level: 0}
				node.connections.GrowLayersTo(0)
				require.NoError(t, f.index.publishNode(4, node, f.vectors[4]))
				node.markAsMaintenance()
				require.NoError(t, f.index.Delete(0, 1, 2))
				require.Equal(t, uint64(4), f.index.getEntrypoint())

				f.addWithin(t, 5, 10*time.Second)
				require.Equal(t, uint64(4), f.index.getEntrypoint())
				require.ElementsMatch(t, []uint64{4}, f.layer(5, 0))
				require.ElementsMatch(t, []uint64{5}, f.layer(4, 0), "the link to the newcomer survives")
				node.unmarkAsMaintenance()
				f.requireReachable(t, 4, 5)
			})

			t.Run("insert stops when its context ends during repair", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				ep := f.index.getEntrypoint()
				others := f.nonEntrypointNodes()
				f.deleteFromObjectStore(ep)
				ctx, cancel := context.WithCancel(f.ctx)
				// the context ends as the entrypoint is found gone, before the
				// repair picks the next candidate
				hookRan := false
				f.setVectorHook(ep, func() {
					hookRan = true
					cancel()
				})

				err := f.index.Add(ctx, 3, f.vectors[3])
				require.True(t, hookRan)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, f.index.nodeByID(3), "a failed insert must not leave a node behind")

				f.addWithin(t, 3, 10*time.Second)
				require.NotEqual(t, ep, f.index.getEntrypoint())
				f.requireReachable(t, others[0], others[1], 3)
			})
		})
	}
}

// A ghost is in the graph but out of reach of any search, which ContainsDoc doesn't
// tell: whatever decides from it that a document needs no work leaves it
// unsearchable for good.
func TestHasGhost(t *testing.T) {
	t.Run("a ghost is reported", func(t *testing.T) {
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)

		require.True(t, f.index.HasGhost(3))
		require.True(t, f.index.ContainsDoc(3))
	})

	t.Run("connected nodes are no ghosts", func(t *testing.T) {
		f := newBootstrapFixture(t, false)

		for id := range uint64(3) {
			require.False(t, f.index.HasGhost(id))
		}
	})

	t.Run("an unknown doc is no ghost", func(t *testing.T) {
		f := newBootstrapFixture(t, false)

		require.False(t, f.index.HasGhost(1000))
	})

	t.Run("an entrypoint needs no edges", func(t *testing.T) {
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)

		require.NoError(t, f.index.Delete(0, 1, 2))
		require.Equal(t, uint64(3), f.index.getEntrypoint())
		require.False(t, f.index.HasGhost(3))
	})

	t.Run("links nobody returns don't help", func(t *testing.T) {
		// an insert that failed after it set its own links
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)

		f.index.nodeByID(3).setConnectionsAtLevel(0, []uint64{0, 1})

		require.True(t, f.index.HasGhost(3))
	})

	t.Run("a part of the graph cut off from the entrypoint", func(t *testing.T) {
		t.Skip("https://github.com/weaviate/weaviate/issues/13698")

		f := newBootstrapFixture(t, false)
		require.Equal(t, uint64(0), f.index.getEntrypoint())

		f.index.nodeByID(0).setConnectionsAtLevel(0, nil)

		require.True(t, f.index.HasGhost(1))
		require.True(t, f.index.HasGhost(2))
	})

	t.Run("a node being inserted is no ghost", func(t *testing.T) {
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)

		f.index.nodeByID(3).markAsMaintenance()
		require.False(t, f.index.HasGhost(3))
	})

	t.Run("a ghost replaced under a new id is gone after cleanup", func(t *testing.T) {
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)

		require.NoError(t, f.index.Delete(3))
		f.addWithin(t, 4, 10*time.Second)
		require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))

		f.requireReachable(t, 0, 1, 2, 4)
		require.False(t, f.index.ContainsDoc(3))
	})

	t.Run("a ghost is still reported after a restart", func(t *testing.T) {
		f := newBootstrapFixture(t, false)
		f.leaveGhost(t, 3)
		f.restart(t)

		require.True(t, f.index.HasGhost(3))
	})
}
