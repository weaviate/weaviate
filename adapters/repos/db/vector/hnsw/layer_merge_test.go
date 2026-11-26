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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func uniqueLinks(ids []uint64, self uint64) []uint64 {
	seen := map[uint64]struct{}{}
	var out []uint64
	for _, id := range ids {
		if _, ok := seen[id]; ok || id == self {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// expectedExtras returns the appended links writeLayer keeps: the unique ones
// that are not already links, in arrival order, as many as fit
func expectedExtras(extras []uint64, self uint64, links []uint64, capacity int) []uint64 {
	var kept []uint64
	room := capacity - len(links)
	for _, id := range uniqueLinks(extras, self) {
		if containsID(links, id) || room <= 0 {
			continue
		}
		kept = append(kept, id)
		room--
	}
	return kept
}

// without returns the links of layer that are not in extras
func without(layer, extras []uint64) []uint64 {
	var rest []uint64
	for _, id := range layer {
		if !containsID(extras, id) {
			rest = append(rest, id)
		}
	}
	return rest
}

func idRange(from, to uint64) []uint64 {
	var ids []uint64
	for id := from; id < to; id++ {
		ids = append(ids, id)
	}
	return ids
}

// requireFinds checks a search for each id's vector returns every id
func (f *bootstrapFixture) requireFinds(t *testing.T, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		found, _, err := f.index.SearchByVector(f.ctx, f.vectors[id], len(f.vectors), nil)
		require.NoError(t, err)
		require.Subset(t, found, ids, "search from %d", id)
	}
}

// appendLinks adds links to a node's layer the way concurrent inserts do in
// connectNeighborAtLevel
func (f *bootstrapFixture) appendLinks(t *testing.T, id uint64, level int, links []uint64) {
	t.Helper()
	node := f.index.nodeByID(id)
	require.NotNil(t, node, "node %d must be in the graph", id)
	node.Lock()
	defer node.Unlock()
	for _, link := range links {
		node.connections.InsertAtLayer(link, uint8(level))
	}
}

// Links concurrent inserts append to a node while it searches a level survive
// its own write of that level, and the layer stays within capacity.
func TestWriteLayer_KeepsLinksAppendedDuringInsert(t *testing.T) {
	const maxConnections = 4
	tests := []struct {
		name   string
		extras []uint64
	}{
		{name: "within capacity", extras: []uint64{10, 11}},
		{name: "over capacity", extras: idRange(10, 26)},
		{name: "self, duplicates and an existing neighbor", extras: []uint64{3, 10, 10, 0, 3, 11}},
		{name: "far over capacity", extras: idRange(10, 60)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapFixtureWith(t, false, maxConnections, 3)
			for id := uint64(10); id < 60; id++ {
				f.leaveGhost(t, id)
			}
			ep := f.index.getEntrypoint()
			probe := f.nonEntrypointNodes()[0]
			f.evict(probe)
			var injected atomic.Bool
			f.setVectorHook(probe, func() {
				f.appendLinks(t, 3, 0, tc.extras)
				injected.Store(true)
			})

			f.addWithin(t, 3, 10*time.Second)
			require.True(t, injected.Load(), "the level search must have asked for %d's vector", probe)
			require.Equal(t, ep, f.index.getEntrypoint())

			layer := f.layer(3, 0)
			capacity := f.index.maximumConnectionsLayerZero
			require.LessOrEqual(t, len(layer), capacity, "layer over capacity: %v", layer)
			require.ElementsMatch(t, uniqueLinks(layer, 3), layer, "duplicate or self link: %v", layer)
			require.Subset(t, layer, []uint64{0, 1, 2}, "neighbors found by the search must be linked")
			found := without(layer, tc.extras)
			require.ElementsMatch(t, append(found, expectedExtras(tc.extras, 3, found, capacity)...), layer,
				"appended links must fill the layer in arrival order")
			f.requireFinds(t, 0, 1, 2, 3)
			f.requireLayersWellFormed(t)

			f.restart(t)
			require.ElementsMatch(t, layer, f.layer(3, 0), "commit log replay must give the same layer")
			f.requireFinds(t, 0, 1, 2, 3)
		})
	}
}

// Links concurrent inserts append to a node while tombstone cleanup rewires
// it survive the rewrite.
func TestWriteLayer_KeepsLinksAppendedDuringRewire(t *testing.T) {
	const maxConnections = 4
	tests := []struct {
		name   string
		extras []uint64
	}{
		{name: "within capacity", extras: []uint64{40, 41}},
		{name: "over capacity", extras: idRange(40, 60)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapFixtureWith(t, false, maxConnections, 12)
			for id := uint64(40); id < 60; id++ {
				f.leaveGhost(t, id)
			}

			// the rewire of target must compute a distance to probe, a node it
			// is not linked to that the deleted node links to
			var target, deleted, probe uint64
			found := false
			for x := uint64(0); x < 12 && !found; x++ {
				links := f.layer(x, 0)
				for _, d := range links {
					for _, y := range f.layer(d, 0) {
						if y != x && !containsID(links, y) {
							target, deleted, probe, found = x, d, y, true
							break
						}
					}
					if found {
						break
					}
				}
			}
			require.True(t, found, "no node with a two-hop neighbor in this graph")

			require.NoError(t, f.index.Delete(deleted))
			before := f.layer(target, 0)
			f.evict(probe)
			var injected atomic.Bool
			f.setVectorHook(probe, func() {
				require.True(t, f.index.nodeByID(target).isUnderMaintenance(), "the hook must run during the rewire")
				f.appendLinks(t, target, 0, tc.extras)
				injected.Store(true)
			})

			ok, err := f.index.reassignNeighbor(f.ctx, target, helpers.NewAllowList(deleted), neverStop, nil)
			require.NoError(t, err)
			require.True(t, ok)
			require.True(t, injected.Load(), "the rewire must have asked for %d's vector", probe)
			require.False(t, f.index.nodeByID(target).isUnderMaintenance())

			layer := f.layer(target, 0)
			capacity := f.index.maximumConnectionsLayerZero
			require.LessOrEqual(t, len(layer), capacity, "layer over capacity: %v", layer)
			require.ElementsMatch(t, uniqueLinks(layer, target), layer, "duplicate or self link: %v", layer)
			require.NotContains(t, layer, deleted)
			for _, id := range before {
				if id != deleted {
					require.Contains(t, layer, id, "the rewire must keep the links not pointing to the deleted node")
				}
			}
			rest := without(layer, tc.extras)
			require.ElementsMatch(t, append(rest, expectedExtras(tc.extras, target, rest, capacity)...), layer,
				"appended links must fill the layer in arrival order")
			f.requireLayersWellFormed(t)
		})
	}
}

// An insert that failed after joining is retried: the retry, and inserts in
// between, land reachable nodes.
func TestInsert_RetryAfterFailedLinkWrite(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%v", compressed), func(t *testing.T) {
			f := newBootstrapFixture(t, compressed)
			f.commitLog.failReplaceLinks.Store(true)
			err := f.index.Add(f.ctx, 3, f.vectors[3])
			require.ErrorContains(t, err, "injected")
			require.NotNil(t, f.index.nodeByID(3), "the node joined before the failed write")
			require.False(t, f.index.nodeByID(3).isUnderMaintenance())
			f.commitLog.failReplaceLinks.Store(false)

			f.addWithin(t, 4, 10*time.Second)
			f.addWithin(t, 3, 10*time.Second)
			f.requireReachable(t, 0, 1, 2, 3, 4)
			f.requireLayersWellFormed(t)
			f.restart(t)
			f.requireReachable(t, 0, 1, 2, 3, 4)
		})
	}
}

// newStressIndex opens an index with random levels over vectors; ids in gone
// are reported as deleted from the object store
func newStressIndex(t *testing.T, compressed bool, vectors [][]float32, gone *sync.Map, maxConns int) *hnsw {
	ctx := context.Background()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = maxConns
	uc.EFConstruction = 64
	uc.VectorCacheMaxObjects = 100000
	if compressed {
		uc.RQ = ent.RQConfig{Enabled: true, Bits: 8}
	} else {
		uc.RQ.Enabled = false
		uc.SkipDefaultQuantization = true
	}
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "stress",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if _, ok := gone.Load(id); ok {
				return nil, storobj.NewErrNotFoundf(id, "deleted from object store")
			}
			return vectors[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })
	index.PostStartup(ctx)
	return index
}

// requireAllReachable checks a search for each live id's vector returns it.
//
// The neighbor heuristic guarantees no node an in-link: an outlier gets few
// backlinks at insert, its neighbors prune them as their layers fill, and the
// hubs it linked to are deleted, so it can end with no live node pointing at
// it and no search can reach it. The fixed-seed stress set has such a node
// (343). That is a known recall weakness of the heuristic, not a torn graph,
// so a bounded number of nodes without any live in-link is tolerated. The
// search explores the whole layer 0 component here (ef exceeds the live
// count), so a node that is unreachable although a reachable live node links
// to it at layer 0 is a bug.
func requireAllReachable(t *testing.T, index *hnsw, vectors [][]float32, live []uint64) {
	t.Helper()
	ctx := context.Background()
	unreachable := map[uint64]struct{}{}
	for _, id := range live {
		found, _, err := index.SearchByVector(ctx, vectors[id], len(live), nil)
		require.NoError(t, err)
		if !containsID(found, id) {
			unreachable[id] = struct{}{}
		}
	}
	if len(unreachable) == 0 {
		return
	}
	for id := range unreachable {
		for _, from := range live {
			if from == id {
				continue
			}
			if _, gone := unreachable[from]; gone {
				continue
			}
			require.False(t, linksToAtLayerZero(index, from, id),
				"node %d is unreachable although reachable node %d links to it", id, from)
		}
		t.Logf("node %d has no in-link from any live node, pruned by the heuristic", id)
	}
	require.LessOrEqual(t, len(unreachable), max(1, len(live)/100),
		"too many nodes without an in-link: %v", unreachable)
}

func linksToAtLayerZero(index *hnsw, from, id uint64) bool {
	node := index.nodeByID(from)
	if node == nil {
		return false
	}
	node.Lock()
	defer node.Unlock()
	return containsID(node.connections.GetLayer(0), id)
}

// Concurrent inserts, deletes and cleanup cycles: every live node with an
// in-link stays reachable and every layer stays within capacity without
// duplicates.
func TestLayerMerge_ConcurrentInsertsDeletesCleanup(t *testing.T) {
	const (
		total      = 400
		upfront    = 100
		deletes    = 50
		inserters  = 10
		maxConns   = 8
		dimensions = 16
	)
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%v", compressed), func(t *testing.T) {
			ctx := context.Background()
			vectors, _ := testinghelpers.RandomVecsFixedSeed(total, 1, dimensions)
			index := newStressIndex(t, compressed, vectors, &sync.Map{}, maxConns)

			for i := range uint64(upfront) {
				require.NoError(t, index.Add(ctx, i, vectors[i]))
			}
			require.Equal(t, compressed, index.Compressed())

			var wg sync.WaitGroup
			errs := make(chan error, inserters+1)
			perInserter := (total - upfront) / inserters
			for w := range inserters {
				wg.Add(1)
				go func() {
					defer wg.Done()
					from := uint64(upfront + w*perInserter)
					for id := from; id < from+uint64(perInserter); id++ {
						if err := index.Add(ctx, id, vectors[id]); err != nil {
							errs <- fmt.Errorf("insert %d: %w", id, err)
							return
						}
					}
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range uint64(deletes) {
					if err := index.Delete(id); err != nil {
						errs <- fmt.Errorf("delete %d: %w", id, err)
						return
					}
					if id%10 == 9 {
						if err := index.CleanUpTombstonedNodes(neverStop); err != nil {
							errs <- fmt.Errorf("cleanup: %w", err)
							return
						}
					}
				}
			}()
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			require.NoError(t, index.CleanUpTombstonedNodes(neverStop))

			requireLayersWellFormed(t, index)
			requireAllReachable(t, index, vectors, idRange(deletes, total))
		})
	}
}

// The live global entrypoint is deleted over and over while inserts import
// and cleanup cycles run, in the production order: the object leaves the
// store first, then the index delete. Every surviving node with an in-link
// stays reachable and the entrypoint ends on a live node.
func TestEntrypoint_DeleteEntrypointDuringConcurrentImport(t *testing.T) {
	const (
		total      = 400
		upfront    = 100
		inserters  = 10
		maxConns   = 8
		dimensions = 16
		deletes    = 40
	)
	for _, compressed := range []bool{false, true} {
		for _, objectGone := range []bool{false, true} {
			t.Run(fmt.Sprintf("compressed=%v/objectGone=%v", compressed, objectGone), func(t *testing.T) {
				ctx := context.Background()
				vectors, _ := testinghelpers.RandomVecsFixedSeed(total, 1, dimensions)
				var gone sync.Map
				index := newStressIndex(t, compressed, vectors, &gone, maxConns)
				for i := range uint64(upfront) {
					require.NoError(t, index.Add(ctx, i, vectors[i]))
				}
				require.Equal(t, compressed, index.Compressed())

				var wg sync.WaitGroup
				errs := make(chan error, inserters+1)
				perInserter := (total - upfront) / inserters
				for w := range inserters {
					wg.Add(1)
					go func() {
						defer wg.Done()
						from := uint64(upfront + w*perInserter)
						for id := from; id < from+uint64(perInserter); id++ {
							if err := index.Add(ctx, id, vectors[id]); err != nil {
								errs <- fmt.Errorf("insert %d: %w", id, err)
								return
							}
						}
					}()
				}
				var deletedMu sync.Mutex
				deleted := map[uint64]struct{}{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range deletes {
						ep := index.getEntrypoint()
						deletedMu.Lock()
						_, again := deleted[ep]
						deleted[ep] = struct{}{}
						deletedMu.Unlock()
						if again {
							// the entrypoint has not moved yet, give the inserts a turn
							time.Sleep(time.Millisecond)
							continue
						}
						if objectGone {
							gone.Store(ep, struct{}{})
							if compressed {
								index.compressor.Delete(ctx, ep)
							} else {
								index.cache.Delete(ctx, ep)
							}
						}
						if err := index.Delete(ep); err != nil {
							errs <- fmt.Errorf("delete entrypoint %d: %w", ep, err)
							return
						}
						if i%5 == 4 {
							if err := index.CleanUpTombstonedNodes(neverStop); err != nil {
								errs <- fmt.Errorf("cleanup: %w", err)
								return
							}
						}
					}
				}()
				wg.Wait()
				close(errs)
				for err := range errs {
					require.NoError(t, err)
				}
				require.NoError(t, index.CleanUpTombstonedNodes(neverStop))

				ep := index.getEntrypoint()
				_, epDeleted := deleted[ep]
				require.False(t, epDeleted, "entrypoint %d was deleted", ep)
				require.NotNil(t, index.nodeByID(ep))
				require.False(t, index.hasTombstone(ep))
				require.Greater(t, len(deleted), 1, "the entrypoint must have moved")
				t.Logf("deleted %d distinct entrypoints during the import", len(deleted))
				var live []uint64
				for id := range uint64(total) {
					if _, ok := deleted[id]; !ok {
						live = append(live, id)
					}
				}
				requireLayersWellFormed(t, index)
				requireAllReachable(t, index, vectors, live)
			})
		}
	}
}

// Cleanup of a deleted entrypoint lands in the middle of an insert's level
// search: the deleted node disappears and the new entrypoint is rewired while
// the insert runs.
func TestEntrypoint_CleanupOfDeletedEntrypointDuringInsert(t *testing.T) {
	for _, objectGone := range []bool{false, true} {
		t.Run(fmt.Sprintf("objectGone=%v", objectGone), func(t *testing.T) {
			f := newBootstrapFixtureWith(t, false, 4, 12)
			oldEP := f.index.getEntrypoint()
			if objectGone {
				f.deleteFromObjectStore(oldEP)
			}
			require.NoError(t, f.index.Delete(oldEP))
			newEP := f.index.getEntrypoint()
			require.NotEqual(t, oldEP, newEP)

			// the search from the new entrypoint asks for a neighbor's vector
			var probe uint64
			for _, id := range f.layer(newEP, 0) {
				if id != oldEP {
					probe = id
					break
				}
			}
			require.NotEqual(t, newEP, probe)
			f.evict(probe)
			var cleaned atomic.Bool
			f.setVectorHook(probe, func() {
				require.NotNil(t, f.index.nodeByID(12), "the hook must run after the node joined")
				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				cleaned.Store(true)
			})

			f.addWithin(t, 12, 10*time.Second)
			require.True(t, cleaned.Load(), "cleanup must have run during the insert")
			// the insert batches its neighbor distances, so it may have queued
			// oldEP before the cleanup removed it and tombstoned it again on the
			// missing vector; the next cycle drains that tombstone
			require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
			require.Nil(t, f.index.nodeByID(oldEP))
			require.False(t, f.index.hasTombstone(oldEP))
			live := append(idRange(0, 12), 12)
			live = without(live, []uint64{oldEP})
			f.requireLayersWellFormed(t)
			f.requireReachable(t, live...)
		})
	}
}
