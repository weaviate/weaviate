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
}

func newBootstrapFixture(t *testing.T, compressed bool) *bootstrapFixture {
	ctx := context.Background()
	f := &bootstrapFixture{ctx: ctx}
	f.vectors, _ = testinghelpers.RandomVecsFixedSeed(8, 1, 16)
	logger, _ := test.NewNullLogger()
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
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(dir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
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

// leaveGhost fails the insert with every live node under maintenance
func (f *bootstrapFixture) leaveGhost(t *testing.T, id uint64) {
	var live []*vertex
	for i := uint64(0); i < uint64(len(f.vectors)); i++ {
		if node := f.index.nodeByID(i); node != nil && !f.index.hasTombstone(i) {
			live = append(live, node)
		}
	}
	for _, node := range live {
		node.markAsMaintenance()
	}
	err := f.index.Add(f.ctx, id, f.vectors[id])
	require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
	for _, node := range live {
		node.unmarkAsMaintenance()
	}
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

				ok, err := f.index.bootstrapEntrypoint(3, node)
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

				err := f.index.Add(f.ctx, 3, f.vectors[3])
				require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
				require.True(t, enterrors.IsTransient(err))
				require.Equal(t, ep, f.index.getEntrypoint())

				f.index.nodeByID(ep).unmarkAsMaintenance()
				f.addWithin(t, 3, 10*time.Second)
				f.requireReachable(t, ep, 3)
			})
		})
	}
}
