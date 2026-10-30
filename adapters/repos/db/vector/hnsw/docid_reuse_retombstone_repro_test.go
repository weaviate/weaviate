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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type reproObjectStore struct {
	mu   sync.Mutex
	vecs map[uint64][]float32
}

func (s *reproObjectStore) get(id uint64) ([]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vecs[id]
	if !ok {
		return nil, storobj.NewErrNotFoundf(id, "object row deleted")
	}
	return v, nil
}

func (s *reproObjectStore) set(id uint64, v []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vecs[id] = v
}

func (s *reproObjectStore) del(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vecs, id)
}

type reproView struct{}

func (reproView) ReleaseView() {}

// reproRescoreGate parks the next rescore at its view acquisition, i.e. after
// the query collected its candidates and before it fetches their vectors.
type reproRescoreGate struct {
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *reproRescoreGate) view() common.BucketView {
	if g.armed.CompareAndSwap(true, false) {
		close(g.reached)
		<-g.release
	}
	return reproView{}
}

func newReproBQIndex(t *testing.T, store *reproObjectStore, gate *reproRescoreGate) (*hnsw, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 16
	uc.EFConstruction = 64
	uc.EF = 64
	uc.VectorCacheMaxObjects = 1_000_000
	uc.BQ = ent.BQConfig{Enabled: true}

	lsm := testinghelpers.NewDummyStore(t)
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "docid-reuse-retombstone-repro",
		Logger:                logger,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return store.get(id)
		},
		GetViewThunk: gate.view,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			v, err := store.get(id)
			if err != nil {
				return nil, err
			}
			copy(container.Slice, v)
			return container.Slice[:len(v)], nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}, uc, cyclemanager.NewCallbackGroupNoop(), lsm)
	require.NoError(t, err)
	t.Cleanup(func() {
		index.Shutdown(context.Background())
		lsm.Shutdown(context.Background())
	})
	return index, hook
}

// reproStaleRescoreAcrossFree runs a real BQ knn search whose rescore is
// parked after candidate collection, while victim is deleted, cleaned up and
// (by CleanForReuse) proven free; the search then resumes against the freed id.
func reproStaleRescoreAcrossFree(t *testing.T, index *hnsw, hook *test.Hook, store *reproObjectStore,
	gate *reproRescoreGate, vecs [][]float32, victim uint64,
) {
	t.Helper()
	ctx := context.Background()
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })

	gate.armed.Store(true)
	searchDone := make(chan []uint64, 1)
	enterrors.GoWrapper(func() {
		ids, _, err := index.SearchByVector(ctx, vecs[victim], 5, nil)
		assert.NoError(t, err)
		searchDone <- ids
	}, index.logger)

	select {
	case <-gate.reached:
	case <-time.After(30 * time.Second):
		t.Fatal("search never reached rescore")
	}

	store.del(victim)
	require.NoError(t, index.Delete(victim))
	require.NoError(t, index.CleanUpTombstonedNodes(never))
	require.False(t, index.ContainsDoc(victim))
	require.True(t, index.CleanForReuse(victim), "victim must be clean (harvestable / acquirable) after cleanup")

	close(gate.release)
	select {
	case <-searchDone:
	case <-time.After(30 * time.Second):
		t.Fatal("search never finished")
	}

	rescoreHitVictim := false
	for _, e := range hook.AllEntries() {
		if e.Data["action"] == "rescore" && strings.Contains(e.Message, fmt.Sprintf("node %d", victim)) {
			rescoreHitVictim = true
		}
	}
	require.True(t, rescoreHitVictim, "the stale query must have rescored victim %d", victim)
}

func reproVectors(n, dims int) [][]float32 {
	r := rand.New(rand.NewSource(7))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = make([]float32, dims)
		for d := range vecs[i] {
			vecs[i][d] = r.Float32()*2 - 1
		}
	}
	return vecs
}

// TestDocIDReuseRepro_StaleRescoreRetombstonesFreedID pins that a query which
// collected a docID before it was freed must not re-dirty that id afterwards.
func TestDocIDReuseRepro_StaleRescoreRetombstonesFreedID(t *testing.T) {
	// ADAPTATION (only change from Jero's verbatim appendix in PR #13036's
	// review, vendored at audit/pr13036-docid-reuse-review.md on
	// tests/docid-reuse-regression): skipped per the regression-suite banner
	// convention. Verified RED at this branch head (f03e86a0fd): both subtests
	// fail (freed-id re-tombstoned / new owner hidden and removed by cleanup).
	// Finding C2.
	t.Skip("pins finding C2 (stale rescore re-tombstones a freed/reused docID); no fix yet — see docid-reuse-jero-audit. Jero's verbatim repro, verified RED at f03e86a0fd.")

	const (
		n      = 50
		dims   = 64
		victim = uint64(7)
	)
	vecs := reproVectors(n+1, dims)

	setup := func(t *testing.T) (*hnsw, *test.Hook, *reproObjectStore, *reproRescoreGate) {
		store := &reproObjectStore{vecs: map[uint64][]float32{}}
		gate := &reproRescoreGate{reached: make(chan struct{}), release: make(chan struct{})}
		index, hook := newReproBQIndex(t, store, gate)
		for i := 0; i < n; i++ {
			store.set(uint64(i), vecs[i])
			require.NoError(t, index.Add(context.Background(), uint64(i), vecs[i]))
		}
		require.True(t, index.compressed.Load())
		require.True(t, index.shouldRescore())
		require.NotEqual(t, victim, index.Entrypoint())
		return index, hook, store, gate
	}

	t.Run("freed id stays clean, so the next Acquire does not fail the insert", func(t *testing.T) {
		index, hook, store, gate := setup(t)
		reproStaleRescoreAcrossFree(t, index, hook, store, gate, vecs, victim)

		assert.False(t, index.hasTombstone(victim), "stale rescore re-added a tombstone on freed docID %d", victim)
		assert.True(t, index.CleanForReuse(victim),
			"freed docID %d no longer clean: Acquire's assertNoTraces would fail the user's insert", victim)
	})

	t.Run("new owner of the reused id stays searchable and survives cleanup", func(t *testing.T) {
		index, hook, store, gate := setup(t)
		reproStaleRescoreAcrossFree(t, index, hook, store, gate, vecs, victim)

		newVec := vecs[n]
		store.set(victim, newVec)
		require.NoError(t, index.Add(context.Background(), victim, newVec))

		ids, _, err := index.SearchByVector(context.Background(), newVec, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, []uint64{victim}, ids, "new owner of docID %d not returned for its own vector", victim)

		require.NoError(t, index.CleanUpTombstonedNodes(func() bool { return false }))
		assert.True(t, index.ContainsDoc(victim),
			"tombstone cleanup removed the live new owner of docID %d from the vector index", victim)
	})
}
