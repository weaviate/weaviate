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

package postingedge

import (
	"context"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// persistentFixture is the index as a shard runs it, over a store that
// stands for the one of the shard: the test owns it, closes it and opens it
// again, which is what a restart of the node does to an index.
type persistentFixture struct {
	root  string
	store *lsmkv.Store
	// flush and compaction are the cycles of the store, which nothing runs
	// in the background here: a test calls them (cycle).
	flush, compaction cyclemanager.CycleCallbackGroup
	cfg               PersistentConfig
	uc                ent.UserConfig
	index             *Persistent
	// vectors has the vector of doc id i at i, as it was given, and
	// normalized has it as the index compares it.
	vectors    [][]float32
	normalized [][]float32
	queries    [][]float32
	provider   distancer.Provider
}

const (
	persistentDims = 64
	// persistentBase is how many vectors the fixture gathers before it
	// builds its first postings.
	persistentBase = 600
)

// newPersistentFixture opens an empty index with room for n vectors of the
// fixture, which are not added. With the posting size of the tests a base of
// persistentBase vectors is four postings or so, and every posting splits
// several times on the way to a few thousand.
func newPersistentFixture(t *testing.T, n int) *persistentFixture {
	t.Helper()
	return newPersistentFixtureWith(t, n, nil)
}

// newPersistentFixtureWith is newPersistentFixture with the config of the
// index changed before it first opens.
func newPersistentFixtureWith(t *testing.T, n int, change func(cfg *PersistentConfig)) *persistentFixture {
	t.Helper()
	all := latentVectors(n+60, persistentDims, 9101)
	f := &persistentFixture{
		root:     t.TempDir(),
		vectors:  all[:n],
		queries:  all[n:],
		provider: distancer.NewCosineDistanceProvider(),
		uc:       ent.NewDefaultUserConfig(),
	}
	f.normalized = normalizedCopy(f.vectors)
	// At 64 dimensions the smallest posting size there is splits past 249
	// vectors. The automatic rescore limit of a code this short is the
	// largest there is, which every insert would pay too.
	f.uc.MaxPostingSizeKB = ent.MinimumMaxPostingSizeKB
	f.uc.RQ.RescoreLimit = 100
	f.uc.EF = 64
	f.cfg = PersistentConfig{
		ID:               "vectors_test",
		ShardName:        "shard",
		ClassName:        "Class",
		RootPath:         f.root,
		Logger:           testLogger(),
		DistanceProvider: f.provider,
		BaseSize:         persistentBase,
		Seed:             7,
	}
	if change != nil {
		change(&f.cfg)
	}
	f.open(t)
	t.Cleanup(func() {
		if f.index != nil {
			f.close(t)
		}
	})
	return f
}

// latentVectors draws vectors that have neighbors at every distance, as
// embeddings do: points of a space of a few dimensions, laid in the space of
// the vectors by a random linear map, with a little noise. The clusters the
// other tests of the package draw are the wrong corpus for an index a shard
// runs with its defaults: every vector of a tight cluster is as near as any
// other, so no vector has an edge out of its posting, and which ten of them
// are the nearest is a matter of the last decimals.
func latentVectors(n, dims int, seed uint64) [][]float32 {
	const latent = 8
	rng := rand.New(rand.NewPCG(seed, 23))
	lay := make([][]float32, dims)
	for d := range lay {
		lay[d] = make([]float32, latent)
		for j := range lay[d] {
			lay[d][j] = float32(rng.NormFloat64())
		}
	}
	vectors := make([][]float32, n)
	point := make([]float32, latent)
	for i := range vectors {
		for j := range point {
			point[j] = float32(rng.NormFloat64())
		}
		vectors[i] = make([]float32, dims)
		for d := range vectors[i] {
			var x float32
			for j, p := range point {
				x += lay[d][j] * p
			}
			vectors[i][d] = x + 0.05*float32(rng.NormFloat64())
		}
	}
	return vectors
}

func (f *persistentFixture) open(t *testing.T) {
	t.Helper()
	lsmDir := filepath.Join(f.root, "lsm")
	f.flush = cyclemanager.NewCallbackGroup("flush", testLogger(), 1)
	f.compaction = cyclemanager.NewCallbackGroup("compaction", testLogger(), 1)
	store, err := lsmkv.New(lsmDir, f.root, testLogger(), nil, nil, f.compaction,
		cyclemanager.NewCallbackGroupNoop(), f.flush)
	require.NoError(t, err)
	f.store = store
	index, err := Open(f.cfg, f.uc, store)
	require.NoError(t, err)
	f.index = index
}

func (f *persistentFixture) close(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, f.index.Flush())
	require.NoError(t, f.index.Shutdown(ctx))
	require.NoError(t, f.store.Shutdown(ctx))
	f.index, f.store = nil, nil
}

// restart closes the index and its store and opens them again from disk.
func (f *persistentFixture) restart(t *testing.T) {
	t.Helper()
	f.close(t)
	f.open(t)
}

// cycle runs the flush and the compaction of the store once, as the cycles
// of a node would in the background.
func (f *persistentFixture) cycle() {
	never := func() bool { return false }
	f.flush.CycleCallback(never)
	f.compaction.CycleCallback(never)
}

// add adds the vectors of the doc ids [from, to), in batches.
func (f *persistentFixture) add(t *testing.T, from, to int) {
	t.Helper()
	const batch = 50
	for start := from; start < to; start += batch {
		end := min(start+batch, to)
		ids := sequentialIDs(end-start, uint64(start))
		require.NoError(t, f.index.AddBatch(context.Background(), ids, f.vectors[start:end]), "docs [%d, %d)", start, end)
	}
}

// truth returns the k doc ids nearest to a query among the first n vectors
// that held gives, by brute force.
func (f *persistentFixture) truth(t *testing.T, query []float32, n, k int, held func(id uint64) bool) []uint64 {
	t.Helper()
	query = distancer.Normalize(query)
	var all []scored
	for id := range uint64(n) {
		if held != nil && !held(id) {
			continue
		}
		dist, err := f.provider.SingleDist(query, f.normalized[id])
		require.NoError(t, err)
		all = append(all, scored{id: id, dist: dist})
	}
	slices.SortFunc(all, func(a, b scored) int {
		switch {
		case worse(b, a):
			return -1
		case worse(a, b):
			return 1
		}
		return 0
	})
	ids := make([]uint64, 0, k)
	for _, s := range all[:min(k, len(all))] {
		ids = append(ids, s.id)
	}
	return ids
}

// answers returns what the index returns for every query of the fixture.
func (f *persistentFixture) answers(t *testing.T, k int) [][]uint64 {
	t.Helper()
	if !f.cfg.noMaintenance {
		// What the index does in the background is done first: its answers
		// are then those it gives until it is written to again.
		require.NoError(t, f.index.Settle(context.Background()))
	}
	out := make([][]uint64, len(f.queries))
	for i, query := range f.queries {
		ids, dists, err := f.index.SearchByVector(context.Background(), query, k, nil)
		require.NoError(t, err, "query %d", i)
		require.Len(t, dists, len(ids))
		require.True(t, slices.IsSorted(dists), "query %d: nearest first, got %v", i, dists)
		out[i] = ids
	}
	return out
}

// recall measures the answers of the index against brute force over the
// first n vectors that held gives. No answer may hold anything else.
func (f *persistentFixture) recall(t *testing.T, n, k int, held func(id uint64) bool) float64 {
	t.Helper()
	hits, total := 0, 0
	for i, ids := range f.answers(t, k) {
		for _, id := range ids {
			require.Less(t, id, uint64(n), "query %d returned a doc that was never added", i)
			if held != nil {
				require.True(t, held(id), "query %d returned doc %d, which the index does not hold", i, id)
			}
		}
		want := f.truth(t, f.queries[i], n, k, held)
		for _, id := range want {
			if slices.Contains(ids, id) {
				hits++
			}
		}
		total += len(want)
	}
	return float64(hits) / float64(total)
}

// stagingDir is where the store keeps the vectors the index holds aside of
// its base.
func (f *persistentFixture) stagingDir() string {
	return filepath.Join(f.root, "lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, f.cfg.ID))
}

// keptAside counts the vectors the store holds aside of the base. An index
// with a base holds none, and nothing of their bucket is left on disk: the
// bytes of the vectors would stay there under their tombstones.
func (f *persistentFixture) keptAside(t *testing.T) int {
	t.Helper()
	if f.index.g != nil {
		require.Nil(t, f.index.staging, "an index with a base keeps nothing aside")
		_, err := os.Stat(f.stagingDir())
		require.True(t, os.IsNotExist(err), "the bucket of the vectors kept aside is still on disk: %v", err)
		return 0
	}
	count := 0
	require.NoError(t, growScan(f.index.staging, func(_, _ []byte) error {
		count++
		return nil
	}))
	return count
}

// covered checks, once the base is built, what the tests of the index that
// grows check of it: every vector in one posting, the one the map gives it,
// with its code, its edges and their distances, all of it as the store holds
// it.
func (f *persistentFixture) covered(t *testing.T, at string) {
	t.Helper()
	// Nothing of the index runs next to the check: not what it does in the
	// background either.
	f.index.writeMu.Lock()
	defer f.index.writeMu.Unlock()
	f.index.mu.Lock()
	defer f.index.mu.Unlock()
	g := f.index.g
	require.NotNil(t, g, at)
	require.NoError(t, g.cleanCentroidGraph(), at)
	require.NoError(t, g.checkCoverage(), at)
}

// vectorsIntact checks that the index reads, for every doc it holds, the
// vector it was given, normalized.
func (f *persistentFixture) vectorsIntact(t *testing.T, at string) {
	t.Helper()
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	g := f.index.g
	require.NotNil(t, g, at)
	var raw []byte
	var vector []float32
	for docID, held := range g.live {
		if !held {
			continue
		}
		var err error
		raw, vector, err = g.exact(uint64(docID), raw, vector)
		require.NoError(t, err, "%s: doc %d", at, docID)
		require.Equal(t, f.normalized[docID], vector, "%s: the vector of doc %d", at, docID)
	}
}

// An index without vectors answers every call a shard makes of an empty
// index: it searches and finds nothing, deletes what it does not have, and
// closes and opens again as empty as it was.
func TestPersistentEmpty(t *testing.T) {
	ctx := context.Background()
	f := newPersistentFixture(t, 10)
	check := func(at string) {
		ids, dists, err := f.index.SearchByVector(ctx, f.vectors[0], 10, nil)
		require.NoError(t, err, at)
		assert.Empty(t, ids, at)
		assert.Empty(t, dists, at)
		assert.False(t, f.index.ContainsDoc(0), at)
		f.index.Iterate(func(uint64) bool {
			t.Errorf("%s: an empty index iterates over nothing", at)
			return true
		})
		require.NoError(t, f.index.Delete(0, 7, math.MaxUint32, math.MaxUint64), at)
		require.NoError(t, f.index.ValidateBeforeInsert(f.vectors[0]), at)
		require.NoError(t, f.index.Flush(), at)
		assert.Equal(t, common.IndexType(common.IndexTypePostingEdge), f.index.Type(), at)
		assert.False(t, f.index.Multivector(), at)
		assert.True(t, f.index.Compressed(), at)
	}
	check("new")
	f.restart(t)
	check("opened again")
}

// The whole life of an index in a shard: it starts empty, is searched
// exactly while it gathers its first vectors, builds its base, grows by
// inserts through splits, has a part of it deleted, is restarted, and goes
// on taking vectors. At every step it answers close to brute force over the
// vectors it holds, and never with one it does not.
func TestPersistentGrowsFromEmpty(t *testing.T) {
	const total, k = 3000, 10
	f := newPersistentFixture(t, total)
	half := persistentBase / 2

	// Before the base, every answer is exact.
	f.add(t, 0, half)
	require.Nil(t, f.index.g, "no base yet")
	require.Equal(t, half, f.keptAside(t))
	assert.Equal(t, 1.0, f.recall(t, half, k, nil), "exact while the vectors are kept aside")
	for id := range uint64(half) {
		require.True(t, f.index.ContainsDoc(id))
	}
	assert.False(t, f.index.ContainsDoc(uint64(half)))

	// The base is built by the insert that completes it.
	f.add(t, half, persistentBase)
	require.NotNil(t, f.index.g, "the base is built once there are %d vectors", persistentBase)
	f.covered(t, "the base")
	assert.Zero(t, f.keptAside(t), "nothing is kept aside once the base holds it")
	measured := func(at string, recall float64) {
		t.Helper()
		t.Logf("recall@%d, %s: %.4f", k, at, recall)
		assert.Greater(t, recall, 0.9, at)
	}
	measured("the base", f.recall(t, persistentBase, k, nil))

	// Inserts, with the store flushed and compacted on the way.
	for from := persistentBase; from < 2000; from += 350 {
		f.add(t, from, min(from+350, 2000))
		f.cycle()
	}
	f.covered(t, "grown")
	g := f.index.g
	assert.Equal(t, 2000, g.meta.Live)
	assert.Greater(t, g.meta.Splits, 3, "postings of %d vectors split on the way from %d to 2000",
		g.meta.Threshold, persistentBase)
	assert.Positive(t, g.stats.InverseEdges, "an insert enters the edges of its neighbors")
	measured("grown to 2000", f.recall(t, 2000, k, nil))
	t.Logf("%d postings given out, %d splits, %d inverse edges of %d checks",
		g.meta.Postings, g.meta.Splits, g.stats.InverseEdges, g.stats.InverseChecks)

	// A third of it deleted: none of those comes back, and the rest is found
	// as before.
	deleted := map[uint64]bool{}
	var ids []uint64
	for id := uint64(0); id < 2000; id += 3 {
		deleted[id] = true
		ids = append(ids, id)
	}
	require.NoError(t, f.index.Delete(ids...))
	held := func(id uint64) bool { return !deleted[id] }
	f.covered(t, "with deletes")
	measured("with a third deleted", f.recall(t, 2000, k, held))
	for id := range uint64(2000) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d", id)
	}

	// Restarted, it is the index it was.
	before := f.answers(t, k)
	f.restart(t)
	require.NotNil(t, f.index.g)
	f.covered(t, "restarted")
	assert.Equal(t, before, f.answers(t, k), "the same answers after a restart")
	for id := range uint64(2000) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d after a restart", id)
	}

	// And it goes on: more vectors, more deletes, another restart.
	f.add(t, 2000, total)
	for id := uint64(2000); id < total; id += 5 {
		deleted[id] = true
		require.NoError(t, f.index.Delete(id))
	}
	f.cycle()
	f.covered(t, "grown again")
	measured("grown to 3000 after a restart, more deleted", f.recall(t, total, k, held))
	before = f.answers(t, k)
	f.restart(t)
	f.covered(t, "restarted again")
	assert.Equal(t, before, f.answers(t, k), "the same answers after the second restart")

	var iterated []uint64
	f.index.Iterate(func(id uint64) bool {
		iterated = append(iterated, id)
		return true
	})
	var want []uint64
	for id := range uint64(total) {
		if held(id) {
			want = append(want, id)
		}
	}
	assert.Equal(t, want, iterated, "it iterates over what it holds")
}

// Closed and opened again at any point of its life, the index answers what
// it answered, holds what it held and goes on.
func TestPersistentRestarts(t *testing.T) {
	const k = 10
	tests := []struct {
		name string
		// added is how many vectors go in before the restart, and deleted
		// which of them are deleted before it.
		added   int
		deleted []uint64
		// built tells whether the base is built by then.
		built bool
	}{
		{name: "empty"},
		{name: "one vector", added: 1},
		{name: "gathering", added: persistentBase - 1},
		{name: "gathering, some deleted", added: persistentBase - 1, deleted: []uint64{0, 17, 598}},
		{name: "the base, just built", added: persistentBase, built: true},
		{name: "the base, some deleted", added: persistentBase, deleted: []uint64{3, 4, 5, 599}, built: true},
		{name: "grown", added: persistentBase + 700, built: true},
		{name: "grown, some deleted", added: persistentBase + 700, deleted: []uint64{1, 600, 601, 1299}, built: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total = persistentBase + 900
			f := newPersistentFixture(t, total)
			f.add(t, 0, tt.added)
			require.NoError(t, f.index.Delete(tt.deleted...))
			require.Equal(t, tt.built, f.index.g != nil)
			held := func(id uint64) bool { return id < uint64(tt.added) && !slices.Contains(tt.deleted, id) }

			before := f.answers(t, k)
			f.restart(t)
			require.Equal(t, tt.built, f.index.g != nil, "restarted")
			assert.Equal(t, before, f.answers(t, k), "the same answers")
			for id := range uint64(total) {
				require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d", id)
			}

			// It takes the rest, whatever it was doing.
			f.add(t, tt.added, total)
			require.NotNil(t, f.index.g)
			f.covered(t, "grown after the restart")
			held = func(id uint64) bool { return !slices.Contains(tt.deleted, id) }
			assert.Greater(t, f.recall(t, total, k, held), 0.9)
		})
	}
}

// A deleted vector is not held and never comes back from a search, whatever
// the index was doing when it was deleted, and deleting what is not there is
// not an error.
func TestPersistentDeletes(t *testing.T) {
	ctx := context.Background()
	const k = 10
	tests := []struct {
		name string
		// added goes in first, then deleted is deleted, then the index takes
		// vectors up to total.
		added, total int
		deleted      []uint64
	}{
		{name: "while gathering", added: 100, total: 100, deleted: []uint64{0, 50, 99}},
		{name: "while gathering, then the base", added: 400, total: persistentBase + 5, deleted: []uint64{0, 399, 123}},
		{name: "in the base", added: persistentBase, total: persistentBase, deleted: []uint64{0, 1, 2, 300}},
		{name: "in the base, then inserts", added: persistentBase, total: 1500, deleted: []uint64{10, 20, 30, 599}},
		{name: "what was never there", added: 50, total: 50, deleted: []uint64{50, 1000, math.MaxUint32, math.MaxUint64}},
		{name: "what was never there, with a base", added: persistentBase, total: persistentBase, deleted: []uint64{persistentBase, 1 << 40}},
		{name: "twice", added: persistentBase, total: persistentBase, deleted: []uint64{5, 5, 6, 5}},
		{name: "every vector of the base", added: persistentBase, total: persistentBase + 300, deleted: sequentialIDs(persistentBase, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Nothing takes the marked vectors out here: what the index does
			// with them later is in TestPersistentCleansItsDeletes.
			f := newPersistentFixtureWith(t, max(tt.total, 1), func(cfg *PersistentConfig) { cfg.noMaintenance = true })
			f.add(t, 0, tt.added)
			require.NoError(t, f.index.Delete(tt.deleted...))
			require.NoError(t, f.index.Delete(tt.deleted...), "deleted again")
			f.add(t, tt.added, tt.total)

			held := func(id uint64) bool { return id < uint64(tt.total) && !slices.Contains(tt.deleted, id) }
			for _, id := range tt.deleted {
				require.False(t, f.index.ContainsDoc(id), "doc %d", id)
			}
			live := 0
			for id := range uint64(tt.total) {
				if held(id) {
					live++
				}
			}
			for i, ids := range f.answers(t, k) {
				for _, id := range ids {
					require.True(t, held(id), "query %d returned doc %d", i, id)
				}
				require.Len(t, ids, min(k, live), "query %d", i)
			}
			if f.index.g != nil {
				f.covered(t, "after the deletes")
			}

			// A deleted vector does not come back by adding it again: while
			// it is marked in its posting, its doc id is taken.
			for _, id := range tt.deleted {
				if id >= uint64(tt.added) {
					continue
				}
				g := f.index.g
				inPosting := g != nil && int(id) < len(g.live) && g.live[id]
				err := f.index.Add(ctx, id, f.vectors[id])
				if inPosting {
					require.ErrorIs(t, err, ErrNotSupported, "doc %d is still in its posting", id)
					continue
				}
				// Deleted while it was kept aside, nothing led to it: it went
				// for good, and its doc id is free.
				require.NoError(t, err, "doc %d, deleted while it was kept aside", id)
				require.NoError(t, f.index.Delete(id))
			}
		})
	}
}

// What the index does not do is an error that says so, and never an answer
// that leaves something out without a word.
func TestPersistentUnsupported(t *testing.T) {
	ctx := context.Background()
	f := newPersistentFixture(t, persistentBase+50)
	query := f.vectors[0]
	allow := helpers.NewAllowList(1, 2, 3)

	calls := []struct {
		name string
		call func() error
	}{
		{"a filtered search", func() error {
			_, _, err := f.index.SearchByVector(ctx, query, 10, allow)
			return err
		}},
		{"a search by distance", func() error {
			_, _, err := f.index.SearchByVectorDistance(ctx, query, 0.2, 100, nil)
			return err
		}},
		{"a backup", func() error { return f.index.PrepareForBackup(ctx) }},
		{"the files of a backup", func() error {
			_, err := f.index.ListFiles(ctx, f.root)
			return err
		}},
		{"the snapshot of a backup", func() error {
			_, err := f.index.SnapshotMutableFiles(ctx, f.root, t.TempDir())
			return err
		}},
	}
	stages := []struct {
		name string
		to   int
	}{
		{"empty", 0},
		{"gathering", 10},
		{"with a base", persistentBase + 50},
	}
	added := 0
	for _, stage := range stages {
		f.add(t, added, stage.to)
		added = stage.to
		for _, c := range calls {
			t.Run(stage.name+"/"+c.name, func(t *testing.T) {
				require.ErrorIs(t, c.call(), ErrNotSupported)
			})
		}
	}

	t.Run("a distance other than cosine", func(t *testing.T) {
		for _, provider := range []distancer.Provider{
			distancer.NewL2SquaredProvider(), distancer.NewDotProductProvider(),
			distancer.NewManhattanProvider(), distancer.NewHammingProvider(),
		} {
			cfg := f.cfg
			cfg.ID, cfg.DistanceProvider = "other", provider
			_, err := Open(cfg, f.uc, f.store)
			require.ErrorIs(t, err, ErrNotSupported, provider.Type())
		}
	})
}

// What an index cannot take it refuses whole, before it changes anything.
func TestPersistentRejects(t *testing.T) {
	ctx := context.Background()
	notFinite := slices.Clone(clusteredVectors(1, persistentDims, 1, 1)[0])
	notFinite[3] = float32(math.NaN())
	tests := []struct {
		name string
		// with is how many vectors the index holds.
		with int
		call func(f *persistentFixture) error
		want error
	}{
		{name: "another length", with: 5, want: common.ErrWrongDimensions, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, 100, make([]float32, persistentDims+1))
		}},
		{name: "another length, with a base", with: persistentBase, want: common.ErrWrongDimensions, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, 1000, []float32{1, 2, 3})
		}},
		{name: "a query of another length", with: 5, want: common.ErrWrongDimensions, call: func(f *persistentFixture) error {
			_, _, err := f.index.SearchByVector(ctx, []float32{1, 2}, 3, nil)
			return err
		}},
		{name: "a query of another length, with a base", with: persistentBase, want: common.ErrWrongDimensions, call: func(f *persistentFixture) error {
			_, _, err := f.index.SearchByVector(ctx, []float32{1, 2}, 3, nil)
			return err
		}},
		{name: "a vector that is not finite", with: 5, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, 100, notFinite)
		}},
		{name: "a query that is not finite", with: persistentBase, call: func(f *persistentFixture) error {
			_, _, err := f.index.SearchByVector(ctx, notFinite, 3, nil)
			return err
		}},
		{name: "a doc id it holds", with: 5, want: ErrNotSupported, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, 3, f.vectors[3])
		}},
		{name: "a doc id it holds, with a base", with: persistentBase, want: ErrNotSupported, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, 3, f.vectors[3])
		}},
		{name: "a doc id twice in a batch", with: 5, want: ErrNotSupported, call: func(f *persistentFixture) error {
			return f.index.AddBatch(ctx, []uint64{100, 101, 100}, f.vectors[:3])
		}},
		{name: "a doc id past what it addresses", with: 5, call: func(f *persistentFixture) error {
			return f.index.Add(ctx, math.MaxUint32, f.vectors[0])
		}},
		{name: "ids and vectors of different lengths", with: 5, call: func(f *persistentFixture) error {
			return f.index.AddBatch(ctx, []uint64{100, 101}, f.vectors[:3])
		}},
		{name: "an empty batch", with: 5, call: func(f *persistentFixture) error {
			return f.index.AddBatch(ctx, nil, nil)
		}},
		{name: "no results asked for", with: 5, call: func(f *persistentFixture) error {
			_, _, err := f.index.SearchByVector(ctx, f.vectors[0], 0, nil)
			return err
		}},
		{name: "a batch whose last vector is of another length", with: persistentBase, want: common.ErrWrongDimensions, call: func(f *persistentFixture) error {
			return f.index.AddBatch(ctx, []uint64{1000, 1001}, [][]float32{f.vectors[0], {1}})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPersistentFixture(t, persistentBase)
			f.add(t, 0, tt.with)
			before := f.answers(t, 5)

			err := tt.call(f)
			require.Error(t, err)
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			}
			assert.Equal(t, before, f.answers(t, 5), "a call that is refused changes nothing")
			for _, id := range []uint64{100, 101, 1000, 1001, math.MaxUint32} {
				if id >= uint64(tt.with) {
					assert.False(t, f.index.ContainsDoc(id), "doc %d", id)
				}
			}
			if f.index.g != nil {
				f.covered(t, "after the refusal")
			}
		})
	}
}

// A shard gives out doc ids without bound, and far apart when its objects
// are updated: the index takes them as they come.
func TestPersistentSparseDocIDs(t *testing.T) {
	ctx := context.Background()
	const k = 10
	f := newPersistentFixture(t, persistentBase+400)
	// The doc id of vector i, far past the room the base was given.
	docID := func(i int) uint64 {
		if i < persistentBase {
			return uint64(3 * i)
		}
		return uint64(persistentBase*3 + (i-persistentBase)*997)
	}
	for i, vector := range f.vectors {
		require.NoError(t, f.index.Add(ctx, docID(i), vector), "vector %d as doc %d", i, docID(i))
	}
	f.covered(t, "grown")
	check := func(at string) {
		hits, total := 0, 0
		for qi, query := range f.queries {
			ids, _, err := f.index.SearchByVector(ctx, query, k, nil)
			require.NoError(t, err)
			want := f.truth(t, query, len(f.vectors), k, nil)
			for _, i := range want {
				if slices.Contains(ids, docID(int(i))) {
					hits++
				}
			}
			total += len(want)
			for _, id := range ids {
				require.True(t, f.index.ContainsDoc(id), "%s: query %d returned doc %d", at, qi, id)
			}
		}
		assert.Greater(t, float64(hits)/float64(total), 0.9, at)
		assert.False(t, f.index.ContainsDoc(1), at)
		assert.True(t, f.index.ContainsDoc(docID(len(f.vectors)-1)), at)
	}
	check("grown")
	f.restart(t)
	f.covered(t, "restarted")
	check("restarted")
	assert.GreaterOrEqual(t, f.index.g.meta.Keys, int(docID(len(f.vectors)-1))+1)
}

// Searches run while the index is written to: they see the index before a
// write or after it, never in the middle, and nothing a delete has returned
// from comes back.
func TestPersistentSearchesNextToWrites(t *testing.T) {
	ctx := context.Background()
	const total = persistentBase + 600
	f := newPersistentFixture(t, total)
	f.add(t, 0, persistentBase/2)

	// gone holds the doc ids whose delete has returned.
	var gone sync.Map
	var stop atomic.Bool
	var searches atomic.Int64
	eg := enterrors.NewErrorGroupWrapper(testLogger())
	for worker := range 4 {
		eg.Go(func() error {
			for i := worker; !stop.Load(); i++ {
				// What was gone before the search started cannot be in what
				// it returns.
				before := map[uint64]bool{}
				gone.Range(func(id, _ any) bool {
					before[id.(uint64)] = true
					return true
				})
				ids, _, err := f.index.SearchByVector(ctx, f.queries[i%len(f.queries)], 10, nil)
				if err != nil {
					return errors.Wrap(err, "search")
				}
				for _, id := range ids {
					if before[id] {
						return errors.Errorf("a search returned doc %d, which was deleted before it started", id)
					}
				}
				searches.Add(1)
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer stop.Store(true)
		for from := persistentBase / 2; from < total; from += 20 {
			to := min(from+20, total)
			if err := f.index.AddBatch(ctx, sequentialIDs(to-from, uint64(from)), f.vectors[from:to]); err != nil {
				return errors.Wrapf(err, "add docs [%d, %d)", from, to)
			}
			// One of every batch goes, and one that came long before.
			for _, id := range []uint64{uint64(from), uint64(from / 7)} {
				if err := f.index.Delete(id); err != nil {
					return errors.Wrapf(err, "delete doc %d", id)
				}
				gone.Store(id, true)
			}
			if err := f.index.Flush(); err != nil {
				return errors.Wrap(err, "flush")
			}
			f.cycle()
		}
		return nil
	})
	require.NoError(t, eg.Wait())
	assert.Positive(t, searches.Load())

	held := func(id uint64) bool {
		_, deleted := gone.Load(id)
		return !deleted
	}
	f.covered(t, "after the writes")
	assert.Greater(t, f.recall(t, total, 10, held), 0.9)
}

// The reads of a search come out of the budget of its query. With a budget
// that does not reach to a round, the round is read in turns and the search
// answers the same.
func TestPersistentSearchWithinBudget(t *testing.T) {
	const total = persistentBase + 600
	f := newPersistentFixture(t, total)
	f.add(t, 0, total)

	want := f.answers(t, 10)
	for _, budget := range []int{1, 2, 3, 8, 64} {
		ctx := concurrency.CtxWithBudget(context.Background(), budget)
		for i, query := range f.queries {
			ids, _, err := f.index.SearchByVector(ctx, query, 10, nil)
			require.NoError(t, err, "budget %d, query %d", budget, i)
			require.Equal(t, want[i], ids, "budget %d, query %d", budget, i)
		}
	}
}

// The config of a class reaches the index, and what a search reads on every
// query can change while it runs. What shaped what is on disk cannot.
func TestPersistentUserConfig(t *testing.T) {
	f := newPersistentFixture(t, persistentBase+20)
	f.add(t, 0, persistentBase+20)
	g := f.index.g
	require.Equal(t, int64(f.uc.EF), g.ix.ef.Load())
	require.Equal(t, int64(f.uc.RQ.RescoreLimit), g.ix.rescoreLimit.Load())
	require.Equal(t, f.uc.EFConstruction, g.insertEF)
	require.Equal(t, splitThreshold(f.uc.MaxPostingSizeKB, persistentDims), g.meta.Threshold)

	change := func(edit func(*ent.UserConfig)) ent.UserConfig {
		uc := f.uc
		edit(&uc)
		return uc
	}
	tests := []struct {
		name    string
		updated ent.UserConfig
		wantErr string
	}{
		{name: "ef", updated: change(func(u *ent.UserConfig) { u.EF = 200 })},
		{name: "the rescore limit", updated: change(func(u *ent.UserConfig) { u.RQ.RescoreLimit = 300 })},
		{name: "the rescore limit, automatic", updated: change(func(u *ent.UserConfig) { u.RQ.RescoreLimit = ent.RescoreLimitAuto })},
		{name: "efConstruction", updated: change(func(u *ent.UserConfig) { u.EFConstruction = 64 }), wantErr: "efConstruction is immutable"},
		{name: "the posting size", updated: change(func(u *ent.UserConfig) { u.MaxPostingSizeKB = 0 }), wantErr: "maxPostingSizeKB is immutable"},
		{name: "the distance", updated: change(func(u *ent.UserConfig) { u.Distance = "l2-squared" }), wantErr: "unsupported distance"},
		{name: "an ef of nothing", updated: change(func(u *ent.UserConfig) { u.EF = 0 }), wantErr: "ef is 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := f.index.uc
			called := false
			err := f.index.UpdateUserConfig(tt.updated, func() { called = true })
			require.True(t, called, "the callback runs whatever happens")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, before, f.index.uc, "a config that is refused changes nothing")
				return
			}
			require.NoError(t, err)
			wantRescore := tt.updated.RQ.RescoreLimit
			if wantRescore == ent.RescoreLimitAuto {
				wantRescore = autoRescoreLimit(64)
			}
			assert.Equal(t, int64(tt.updated.EF), g.ix.ef.Load())
			assert.Equal(t, int64(wantRescore), g.ix.rescoreLimit.Load())
			_, _, err = f.index.SearchByVector(context.Background(), f.queries[0], 10, nil)
			require.NoError(t, err)
		})
	}

	t.Run("a config of another index", func(t *testing.T) {
		err := f.index.UpdateUserConfig(NewDefaultUserConfig(), func() {})
		require.ErrorContains(t, err, "not a posting-edge UserConfig")
	})
}

func TestAutoRescoreLimit(t *testing.T) {
	tests := []struct {
		scanBits, want int
	}{
		// The row of the plan, "El valor automático de rq.rescoreLimit".
		{384, 1600},
		{512, 675},
		{768, 200},
		{1024, 85},
		{1536, 64},
		{3072, 64},
		// Under 384 bits the rule has no measurement behind it, and stops.
		{64, 1600},
		{256, 1600},
		// While there is no code yet.
		{0, 64},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, autoRescoreLimit(tt.scanBits), "%d bits", tt.scanBits)
	}
}

func TestSplitThreshold(t *testing.T) {
	tests := []struct {
		name                         string
		maxPostingSizeKB, dims, want int
	}{
		{"automatic, at any dimension", 0, 64, 360},
		{"automatic, at 1536d", 0, 1536, 360},
		{"automatic, at 3072d", 0, 3072, 360},
		// The table of the plan, "Comprobación 1": what HFresh gives.
		{"the 48 of HFresh at 1536d", 48, 1536, 227},
		{"the 48 of HFresh at 768d", 48, 768, 407},
		{"76 at 1536d, the mean of 236", 76, 1536, 359},
		{"77 at 1536d", 77, 1536, 364},
		{"76 at 384d", 76, 384, 1067},
		{"76 at 3072d, on the floor", 76, 3072, 192},
		{"the smallest at 64d", 8, 64, 249},
		{"the smallest at 1536d, on the floor", 8, 1536, 192},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, splitThreshold(tt.maxPostingSizeKB, tt.dims))
		})
	}
}

// The buckets the index opens in the store of its shard are the ones a shard
// removes when it drops the index, by name, without opening it: the two
// lists cannot drift apart.
func TestPersistentBuckets(t *testing.T) {
	ctx := context.Background()
	f := newPersistentFixture(t, persistentBase+10)
	f.add(t, 0, persistentBase+10)
	require.NoError(t, f.index.Delete(3))
	f.cycle()

	// Every bucket the index can have is in the list a shard removes by. With
	// its base built it has them all but the one of the vectors it kept aside
	// before, which went with them.
	everyName := helpers.PostingEdgeBucketNames(f.cfg.ID)
	staging := helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, f.cfg.ID)
	require.Contains(t, everyName, staging)
	listed := slices.DeleteFunc(slices.Clone(everyName), func(name string) bool { return name == staging })
	inStore := func() []string {
		entries, err := os.ReadDir(filepath.Join(f.root, "lsm"))
		require.NoError(t, err)
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		return names
	}
	assert.ElementsMatch(t, listed, inStore(), "the buckets of the index in the store")
	for name := range f.store.GetBucketsByName() {
		assert.Contains(t, listed, name)
	}
	_, err := os.Stat(filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID), diskMetaFile))
	require.NoError(t, err)

	t.Run("closed, its buckets leave the store", func(t *testing.T) {
		require.NoError(t, f.index.Shutdown(ctx))
		assert.Empty(t, f.store.GetBucketsByName())
		assert.ElementsMatch(t, listed, inStore(), "and stay on disk")
		index, err := Open(f.cfg, f.uc, f.store)
		require.NoError(t, err)
		f.index = index
		assert.True(t, f.index.ContainsDoc(0))
		assert.False(t, f.index.ContainsDoc(3))
	})

	t.Run("dropped, nothing of it is left", func(t *testing.T) {
		require.NoError(t, f.index.Drop(ctx, false))
		assert.Empty(t, f.store.GetBucketsByName())
		assert.Empty(t, inStore())
		entries, err := os.ReadDir(f.root)
		require.NoError(t, err)
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		assert.Equal(t, []string{"lsm"}, names)

		// And an index of the same id starts empty.
		index, err := Open(f.cfg, f.uc, f.store)
		require.NoError(t, err)
		f.index = index
		assert.False(t, f.index.ContainsDoc(0))
		ids, _, err := f.index.SearchByVector(ctx, f.queries[0], 10, nil)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

// A base is the index only once its meta is written. Whatever a build left
// before that is removed when the index opens, with the vectors it was
// built from still kept aside; and what is still kept aside of a base that
// was written whole is let go.
func TestPersistentInterruptedBase(t *testing.T) {
	const k = 10

	t.Run("a base without a meta", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase)
		f.add(t, 0, persistentBase-1)
		before := f.answers(t, k)
		f.close(t)

		// What a build leaves when it stops before the meta: files of the
		// index, a bucket of it with something in it, and its build
		// directory.
		dir := filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID))
		require.NoError(t, os.WriteFile(filepath.Join(dir, diskMeanFile), []byte("half a mean"), 0o644))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, diskGraphDir), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(f.root, helpers.PostingEdgeBuildDirName(f.cfg.ID), "x"), 0o755))
		bucket := filepath.Join(f.root, "lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketMap, f.cfg.ID))
		require.NoError(t, os.MkdirAll(bucket, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(bucket, "leftover"), []byte("x"), 0o644))

		f.open(t)
		require.Nil(t, f.index.g, "still gathering")
		assert.Equal(t, before, f.answers(t, k))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries)
		_, err = os.Stat(bucket)
		assert.True(t, os.IsNotExist(err), "the bucket of the base that was not written whole")
		_, err = os.Stat(filepath.Join(f.root, helpers.PostingEdgeBuildDirName(f.cfg.ID)))
		assert.True(t, os.IsNotExist(err), "the directory of the build")

		// And the next vector builds the base.
		f.add(t, persistentBase-1, persistentBase)
		require.NotNil(t, f.index.g)
		f.covered(t, "the base")
		assert.Greater(t, f.recall(t, persistentBase, k, nil), 0.9)
	})

	t.Run("a base with its vectors still kept aside", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase+100)
		f.add(t, 0, persistentBase)
		require.NotNil(t, f.index.g)
		// The build stopped between writing the meta and letting go of the
		// vectors: they are in the base and their bucket is still there.
		name := helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, f.cfg.ID)
		require.NoError(t, f.store.CreateOrLoadBucket(context.Background(), name, lsmkv.WithStrategy(lsmkv.StrategyReplace)))
		for id := range uint64(10) {
			raw := make([]byte, 4*persistentDims)
			for i, x := range f.normalized[id] {
				putFloat32(raw[4*i:], x)
			}
			require.NoError(t, f.store.Bucket(name).Put(growDocKey(id), raw))
		}
		before := f.answers(t, k)
		f.restart(t)
		_, err := os.Stat(f.stagingDir())
		require.True(t, os.IsNotExist(err), "the bucket of the vectors kept aside is removed when the index opens")

		require.NotNil(t, f.index.g)
		assert.Zero(t, f.keptAside(t))
		assert.Empty(t, f.index.staged)
		assert.Equal(t, before, f.answers(t, k))
		f.add(t, persistentBase, persistentBase+100)
		f.covered(t, "grown")
	})
}

// A write that fails halfway leaves the index stopped: it says so to every
// call from then on, rather than serve or add to something that is not known
// to be an index.
func TestPersistentStopsAfterAFailedWrite(t *testing.T) {
	ctx := context.Background()
	f := newPersistentFixture(t, persistentBase+10)
	f.add(t, 0, persistentBase+5)

	// The file of the vectors can no longer be written to: the next insert
	// finds its edges, writes its member and its edges, and fails writing
	// its vector.
	g := f.index.g
	readOnly, err := os.Open(filepath.Join(g.dir, growVectorsName(g.meta.VectorGen)))
	require.NoError(t, err)
	require.NoError(t, g.vectors.Close())
	g.vectors = readOnly
	err = f.index.Add(ctx, persistentBase+5, f.vectors[persistentBase+5])
	require.Error(t, err)
	require.NotContains(t, err.Error(), "stopped", "the insert that failed says what failed")

	stopped := "posting-edge index stopped"
	_, _, err = f.index.SearchByVector(ctx, f.queries[0], 10, nil)
	require.ErrorContains(t, err, stopped)
	require.ErrorContains(t, f.index.Add(ctx, persistentBase+6, f.vectors[persistentBase+6]), stopped)
	require.ErrorContains(t, f.index.Delete(0), stopped)
	require.NoError(t, f.index.Flush(), "a flush writes nothing of an index that stopped")

	// It closes, releasing what it holds, and writes nothing down as the
	// index: the meta is the one of the last flush.
	require.NoError(t, f.index.Shutdown(ctx))
	require.NoError(t, f.store.Shutdown(ctx))
	f.index, f.store = nil, nil
}

// crashed opens a copy of the files of the index as they are now, with the
// index and its store still open: what a process that dies at this point
// leaves on disk, as far as it has been handed to the system.
func (f *persistentFixture) crashed(t *testing.T) *persistentFixture {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS(f.root)))
	c := &persistentFixture{
		root: root, cfg: f.cfg, uc: f.uc, vectors: f.vectors, normalized: f.normalized, queries: f.queries,
		provider: f.provider,
	}
	c.cfg.RootPath = root
	c.open(t)
	t.Cleanup(func() {
		if c.index != nil {
			c.close(t)
		}
	})
	return c
}

// What a flush hands to the system is what a crash leaves: an index opened
// from a copy of its files taken right after one, with nothing closed, is
// the index it was, and goes on. A shard flushes its indexes after every
// request that writes. This says nothing of a crash in the middle of an
// insert, nor of a machine that loses what the system had not written yet.
func TestPersistentOpensWhatAFlushLeft(t *testing.T) {
	const k = 10
	tests := []struct {
		name    string
		added   int
		deleted []uint64
	}{
		{name: "gathering", added: 300, deleted: []uint64{7, 299}},
		{name: "the base, just built", added: persistentBase},
		{name: "grown, with deletes", added: persistentBase + 700, deleted: []uint64{0, 599, 600, 1299}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total = persistentBase + 900
			f := newPersistentFixture(t, total)
			f.add(t, 0, tt.added)
			require.NoError(t, f.index.Delete(tt.deleted...))
			require.NoError(t, f.index.Flush())
			before := f.answers(t, k)

			c := f.crashed(t)
			require.Equal(t, f.index.g != nil, c.index.g != nil)
			assert.Equal(t, before, c.answers(t, k), "the same answers")
			held := func(id uint64) bool { return id < uint64(tt.added) && !slices.Contains(tt.deleted, id) }
			for id := range uint64(total) {
				require.Equal(t, held(id), c.index.ContainsDoc(id), "doc %d", id)
			}
			if c.index.g != nil {
				c.covered(t, "opened from what the flush left")
			}

			c.add(t, tt.added, total)
			c.covered(t, "grown from there")
			held = func(id uint64) bool { return !slices.Contains(tt.deleted, id) }
			assert.Greater(t, c.recall(t, total, k, held), 0.9)
		})
	}
}
