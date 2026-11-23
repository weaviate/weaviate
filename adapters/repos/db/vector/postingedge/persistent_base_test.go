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
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// baseSteps stops the build of the base of an index at every point of it,
// for a test to do what it wants meanwhile, until the test lets it run.
type baseSteps struct {
	stopping atomic.Bool
	reached  chan string
	proceed  chan struct{}
}

func newBaseSteps() *baseSteps {
	s := &baseSteps{reached: make(chan string), proceed: make(chan struct{})}
	s.stopping.Store(true)
	return s
}

func (s *baseSteps) hook(point string) error {
	if !s.stopping.Load() {
		return nil
	}
	s.reached <- point
	<-s.proceed
	return nil
}

// at waits for the build to reach a point.
func (s *baseSteps) at(t *testing.T, point string) {
	t.Helper()
	select {
	case reached := <-s.reached:
		require.Equal(t, point, reached)
	case <-time.After(2 * time.Minute):
		require.FailNow(t, "the build of the base did not get to where "+point)
	}
}

// expect checks an index against the doc ids it must hold: of the first n
// it holds those the given function tells and no other, it is covered, reads
// every vector as it was given, returns none it does not hold and finds
// what an exact search does.
func (f *persistentFixture) expect(t *testing.T, at string, n int, held func(id uint64) bool) {
	t.Helper()
	const k = 10
	ctx := context.Background()
	for id := range uint64(len(f.vectors)) {
		require.Equal(t, id < uint64(n) && held(id), f.index.ContainsDoc(id), "%s: doc %d", at, id)
	}
	f.covered(t, at)
	f.vectorsIntact(t, at)
	for q, query := range f.queries {
		ids, _, err := f.index.SearchByVector(ctx, query, k, nil)
		require.NoError(t, err, "%s: query %d", at, q)
		for _, id := range ids {
			require.True(t, id < uint64(n) && held(id), "%s: query %d returned doc %d, which is not held", at, q, id)
		}
	}
	assert.Greater(t, f.recall(t, n, k, held), 0.9, at)
}

// The base of the index is built in the background. The insert that brings
// the vector that completes it returns as any other; while the base is built
// the vectors that come are kept aside as those before them, the searches
// read every vector kept aside, and a vector that is deleted is gone at once,
// whether the base that is being built holds it or not. When the base is the
// index, the vectors that came meanwhile are found next to it, exactly,
// until the index has taken them all. And a process that dies at any point
// of it leaves an index that, opened, ends up holding the same vectors.
func TestPersistentBuildsItsBaseInTheBackground(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const base, total = persistentBase, persistentBase + 400
	steps := newBaseSteps()
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.baseHook = steps.hook })
	add := func(from, to int) {
		t.Helper()
		for start := from; start < to; start += 50 {
			end := min(start+50, to)
			require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(end-start, uint64(start)), f.vectors[start:end]))
		}
	}
	deleted := map[uint64]bool{}
	held := func(id uint64) bool { return !deleted[id] }
	remove := func(ids ...uint64) {
		t.Helper()
		require.NoError(t, f.index.Delete(ids...))
		for _, id := range ids {
			deleted[id] = true
		}
	}
	// exact checks that the index, which has no base or has not taken every
	// vector yet, answers as an exact search of what it holds does.
	exact := func(at string, n int) {
		t.Helper()
		for q, query := range f.queries {
			ids, _, err := f.index.SearchByVector(ctx, query, k, nil)
			require.NoError(t, err, "%s: query %d", at, q)
			require.Equal(t, f.truth(t, query, n, k, held), ids, "%s: query %d", at, q)
		}
	}
	// crash copies the files as they are, after a flush if asked: what a
	// process that died now would leave.
	type crash struct {
		root, at string
		n        int
		deleted  map[uint64]bool
	}
	var crashes []crash
	crashNow := func(at string, n int, flush bool) {
		t.Helper()
		if flush {
			require.NoError(t, f.index.Flush())
		}
		root := t.TempDir()
		require.NoError(t, os.CopyFS(root, os.DirFS(f.root)))
		crashes = append(crashes, crash{root: root, at: at, n: n, deleted: mapsClone(deleted)})
	}

	// The vectors of the base: the call that brings the last returns, and
	// the base is not built yet.
	add(0, base)
	steps.at(t, "the vectors of the base are read")
	f.index.mu.RLock()
	require.Nil(t, f.index.g, "the base is being built")
	f.index.mu.RUnlock()
	add(base, base+150)
	remove(5, 17, base+3)
	// The base that is being built may hold a vector that was deleted
	// since: its doc id is not given again until the base is the index.
	require.ErrorIs(t, f.index.Add(ctx, 5, f.vectors[5]), ErrNotSupported)
	exact("while the base is built", base+150)
	crashNow("while the base is built", base+150, true)
	steps.proceed <- struct{}{}

	steps.at(t, "the base is written")
	add(base+150, base+250)
	remove(29)
	exact("with the base written", base+250)
	crashNow("with the base written", base+250, true)
	steps.proceed <- struct{}{}

	// The base becomes the index with nobody else in, and then the index
	// takes the vectors that were kept aside meanwhile, one by one.
	steps.at(t, "the base is the index")
	steps.proceed <- struct{}{}
	for taken := 1; taken <= 5; taken++ {
		steps.at(t, "a vector kept aside is in the index")
		if taken == 5 {
			crashNow("with five of the vectors kept aside taken", base+250, false)
			steps.stopping.Store(false)
		}
		steps.proceed <- struct{}{}
	}
	// Next to the vectors that are still aside, with the index taking them,
	// lowest doc id first: a search for one of the last finds it, whether it
	// is still aside or the index has it. One of them is deleted before the
	// index takes it, or after.
	remove(base + 240)
	for id := base + 249; id > base+229; id-- {
		if id == base+240 {
			continue
		}
		ids, _, err := f.index.SearchByVector(ctx, f.vectors[id], 1, nil)
		require.NoError(t, err, "doc %d", id)
		require.Equal(t, []uint64{uint64(id)}, ids, "a search for the vector of doc %d", id)
	}
	exactAside := 0
	for q, query := range f.queries {
		f.index.mu.RLock()
		aside := len(f.index.staged)
		f.index.mu.RUnlock()
		ids, _, err := f.index.SearchByVector(ctx, query, k, nil)
		require.NoError(t, err, "query %d", q)
		for _, id := range ids {
			require.True(t, held(id), "query %d returned doc %d, which was deleted", q, id)
		}
		if aside > 0 {
			exactAside++
		}
	}
	t.Logf("%d of %d queries were answered with vectors still aside", exactAside, len(f.queries))
	add(base+250, total)
	require.NoError(t, f.index.settleBase(ctx))

	f.index.mu.RLock()
	require.NotNil(t, f.index.g)
	require.Empty(t, f.index.staged, "every vector kept aside was taken")
	require.Nil(t, f.index.staging)
	f.index.mu.RUnlock()
	_, err := os.Stat(filepath.Join(f.root, "lsm", f.index.stagingName()))
	require.True(t, os.IsNotExist(err), "the bucket of the vectors kept aside is gone")
	f.expect(t, "at the end", total, held)
	before := f.answers(t, k)
	f.restart(t)
	require.Equal(t, before, f.answers(t, k), "the same answers")
	f.expect(t, "opened again", total, held)

	for _, c := range crashes {
		opened := f.openedAt(t, c.root)
		require.NoError(t, opened.index.settleBase(ctx), c.at)
		opened.expect(t, "a crash "+c.at, c.n, func(id uint64) bool { return !c.deleted[id] })
		// And it goes on from there.
		if c.n < total {
			require.NoError(t, opened.index.AddBatch(ctx, sequentialIDs(total-c.n, uint64(c.n)), f.vectors[c.n:total]), c.at)
			require.NoError(t, opened.index.settleBase(ctx), c.at)
			opened.expect(t, "a crash "+c.at+", with the rest added", total, func(id uint64) bool { return !c.deleted[id] })
		}
		opened.close(t)
	}
}

// A build of the base that fails leaves the vectors aside, searched as
// before, and those deleted while it ran gone; the base is built again once
// more vectors have come, and not before.
func TestPersistentBuildsItsBaseAgainAfterAFailure(t *testing.T) {
	ctx := context.Background()
	const base, total = persistentBase, persistentBase + 200
	var failing atomic.Bool
	failing.Store(true)
	reached, proceed := make(chan struct{}), make(chan struct{})
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) {
		cfg.baseHook = func(point string) error {
			if point != "the vectors of the base are read" || !failing.Load() {
				return nil
			}
			reached <- struct{}{}
			<-proceed
			return errors.New("the build is made to fail")
		}
	})
	for start := 0; start < base; start += 50 {
		require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(50, uint64(start)), f.vectors[start:start+50]))
	}
	<-reached
	require.NoError(t, f.index.Delete(7, 11))
	proceed <- struct{}{}
	// The build ends, failed, and nothing builds the base again by itself.
	f.index.baseMu.Lock()
	f.index.baseMu.Unlock() //nolint:staticcheck // waits for the build that is running
	require.NoError(t, f.index.settleBase(ctx))
	held := func(id uint64) bool { return id != 7 && id != 11 }
	f.index.mu.RLock()
	require.Nil(t, f.index.g, "no base")
	require.Len(t, f.index.staged, base-2)
	require.Empty(t, f.index.gone)
	f.index.mu.RUnlock()
	for q, query := range f.queries {
		ids, _, err := f.index.SearchByVector(ctx, query, 10, nil)
		require.NoError(t, err)
		require.Equal(t, f.truth(t, query, base, 10, held), ids, "query %d", q)
	}

	// More vectors, and a build that does not fail.
	failing.Store(false)
	f.add(t, base, total)
	f.index.mu.RLock()
	require.NotNil(t, f.index.g, "the base was built")
	f.index.mu.RUnlock()
	f.expect(t, "with its base", total, held)
	f.restart(t)
	f.expect(t, "opened again", total, held)
}

// An index that is shut down while its base is built keeps every vector
// aside, with what was deleted meanwhile gone: opened, it builds its base.
func TestPersistentShutsDownWhileItsBaseIsBuilt(t *testing.T) {
	ctx := context.Background()
	const base, total = persistentBase, persistentBase + 100
	steps := newBaseSteps()
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.baseHook = steps.hook })
	for start := 0; start < base; start += 50 {
		require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(50, uint64(start)), f.vectors[start:start+50]))
	}
	steps.at(t, "the vectors of the base are read")
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(40, base), f.vectors[base:base+40]))
	require.NoError(t, f.index.Delete(9, base+1))
	require.NoError(t, f.index.Flush())
	// The shutdown waits for the build, which is told to stop and goes on
	// to wherever it notices.
	closed := make(chan error)
	enterrors.GoWrapper(func() { closed <- f.index.Shutdown(ctx) }, testLogger())
	steps.stopping.Store(false)
	steps.proceed <- struct{}{}
	require.NoError(t, <-closed)
	require.NoError(t, f.store.Shutdown(ctx))
	f.index, f.store = nil, nil

	held := func(id uint64) bool { return id != 9 && id != base+1 }
	f.cfg.baseHook = nil
	f.open(t)
	require.NoError(t, f.index.settleBase(ctx))
	f.expect(t, "opened again", base+40, held)
	f.add(t, base+40, total)
	f.expect(t, "with more added", total, held)
}

// Settle waits for a base that is being built, and for the index to take
// what was kept aside meanwhile: whoever calls it gets an index with its
// base. It used to return at once while the base was being built, since no
// base was due.
func TestPersistentSettleWaitsForItsBase(t *testing.T) {
	ctx := context.Background()
	const base, total = persistentBase, persistentBase + 60
	steps := newBaseSteps()
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.baseHook = steps.hook })
	for start := 0; start < base; start += 50 {
		require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(50, uint64(start)), f.vectors[start:start+50]))
	}
	steps.at(t, "the vectors of the base are read")
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(60, base), f.vectors[base:total]))
	settled := make(chan error, 1)
	enterrors.GoWrapper(func() { settled <- f.index.Settle(ctx) }, testLogger())
	var early bool
	select {
	case <-settled:
		early = true
	case <-time.After(300 * time.Millisecond):
	}
	// The build goes on whatever Settle did: the index cannot be closed
	// with it stopped.
	steps.stopping.Store(false)
	steps.proceed <- struct{}{}
	require.False(t, early, "Settle returned while the base was being built")
	require.NoError(t, <-settled)
	f.index.mu.RLock()
	require.NotNil(t, f.index.g, "the base is built")
	require.Empty(t, f.index.staged, "and nothing is left aside")
	f.index.mu.RUnlock()
	f.expect(t, "settled", total, func(uint64) bool { return true })
}

func mapsClone(m map[uint64]bool) map[uint64]bool {
	out := make(map[uint64]bool, len(m))
	for id, is := range m {
		out[id] = is
	}
	return out
}

// mergeNearest gives the k nearest of two answers, each nearest first.
func TestMergeNearest(t *testing.T) {
	tests := []struct {
		name      string
		ids       []uint64
		dists     []float32
		moreIDs   []uint64
		moreDists []float32
		k         int
		want      []uint64
	}{
		{name: "both empty", k: 3},
		{name: "one empty", ids: []uint64{1, 2}, dists: []float32{0.1, 0.2}, k: 3, want: []uint64{1, 2}},
		{name: "the other empty", moreIDs: []uint64{1, 2}, moreDists: []float32{0.1, 0.2}, k: 1, want: []uint64{1}},
		{
			name: "interleaved", ids: []uint64{1, 3, 5}, dists: []float32{0.1, 0.3, 0.5},
			moreIDs: []uint64{2, 4}, moreDists: []float32{0.2, 0.4}, k: 4, want: []uint64{1, 2, 3, 4},
		},
		{
			name: "all of the second first", ids: []uint64{1}, dists: []float32{0.9},
			moreIDs: []uint64{2, 4}, moreDists: []float32{0.2, 0.4}, k: 10, want: []uint64{2, 4, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, dists := mergeNearest(tt.ids, tt.dists, tt.moreIDs, tt.moreDists, tt.k)
			require.True(t, slices.Equal(tt.want, ids), "%v", ids)
			require.Len(t, dists, len(ids))
			require.True(t, slices.IsSorted(dists))
		})
	}
}

// The fixture builds the base of its index from BaseSize vectors whatever
// the scheduler does. Here the build in the background cannot start until
// well after the call that brings the last vector has returned, as on a
// loaded machine: before the fixture waited for the base at its size, it read
// every vector the test added, and an index of 2 100 vectors built from all of
// them never split.
func TestPersistentFixtureBuildsItsBaseFromItsBaseSize(t *testing.T) {
	const total = persistentBase + 1500
	logger, logs := logtest.NewNullLogger()
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.Logger = logger })
	f.index.baseMu.Lock()
	enterrors.GoWrapper(func() {
		time.Sleep(time.Second)
		f.index.baseMu.Unlock()
	}, testLogger())
	f.add(t, 0, total)
	built := 0
	for _, entry := range logs.AllEntries() {
		if entry.Data["action"] == "postingedge_build_base" {
			built = entry.Data["vectors"].(int)
		}
	}
	assert.Equal(t, persistentBase, built, "the vectors the base was built from")
	require.NoError(t, f.index.Settle(context.Background()))
	f.index.mu.RLock()
	splits := f.index.g.meta.Splits
	f.index.mu.RUnlock()
	assert.Positive(t, splits, "the index split on the way")
}
