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
	"os"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openedAt opens the index that the files under root are, with their store,
// as the fixture's was opened.
func (f *persistentFixture) openedAt(t *testing.T, root string) *persistentFixture {
	t.Helper()
	c := &persistentFixture{
		root: root, cfg: f.cfg, uc: f.uc, vectors: f.vectors, normalized: f.normalized, queries: f.queries,
		provider: f.provider,
	}
	c.cfg.RootPath = root
	c.open(t)
	return c
}

// The kinds of the steps of the index a crash is taken in, told apart by
// what the step changed of the index. The pass that takes the deleted
// vectors out gives the edges that lead to them substitutes, takes them out
// of their postings, and merges the postings it left small, a posting a
// step.
const (
	crashInsert     = "an insert"
	crashSplit      = "an insert that splits its posting"
	crashMarks      = "the marks of a delete call"
	crashSubstitute = "a step of the pass over the deleted vectors that gives their edges substitutes"
	crashRemove     = "a step of that pass that takes them out of their posting"
	crashMerge      = "a step of that pass that merges a posting"
	crashRefresh    = "the refresh of the edges of a vector"
	// crashOther is a step that changes none of the counts: the one that
	// ends a pass of the refresh writes only the meta.
	crashOther = "another step"
)

var crashKinds = []string{crashInsert, crashSplit, crashMarks, crashSubstitute, crashRemove, crashMerge, crashRefresh}

// stepCounts is what the steps of the index have done so far.
type stepCounts struct {
	live, splits, marked, rewritten, removed, merges, refreshed int
}

// countSteps reads the counts of the index, with those of the pass over the
// deleted vectors that is on. Whoever calls holds the lock of the index or
// is its writer.
func countSteps(p *Persistent) stepCounts {
	g := p.g
	cleaned := g.cleaned
	if pass := p.cleaning; pass != nil {
		cleaned.add(&pass.stats)
	}
	return stepCounts{
		live: g.meta.Live, splits: g.meta.Splits, marked: g.deletedCount,
		rewritten: cleaned.DeadEdges + cleaned.RowsWritten, removed: cleaned.Removed, merges: cleaned.Merges,
		refreshed: g.refreshed.Refreshed,
	}
}

// kindOf tells the kind of the step that took the counts from c to after.
func (c stepCounts) kindOf(after stepCounts) string {
	switch {
	case after.merges > c.merges:
		return crashMerge
	case after.removed > c.removed:
		return crashRemove
	case after.rewritten > c.rewritten:
		return crashSubstitute
	case after.refreshed > c.refreshed:
		return crashRefresh
	case after.splits > c.splits:
		return crashSplit
	case after.marked > c.marked:
		return crashMarks
	case after.live > c.live:
		return crashInsert
	}
	return crashOther
}

// A process that dies while the index a shard opens writes leaves an index,
// with nothing flushed since the write began. Of the batch it was adding it
// holds the vectors whose insert had ended, the first so many of it, and
// everything of the calls before; of a call that was deleting, all of its
// marks or none. It is covered, reads every vector it holds as it was given,
// answers with what it holds, and takes what comes next. So through the
// splits of the postings, and through what the index does in line with its
// writes: the pass that takes the deleted vectors out, with its merges, and
// the refresh of the edges once the index has doubled.
//
// A copy of the files is taken in the first step of every kind, at the
// points a process can die at that differ: after its record is written to
// the journal, with nothing of it handed to the store, and after the write
// in the middle of those it hands to the store. Of the first split the copy
// before its record is taken too, where the step never happened. Every copy
// is opened in a subtest of its own once the calls are made, next to the
// others.
func TestPersistentOpensWhatACrashLeft(t *testing.T) {
	t.Parallel()
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) {
			t.Parallel()
			persistentOpensWhatACrashLeft(t, cacheMode)
		})
	}
}

func persistentOpensWhatACrashLeft(t *testing.T, cacheMode string) {
	ctx := persistentCtx()
	const total = 2*persistentBase + 100
	f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) { cfg.noBackground, cfg.refreshEdges = true, true })
	f.add(t, 0, persistentBase+100)
	require.NoError(t, f.index.Flush())

	type crash struct{ root, kind, point string }
	var crashes []crash
	// steps counts the steps of every kind, and copied the copies taken in
	// them.
	steps, copied := map[string]int{}, map[string]int{}
	kind := crashInsert
	f.index.mu.Lock()
	last := countSteps(f.index)
	f.index.g.journal.crash = func(point string, written int) {
		p := f.index
		if point == "before its record" {
			now := countSteps(p)
			kind = last.kindOf(now)
			last = now
			steps[kind]++
		}
		if kind == crashOther || steps[kind] != 1 {
			return
		}
		switch point {
		case "before its record":
			if kind != crashSplit {
				return
			}
		case "after its record":
		case "after a write":
			if written != (len(p.g.journal.entries)+1)/2 {
				return
			}
		default:
			return
		}
		root := t.TempDir()
		require.NoError(t, os.CopyFS(root, os.DirFS(f.root)))
		crashes = append(crashes, crash{root: root, kind: kind, point: point})
	}
	f.index.mu.Unlock()

	deleted := make(map[uint64]bool)
	// check has the copies taken during a call opened, each in a subtest of
	// its own that runs once the calls are made: acked is how many doc ids
	// the calls before had added, adding the doc ids the call was adding and
	// deleting those it was deleting.
	taken := 0
	check := func(name string, acked int, adding [2]int, deleting []uint64) {
		t.Helper()
		st := crashState{acked: acked, adding: adding, deleting: deleting, deleted: mapsClone(deleted)}
		for i, cr := range crashes {
			at := errors.Errorf("%s, crash %d of %d, in %s, %s", name, i+1, len(crashes), cr.kind, cr.point).Error()
			copied[cr.kind]++
			taken++
			t.Run(errors.Errorf("crash %d", taken).Error(), func(t *testing.T) {
				t.Parallel()
				f.openCrash(t, cr.root, at, st)
			})
		}
		crashes = crashes[:0]
	}

	// A batch, with the first split of a posting.
	added := persistentBase + 100
	splits := f.index.g.meta.Splits
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(80, uint64(added)), f.vectors[added:added+80]))
	check("a batch", added, [2]int{added, added + 80}, nil)
	added += 80

	// The marks of a call, enough of them for a pass to be due, and for the
	// halves of a posting that split to be left under the size a posting
	// merges at. The deletes of the call take the pass a step further for
	// every mark.
	var marks []uint64
	for id := uint64(3); id < uint64(added); id += 6 {
		marks = append(marks, id)
	}
	require.NoError(t, f.index.Delete(marks...))
	check("the marks of a call", added, [2]int{added, added}, marks)
	for _, id := range marks {
		deleted[id] = true
	}

	// More batches, each with a share of the pass that takes the marked
	// vectors out if it is still on, and once the index holds twice its
	// base, with the refresh of the edges.
	for added < total {
		to := min(added+40, total)
		require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(to-added, uint64(added)), f.vectors[added:to]))
		check(errors.Errorf("the batch from doc %d", added).Error(), added, [2]int{added, to}, nil)
		added = to
	}
	f.index.mu.Lock()
	g := f.index.g
	g.journal.crash = nil
	cleaned, refreshed := g.cleaned, g.refreshed
	nowSplits := g.meta.Splits
	f.index.mu.Unlock()
	assert.Greater(t, nowSplits, splits, "postings split on the way")
	assert.Positive(t, cleaned.Removed, "deleted vectors were taken out on the way")
	assert.Positive(t, cleaned.Merges, "postings merged on the way")
	assert.Positive(t, refreshed.Refreshed, "edges were refreshed on the way")
	for _, kind := range crashKinds {
		assert.Positive(t, copied[kind], "crashes taken in %s", kind)
	}
	t.Logf("crashes taken by kind %v, over steps by kind %v; %d splits, %d vectors taken out with %d merges, %d refreshed",
		copied, steps, nowSplits-splits, cleaned.Removed, cleaned.Merges, refreshed.Refreshed)
	f.covered(t, "at the end")
}

// crashState is what the copy of the files of an index taken in a call must
// hold: acked is how many doc ids the calls before had added, adding the doc
// ids the call was adding, deleting those it was deleting, and deleted those
// the calls before had deleted.
type crashState struct {
	acked    int
	adding   [2]int
	deleting []uint64
	deleted  map[uint64]bool
}

// openCrash opens the copy of the files of the fixture's index under root,
// taken in a call, and checks that it is an index that holds what st says,
// answers with what it holds, and takes what comes next.
func (f *persistentFixture) openCrash(t *testing.T, root, at string, st crashState) {
	t.Helper()
	ctx := persistentCtx()
	const k = 10
	total := len(f.vectors)
	c := f.openedAt(t, root)
	c.covered(t, at)
	c.vectorsIntact(t, at)
	for id := range uint64(st.acked) {
		if slices.Contains(st.deleting, id) {
			continue
		}
		require.Equal(t, !st.deleted[id], c.index.ContainsDoc(id), "%s: doc %d, of a call that had ended", at, id)
	}
	held := st.adding[0]
	for held < st.adding[1] && c.index.ContainsDoc(uint64(held)) {
		held++
	}
	for id := held; id < total; id++ {
		require.False(t, c.index.ContainsDoc(uint64(id)), "%s: doc %d is held, and doc %d before it is not", at, id, held)
	}
	gone := 0
	for _, id := range st.deleting {
		if !c.index.ContainsDoc(id) {
			gone++
		}
	}
	require.True(t, gone == 0 || gone == len(st.deleting), "%s: %d of the %d marks of a call", at, gone, len(st.deleting))
	for q, query := range f.queries[:6] {
		ids, _, err := c.index.SearchByVector(ctx, query, k, nil)
		require.NoError(t, err, "%s: query %d", at, q)
		require.Len(t, ids, k, "%s: query %d", at, q)
		for _, id := range ids {
			require.True(t, c.index.ContainsDoc(id), "%s: query %d returned doc %d, which is not held", at, q, id)
		}
	}
	// And it goes on from there.
	if more := min(held+8, total); more > held {
		require.NoError(t, c.index.AddBatch(ctx, sequentialIDs(more-held, uint64(held)), f.vectors[held:more]), at)
		c.covered(t, at+", with more added")
	}
	c.close(t)
	require.NoError(t, os.RemoveAll(root))
}
