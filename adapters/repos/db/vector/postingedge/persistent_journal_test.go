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

// A process that dies while the index a shard opens writes leaves an index,
// with nothing flushed since the write began. Of the batch it was adding it
// holds the vectors whose insert had ended, the first so many of it, and
// everything of the calls before; of a call that was deleting, all of its
// marks or none. It is covered, reads every vector it holds as it was given,
// answers with what it holds, and takes what comes next. So through the
// splits of the postings, and through what the index does in line with its
// writes: the pass that takes the deleted vectors out, with its merges, and
// the refresh of the edges once the index has doubled.
func TestPersistentOpensWhatACrashLeft(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentOpensWhatACrashLeft(t, cacheMode) })
	}
}

func persistentOpensWhatACrashLeft(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const k = 10
	const total = 2*persistentBase + 150
	f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) { cfg.noBackground, cfg.refreshEdges = true, true })
	f.add(t, 0, persistentBase+100)
	require.NoError(t, f.index.Flush())

	// A copy of the files is taken at one of every so many points a write
	// can die at, while the write runs.
	var roots []string
	points, every := 0, 1
	f.index.mu.Lock()
	f.index.g.journal.crash = func(string, int) {
		if points++; points%every != 0 {
			return
		}
		root := t.TempDir()
		require.NoError(t, os.CopyFS(root, os.DirFS(f.root)))
		roots = append(roots, root)
	}
	f.index.mu.Unlock()

	deleted := make(map[uint64]bool)
	opened := 0
	// check opens the copies taken during a call: acked is how many doc ids
	// the calls before had added, adding the doc ids the call was adding and
	// deleting those it was deleting.
	check := func(name string, acked int, adding [2]int, deleting []uint64) {
		t.Helper()
		for i, root := range roots {
			at := errors.Errorf("%s, crash %d of %d", name, i+1, len(roots)).Error()
			c := f.openedAt(t, root)
			c.covered(t, at)
			c.vectorsIntact(t, at)
			for id := range uint64(acked) {
				if slicesContains(deleting, id) {
					continue
				}
				require.Equal(t, !deleted[id], c.index.ContainsDoc(id), "%s: doc %d, of a call that had ended", at, id)
			}
			held := adding[0]
			for held < adding[1] && c.index.ContainsDoc(uint64(held)) {
				held++
			}
			for id := held; id < total; id++ {
				require.False(t, c.index.ContainsDoc(uint64(id)), "%s: doc %d is held, and doc %d before it is not", at, id, held)
			}
			gone := 0
			for _, id := range deleting {
				if !c.index.ContainsDoc(id) {
					gone++
				}
			}
			require.True(t, gone == 0 || gone == len(deleting), "%s: %d of the %d marks of a call", at, gone, len(deleting))
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
			opened++
		}
		roots = roots[:0]
	}

	// A batch, with the splits of its postings.
	added := persistentBase + 100
	splits := f.index.g.meta.Splits
	every = 61
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(60, uint64(added)), f.vectors[added:added+60]))
	check("a batch", added, [2]int{added, added + 60}, nil)
	added += 60

	// The marks of a call, enough of them for a pass to be due.
	var marks []uint64
	for id := uint64(3); id < 300; id += 6 {
		marks = append(marks, id)
	}
	every = 7
	require.NoError(t, f.index.Delete(marks...))
	check("the marks of a call", added, [2]int{added, added}, marks)
	for _, id := range marks {
		deleted[id] = true
	}

	// More batches, each with a share of the pass that takes the marked
	// vectors out, and from twice the base on with the refresh of the edges.
	every = 733
	for added < total {
		to := min(added+50, total)
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
	assert.Positive(t, refreshed.Refreshed, "edges were refreshed on the way")
	assert.Greater(t, opened, 60, "crashes opened")
	t.Logf("%d crashes opened over %d points; %d splits, %d vectors taken out with %d merges, %d refreshed", opened, points,
		nowSplits-splits, cleaned.Removed, cleaned.Merges, refreshed.Refreshed)
	f.covered(t, "at the end")
}

func slicesContains(ids []uint64, id uint64) bool {
	for _, other := range ids {
		if other == id {
			return true
		}
	}
	return false
}
