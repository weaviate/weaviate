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
	"testing"

	"github.com/stretchr/testify/require"
)

// The index asks its store to leave its bucket of postings as one segment
// once a tenth of what it holds has been written since the last time, and
// not before. The cycles of the store then do it, the request is over on the
// two sides, and the index answers as it did.
func TestPersistentAsksForItsPostingsAsOneSegment(t *testing.T) {
	t.Parallel()
	const k = 10
	// few is fewer vectors than a tenth of the index, and many more.
	const few, many = persistentBase / 10, persistentBase / 2
	const total = persistentBase + 100 + few + many
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	never := func() bool { return false }
	type state struct {
		segments, merges, written int
		merging, asked            bool
	}
	now := func() state {
		f.index.mu.RLock()
		defer f.index.mu.RUnlock()
		g := f.index.g
		return state{
			segments: g.postings.SegmentCount(), merges: g.merges, written: g.sinceMerge, merging: g.merging,
			asked: g.postings.CompactingLeftOverSegmentsOfAnySize(),
		}
	}
	// flushed adds vectors and has the store flush, and not compact: the
	// bucket of postings has a segment more.
	added := 0
	flushed := func(n int) {
		f.add(t, added, added+n)
		added += n
		require.NoError(t, f.index.Flush())
		f.index.mu.Lock()
		err := f.index.g.flushAll()
		f.index.mu.Unlock()
		require.NoError(t, err)
	}

	flushed(persistentBase)
	flushed(100)
	before := now()
	require.Greater(t, before.segments, 1)
	require.False(t, before.merging)
	answers := f.answers(t, k)

	// Everything the index holds counts as written since the last time: it
	// asks, and the store does it.
	f.index.mergeRows()
	asked := now()
	require.True(t, asked.merging, "the index asked")
	require.True(t, asked.asked, "and the store was asked")
	require.Zero(t, asked.written)
	f.index.mergeRows()
	require.True(t, now().merging, "the store has not compacted yet: the request is on")
	for f.compaction.CycleCallback(never) {
	}
	require.False(t, now().asked, "the store ends the request once it has written the oldest segment again")
	f.index.mergeRows()
	done := now()
	require.Equal(t, state{segments: 1, merges: 1}, done)
	require.Equal(t, answers, f.answers(t, k), "the same answers")
	f.covered(t, "as one segment")

	// A few vectors more are another segment, and not enough to ask again.
	flushed(few)
	f.index.mergeRows()
	fewer := now()
	require.Greater(t, fewer.segments, 1)
	require.False(t, fewer.merging, "not a tenth of the index yet")
	require.False(t, fewer.asked)

	// With a tenth of it written, it asks again.
	flushed(many)
	f.index.mergeRows()
	again := now()
	require.True(t, again.merging)
	require.True(t, again.asked)
	for f.compaction.CycleCallback(never) {
	}
	f.index.mergeRows()
	require.Equal(t, state{segments: 1, merges: 2}, now())
	f.covered(t, "as one segment again")
	f.vectorsIntact(t, "as one segment again")
}
