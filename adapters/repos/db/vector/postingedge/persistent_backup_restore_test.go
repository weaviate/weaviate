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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A backup of a shard taken while it goes on writing, restored, opens as the
// index it took: the same answers, every vector it held, and it goes on
// growing. A shard prepares its indexes before it flushes the memtables of
// its store, and lists and copies their files after (Shard.haltForTransfer,
// #14003): the writes on both sides of the flush are in the backup. The
// buckets of the index are those of the flush and the journal tells what was
// written after, up to the copy, so the index restored is the one there was
// when its files were copied. The vectors kept aside before there is a base
// are in a bucket of the store and nowhere else: of them the backup holds
// what the flush wrote, as of every bucket of the shard.
func TestPersistentBackupRestoresTheIndexItTook(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentBackupRestoresTheIndexItTook(t, cacheMode) })
	}
}

func persistentBackupRestoresTheIndexItTook(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const k = 10
	const total = 2*persistentBase + 600
	tests := []struct {
		name string
		// added are the vectors before the backup; before and after it
		// deletes the doc ids of deleted that it holds.
		added   int
		deleted []uint64
		// laidOut has a pass over the vectors due when the backup begins.
		laidOut bool
		// window is how many vectors come on each side of the flush, 100
		// when 0; split has postings split meanwhile, which changes the
		// centroid graph.
		window int
		split  bool
	}{
		{name: "gathering", added: 300, deleted: []uint64{7}},
		{name: "the base", added: persistentBase + 50, deleted: []uint64{3, 640, 700}},
		{
			name: "the base, split during the backup", added: persistentBase + 50, deleted: []uint64{3, 640, 1000},
			window: 300, split: true,
		},
		{name: "a pass over the vectors due", added: 2*persistentBase + 50, deleted: []uint64{11, 1210, 1260}, laidOut: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) { cfg.noBackground = true })
			f.add(t, 0, tt.added)
			if tt.laidOut {
				require.True(t, f.index.g.layoutDue())
			}
			at := tt.added
			write := func(n int) {
				f.addBatches(t, at, at+n)
				at += n
				var gone []uint64
				for _, id := range tt.deleted {
					if id < uint64(at) && f.index.ContainsDoc(id) {
						gone = append(gone, id)
						break
					}
				}
				require.NoError(t, f.index.Delete(gone...))
			}
			window := max(tt.window, 100)
			postings := 0
			if f.index.g != nil {
				postings = f.index.g.meta.Postings
			}
			cut := f.backup(t, k, func() { write(window) }, func() { write(window) })
			if tt.split {
				require.Greater(t, f.index.g.meta.Postings, postings, "postings split during the backup")
				require.True(t, cut.marked, "the centroid graph changed during the backup")
			}

			// The index goes on after the backup: it grows, deletes and
			// lays its vectors out, which removes the files of the
			// generation the backup linked.
			f.add(t, at, max(at, total-200))
			require.NoError(t, f.index.Delete(1, 2))
			require.NoError(t, f.index.Settle(ctx))
			if f.index.g != nil && cut.generation > 0 {
				require.Greater(t, f.index.g.meta.VectorGen, cut.generation, "the vectors were laid out after the backup")
			}
			require.NoError(t, f.index.Flush())

			c := f.restored(t, cut)
			want, wantHolds := cut.answers, cut.holds
			if cut.generation == 0 && c.index.g == nil {
				// Gathering: the vectors aside are what the flush wrote.
				want, wantHolds = cut.atFlush, cut.holdsAtFlush
			}
			assert.Equal(t, wantHolds, c.holds(), "the vectors the index held")
			assert.Equal(t, want, c.searched(t, k), "the same answers")
			if c.index.g != nil {
				// The writes after the flush are in the backup by the journal
				// alone.
				require.Positive(t, c.index.g.recovered.Records, "the journal of the backup is applied again")
				t.Logf("restored: %d records of the journal applied again, centroid graph built again: %t",
					c.index.g.recovered.Records, c.index.g.recovered.GraphRebuilt)
				require.Equal(t, cut.marked, c.index.g.recovered.GraphRebuilt,
					"the centroid graph is built again where it changed during the backup")
				c.covered(t, "restored")
				c.vectorsIntact(t, "restored")
			}

			c.add(t, total-200, total)
			held := func(id uint64) bool { return c.index.ContainsDoc(id) }
			if c.index.g != nil {
				c.covered(t, "grown after the restore")
			}
			assert.Greater(t, c.recall(t, total, k, held), 0.9)
		})
	}
}
