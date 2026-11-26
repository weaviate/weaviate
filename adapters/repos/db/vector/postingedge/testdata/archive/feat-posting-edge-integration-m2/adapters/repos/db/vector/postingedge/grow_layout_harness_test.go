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

//go:build postingedgebench

package postingedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An index whose vectors have slots, and one that keeps in a bucket what
// waits of its edges, is the streaming index in memory at every insert, as
// the index of v5.0 is: where a vector is on disk and where a change waits
// change nothing of what the index is. Its vectors are laid out again on the
// way, at once and spread over two hundred inserts, with postings splitting
// under the pass, and it is closed and opened with changes waiting.
// TestGrowSlotsAndPendingEqualMemory, in the package, grows smaller indexes
// over fewer formats.
func TestGrowSlotsAndPendingEqualMemoryAtScale(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		format     growConfig
		cosine     bool
		bits       int
		threshold  int
		flushEvery int
	}{
		{name: "a value per list, vectors by slot", format: growConfig{slots: true}, bits: 64, threshold: 60, flushEvery: 37},
		{name: "rows next to the members", format: shardFormat(4), bits: 64, threshold: 60, flushEvery: 41},
		{
			name:   "rows in a bucket of their own, vectors at their doc ids",
			format: growConfig{consolidate: 8, open: growOpen{keepPending: true}}, bits: 64, threshold: 60, flushEvery: 53,
		},
		{name: "the format of a shard, cosine", format: shardFormat(DefaultConsolidate), cosine: true, bits: 128, threshold: 60, flushEvery: 29},
		{name: "the format of a shard, few splits, everything in the memtable", format: shardFormat(DefaultConsolidate), threshold: 400},
		{name: "no codes in memory", format: haloFormat(4, false), bits: 64, threshold: 60, flushEvery: 41},
		{name: "no codes in memory, cosine", format: haloFormat(DefaultConsolidate, false), cosine: true, bits: 128, threshold: 60, flushEvery: 29},
		{name: "no codes in memory, the full code, everything in the memtable", format: haloFormat(DefaultConsolidate, false), threshold: 60},
		{name: "the halo kept up with the codes in memory", format: haloFormat(8, true), bits: 64, threshold: 60, flushEvery: 37},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 2200, 1000, 64
			f := newGrowFixtureFormat(t, total, base, dims, tt.cosine, tt.bits, tt.threshold, tt.format)
			slots := tt.format.slots
			require.Equal(t, tt.format.open.keepPending && tt.format.consolidate > 0, f.g.waiting != nil)
			f.same(t, "the base")
			f.vectorsIntact(t, "the base")
			if slots {
				require.Equal(t, 1, f.g.meta.VectorGen)
				f.laidOut(t, "the base")
			} else {
				require.Zero(t, f.g.meta.VectorGen)
				require.Nil(t, f.g.slotOf)
			}

			var pass *growLayout
			splitsBefore := 0
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				done := f.inserted - base
				if tt.flushEvery > 0 && done%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
				}
				switch {
				case done == 300 && slots:
					require.NoError(t, f.g.layOut())
					f.laidOut(t, "laid out at once")
					f.vectorsIntact(t, "laid out at once")
					f.same(t, "laid out at once")
				case done == 500:
					waited := f.g.pendingMembers
					f.reopen(t)
					if f.g.waiting != nil {
						require.Equal(t, waited, f.g.pendingMembers, "what waited when it closed waits when it opens")
					}
					f.same(t, "opened again")
					f.vectorsIntact(t, "opened again")
				case done == 700 && slots:
					pass, err = f.g.beginLayout()
					require.NoError(t, err)
					splitsBefore = f.g.stats.Splits
				case done == 900 && slots:
					require.NoError(t, pass.finish())
					pass = nil
					require.Equal(t, f.g.meta.Live, int(f.g.nextSlot), "a slot per vector")
					require.Equal(t, f.g.meta.Live-200, f.g.meta.VectorsLaidOut, "those that came in under the pass are after the others")
					if tt.threshold < 100 {
						require.Greater(t, f.g.stats.Splits, splitsBefore, "postings split under the pass")
					}
					f.vectorsIntact(t, "laid out next to the inserts")
					f.same(t, "laid out next to the inserts")
				}
				if pass != nil {
					_, err := pass.step(5)
					require.NoError(t, err)
				}
				if done%400 == 0 {
					f.same(t, "on the way")
				}
			}
			f.same(t, "at the end")
			f.vectorsIntact(t, "at the end")
			// What the index counted since it was opened, halfway.
			if f.g.waiting != nil {
				assert.Positive(t, f.g.stats.PendingLog.N, "the changes that wait are written to their bucket")
				if tt.format.consolidate <= 8 {
					assert.Positive(t, f.g.stats.RowWritesFull, "rows are written after %d changes", tt.format.consolidate)
				}
			} else {
				assert.Zero(t, f.g.stats.PendingLog.N)
			}
			f.reopen(t)
			f.same(t, "at the end, opened again")
			f.vectorsIntact(t, "at the end, opened again")
			if slots {
				require.Equal(t, 3, f.g.meta.VectorGen, "two passes after the base")
				require.Equal(t, []string{"slots-3.u32", "vectors-3.f32"}, generationFiles(t, f.dir))
			} else {
				require.Equal(t, []string{"vectors.f32"}, generationFiles(t, f.dir))
			}
		})
	}
}
