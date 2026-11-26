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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// crashPoints are the writes of a step after which a crash is tried, counted
// from the first: all of the first ones and then fewer and fewer, so that a
// step of a few writes is tried whole and one of hundreds at a dozen places.
var crashPoints = map[int]bool{
	1: true, 2: true, 3: true, 5: true, 8: true, 13: true, 21: true, 34: true, 55: true, 89: true, 144: true, 233: true,
	377: true, 610: true, 987: true,
}

// crashAtPoints tries a crash after the writes of crashPoints.
func crashAtPoints(_, wrote int) bool {
	return crashPoints[wrote]
}

// What a process that dies in the middle of a write leaves on disk is the
// index as it was before that write, or as it is after it: an insert, with
// the split of its posting, a mark of a deleted vector, a step of the pass
// that takes the deleted vectors out, a refresh of the edges of a vector.
// The journal is not emptied between two of them unless the case says so,
// and an index that is opened over it is the one all its records tell.
// Every step is tried, at every point of crashPoints: the sweep that
// TestGrowOpensWhatACrashLeft samples in the package.
func TestGrowOpensWhatACrashLeftEveryPoint(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		// syncEvery empties the journal every so many steps, 0 for never.
		syncEvery int
		// halo has the postings carry their halo, and the index no codes in
		// memory.
		halo bool
	}{
		{name: "the journal grows", syncEvery: 0},
		{name: "the journal is emptied after every step", syncEvery: 1},
		{name: "no codes in memory, the journal grows", syncEvery: 0, halo: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 1036, 1000, 64
			format := shardFormat(8)
			if tt.halo {
				format = haloFormat(8, false)
			}
			format.open.journal, format.open.deletes = true, true
			f := newGrowFixtureFormat(t, total, base, dims, false, 64, 46, format)
			g := f.g
			sweep := newCrashSweep(f)
			steps, checked := 0, 0
			digest, _ := sweep.open(t, f.copyNow(t), "the base")
			// crashed runs a step and tries the crashes of its end.
			crashed := func(name string, step func() error) {
				var opened int
				digest, opened = sweep.crashing(t, name, digest, crashAtPoints, step)
				checked += opened
				if steps++; tt.syncEvery > 0 && steps%tt.syncEvery == 0 {
					require.NoError(t, g.sync())
				}
			}
			splits := g.meta.Splits
			for f.inserted < total {
				docID := f.inserted
				f.inserted++
				crashed(errors.Errorf("the insert of doc %d", docID).Error(), func() error {
					return g.insert(ctx, uint64(docID), f.all[docID])
				})
			}
			require.Greater(t, g.meta.Splits, splits+2, "postings split on the way")

			// Marks of deleted vectors, each a step, and several in one.
			for docID := uint64(0); docID < 12; docID += 4 {
				crashed(errors.Errorf("the mark of doc %d", docID).Error(), func() error {
					_, err := g.remove(docID)
					return err
				})
			}
			crashed("the marks of many docs", func() error {
				return g.step(func() error {
					for docID := uint64(100); docID < 700; docID += 2 {
						if _, err := g.remove(docID); err != nil {
							return err
						}
					}
					return nil
				})
			})

			// The pass that takes them out, a few postings a step.
			pass, err := g.beginClean(mergeUnder(g.meta.Threshold), g.meta.Threshold)
			require.NoError(t, err)
			for finished := false; !finished; {
				if pass.gathers() {
					finished, err = pass.step(ctx, cleanGatherStep)
					require.NoError(t, err)
					continue
				}
				crashed(errors.Errorf("step %d of the pass over the deleted vectors", steps).Error(), func() error {
					var err error
					finished, err = pass.step(ctx, 6)
					return err
				})
			}
			require.Zero(t, g.deletedCount)
			require.Positive(t, g.cleaned.Merges, "postings were merged")
			if tt.halo {
				require.Positive(t, g.stats.HaloAdded.N, "the writes added to the halos")
				require.Positive(t, g.stats.HaloRows.N, "the halves of a split got their halos")
			}
			t.Logf("%d steps, %d crashes opened, %d records and %d bytes in the journal", steps, checked, g.journal.records,
				g.journal.bytes)
			assert.Greater(t, checked, 100)
		})
	}
}
