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

//go:build integrationTest

package inverted

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
)

// The two validation instruments the batched Contains fold's design calls for
// and nothing else provides: what the fold costs across the shape space, and
// what a static split does to ContainsAll's early exit.
//
// Run:
//
//	go test -tags integrationTest -run '^$' -bench 'ContainsShapeMatrix' \
//	    -benchmem -count 6 ./adapters/repos/db/inverted/
//	go test -tags integrationTest -run '^$' -bench 'ContainsEarlyExit' \
//	    -count 6 ./adapters/repos/db/inverted/
//
// Never both at once on the same machine — they contend, and the numbers say so
// without saying why.

// BenchmarkDocIDs_ContainsShapeMatrix compares the batched path against the
// desugared one it replaces, over the dimensions the fold's cost actually turns
// on: which operator, how many keys, and how big a row is.
//
// Row size is a separate corpus rather than a parameter because it is a
// property of what was indexed. The thin one is the reported pathological shape
// — unique values, one doc per row — where the per-key overhead the batching
// removes is the whole cost. The fat one gives every row ten thousand docs, so
// merging dominates instead, and it keeps a quarter of its keys unflushed so
// the windowed memtable read runs at all.
//
// It does NOT measure memory. The thin corpus is flushed, so its active
// memtable is empty and the reader drops it from the view, and its single-doc
// rows make realized window bytes a rounding error — the window term is
// structurally invisible here. BenchmarkDocIDs_ContainsAllBigRows is where that
// is observable.
func BenchmarkDocIDs_ContainsShapeMatrix(b *testing.B) {
	ctx := context.Background()
	thin := newContainsFixture(b, benchCorpusSize)
	fat := newBigRowsFixture(b)

	corpora := []struct {
		name     string
		searcher *Searcher
		values   func(size int) []string
		sizes    []int
	}{
		{
			name: "thin", searcher: thin.searcher,
			values: func(size int) []string { v, _ := thin.sampleValues(size); return v },
			sizes:  []int{16, 128, 1024, 10_000, 100_000},
		},
		{
			name: "fat", searcher: fat.searcher,
			values: bigRowsValues,
			sizes:  []int{16, 128, 512},
		},
	}
	paths := []struct {
		name   string
		filter func(filters.Operator, []string) *filters.LocalFilter
	}{
		{"batched", containsFilter},
		{"desugared", equalCompoundFilter},
	}

	for _, corpus := range corpora {
		for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
			for _, size := range corpus.sizes {
				values := corpus.values(size)
				for _, path := range paths {
					filter := path.filter(op, values)
					b.Run(fmt.Sprintf("%s/%s/N=%04d/%s", corpus.name, op.Name(), size, path.name), func(b *testing.B) {
						benchDocIDs(b, corpus.searcher, ctx, filter)
					})
				}
			}
		}
	}
}

// BenchmarkDocIDs_ContainsEarlyExit measures what a static split costs
// ContainsAll's early exit. A single walk stops at the key that empties the
// intersection; a worker can only stop at a key inside its OWN share, so
// emptiness a sequential fold would have found on its second read may not be
// found until some worker has read most of its share.
//
// The sweep places the emptying key at a fraction of the way through the batch
// and compares one worker against the planner's own count. Early positions are
// where the split should lose and late ones where it should win; what matters
// is the size of both, not that either exists.
func BenchmarkDocIDs_ContainsEarlyExit(b *testing.B) {
	ctx := context.Background()
	const numKeys = 512

	for _, at := range []int{1, numKeys / 8, numKeys / 2, numKeys - 1} {
		f := newEarlyExitFixture(b, numKeys, at)
		values := bigRowsValues(numKeys)
		filter := containsFilter(filters.ContainsAll, values)

		for _, workers := range []int{1, 0} {
			name := fmt.Sprintf("emptiesAt=%03d/workers=default", at)
			if workers > 0 {
				name = fmt.Sprintf("emptiesAt=%03d/workers=%d", at, workers)
			}
			b.Run(name, func(b *testing.B) {
				forceContainsWorkers(b, workers)

				benchDocIDs(b, f.searcher, ctx, filter)
			})
		}
	}
}

// newEarlyExitFixture builds a corpus whose ContainsAll empties at exactly one
// known key: every row holds the shared core except the one at emptiesAt, whose
// row is disjoint from everything. Where that key falls in the batch is what
// decides how much of the batch a fold has to read.
// earlyExitDocs is the doc-ID count the fixture's shard reports. It sits clear
// of the highest ID any row holds — the disjoint row ends at 10,001,999 — so
// the planner prices a row against a shard the rows fit inside.
const earlyExitDocs = 11_000_000

func newEarlyExitFixture(tb testing.TB, numKeys, emptiesAt int) *roaringRowsFixture {
	tb.Helper()

	const rowSize = 2_000
	rows := make([][]uint64, numKeys)
	for k := range rows {
		row := make([]uint64, 0, rowSize)
		if k == emptiesAt {
			// disjoint from every other row, so the intersection is empty the
			// moment this key is folded in
			for d := 0; d < rowSize; d++ {
				row = append(row, uint64(10_000_000+d))
			}
		} else {
			for d := 0; d < rowSize; d++ {
				row = append(row, uint64(d))
			}
		}
		rows[k] = row
	}

	return newRoaringRowsFixture(tb, rows, earlyExitDocs)
}

// TestDocIDs_ContainsEarlyExitCorpus pins the premise the early-exit sweep
// rests on: the intersection really is empty, and it is empty because of the
// one key the fixture placed rather than for any other reason.
func TestDocIDs_ContainsEarlyExitCorpus(t *testing.T) {
	ctx := context.Background()
	const numKeys = 64
	for _, at := range []int{1, numKeys / 2, numKeys - 1} {
		t.Run(fmt.Sprintf("emptiesAt=%d", at), func(t *testing.T) {
			f := newEarlyExitFixture(t, numKeys, at)
			values := bigRowsValues(numKeys)

			require.Empty(t, resolveDocIDs(t, ctx, f.searcher, containsFilter(filters.ContainsAll, values)),
				"the placed key must empty the intersection")

			// without that key the batch intersects to the shared core, so the
			// emptiness is the fixture's doing and not an accident of the rest
			withoutIt := append(append([]string{}, values[:at]...), values[at+1:]...)
			require.Len(t, resolveDocIDs(t, ctx, f.searcher, containsFilter(filters.ContainsAll, withoutIt)), 2_000,
				"every other row shares the whole core")
		})
	}
}
