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

package sorter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// tiedBasicComparator always reports equality, simulating objects whose sort-key
// values all tie so that the tie-breaker in comparator.compare is exercised.
type tiedBasicComparator struct{}

func (tiedBasicComparator) compare(a, b interface{}) int { return 0 }

func TestComparatorTieBreaksOnDocID(t *testing.T) {
	c := &comparator{comparators: []basicComparator{tiedBasicComparator{}}}

	t.Run("compare falls back to ascending docID when sort keys tie", func(t *testing.T) {
		lo := &comparable{docID: 1, values: []interface{}{"x"}}
		hi := &comparable{docID: 2, values: []interface{}{"x"}}
		assert.Equal(t, -1, c.compare(lo, hi))
		assert.Equal(t, 1, c.compare(hi, lo))
		assert.Equal(t, 0, c.compare(lo, lo))
	})

	t.Run("tied objects sort deterministically by docID regardless of input order", func(t *testing.T) {
		// The cross-shard merge appends per-shard results in nondeterministic
		// goroutine-completion order and then runs an unstable sort. Objects that tie on
		// every sort key must still end up in a single deterministic order (ascending
		// docID), otherwise a boundary object can be dropped from or duplicated across
		// paginated (offset/limit) requests.
		want := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		inputs := [][]uint64{
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			{5, 3, 9, 1, 7, 2, 8, 4, 6, 0},
		}
		for _, in := range inputs {
			sorter := newDefaultSorter(c, len(in))
			for _, id := range in {
				sorter.addComparable(&comparable{docID: id, values: []interface{}{"tie"}})
			}
			sorted := sorter.getSorted()
			got := make([]uint64, len(sorted))
			for i, cmp := range sorted {
				got[i] = cmp.docID
			}
			assert.Equal(t, want, got,
				"tied objects must sort by ascending docID, input order %v", in)
		}
	})
}
