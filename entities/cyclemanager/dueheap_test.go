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

package cyclemanager

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkHeapInvariant asserts the min-heap property on the backing array: every
// parent is due no later than either child.
func checkHeapInvariant(t *testing.T, h dueHeap) {
	t.Helper()
	for i := range h {
		if left := 2*i + 1; left < len(h) {
			assert.LessOrEqualf(t, h[i].due, h[left].due,
				"heap invariant violated: parent %d (due=%d) > left child %d (due=%d)",
				i, h[i].due, left, h[left].due)
		}
		if right := 2*i + 2; right < len(h) {
			assert.LessOrEqualf(t, h[i].due, h[right].due,
				"heap invariant violated: parent %d (due=%d) > right child %d (due=%d)",
				i, h[i].due, right, h[right].due)
		}
	}
}

func TestDueHeap_PopOrder(t *testing.T) {
	base := time.Now().UnixNano()

	// deepEntries builds n entries with due times scrambled by a fixed LCG, so
	// push and pop sift through every level of a tall heap.
	deepEntries := func(n int) []dueEntry {
		es := make([]dueEntry, n)
		x := uint64(1)
		for i := 0; i < n; i++ {
			x = x*6364136223846793005 + 1442695040888963407
			es[i] = dueEntry{
				callbackId: uint32(i),
				due:        base + int64(x%uint64(n)),
				schedGen:   uint64(i) + 1,
			}
		}
		return es
	}

	tests := []struct {
		name       string
		entries    []dueEntry
		want       []uint32 // callbackIds in exact pop order
		wantPrefix []uint32 // callbackIds that must occupy the first len(wantPrefix) pops, in any order
	}{
		{
			name:    "empty",
			entries: nil,
			want:    nil,
		},
		{
			name:    "single",
			entries: []dueEntry{{callbackId: 1, due: base, schedGen: 10}},
			want:    []uint32{1},
		},
		{
			name: "ascending input",
			entries: []dueEntry{
				{callbackId: 1, due: base, schedGen: 1},
				{callbackId: 2, due: base + int64(time.Second), schedGen: 2},
				{callbackId: 3, due: base + int64(2*time.Second), schedGen: 3},
			},
			want: []uint32{1, 2, 3},
		},
		{
			name: "descending input",
			entries: []dueEntry{
				{callbackId: 3, due: base + int64(2*time.Second), schedGen: 3},
				{callbackId: 2, due: base + int64(time.Second), schedGen: 2},
				{callbackId: 1, due: base, schedGen: 1},
			},
			want: []uint32{1, 2, 3},
		},
		{
			name: "shuffled with equal due times",
			entries: []dueEntry{
				{callbackId: 4, due: base + int64(3*time.Second), schedGen: 4},
				{callbackId: 1, due: base, schedGen: 1},
				{callbackId: 3, due: base + int64(time.Second), schedGen: 3},
				{callbackId: 2, due: base, schedGen: 2},
			},
			// callbackId 1 and 2 share base due; they must pop before 3 and 4,
			// but their relative order within the tie is unspecified.
			wantPrefix: []uint32{1, 2},
		},
		{
			name:    "deep sift, 200 entries",
			entries: deepEntries(200),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &dueHeap{}
			for _, e := range tt.entries {
				h.push(e)
				checkHeapInvariant(t, *h)
			}

			var poppedDue []int64
			var poppedIds []uint32
			var poppedSched []uint64
			origSched := make(map[uint32]uint64, len(tt.entries))
			for _, e := range tt.entries {
				origSched[e.callbackId] = e.schedGen
			}

			for len(*h) > 0 {
				e := h.pop()
				checkHeapInvariant(t, *h)
				poppedDue = append(poppedDue, e.due)
				poppedIds = append(poppedIds, e.callbackId)
				poppedSched = append(poppedSched, e.schedGen)
			}

			// pop order is always non-decreasing in due
			for i := 1; i < len(poppedDue); i++ {
				assert.False(t, poppedDue[i] < poppedDue[i-1],
					"pop %d (%v) before pop %d (%v)", i, poppedDue[i], i-1, poppedDue[i-1])
			}

			// value integrity: each popped entry retains its original callbackId and schedGen
			for i, id := range poppedIds {
				expected, ok := origSched[id]
				require.True(t, ok, "popped unknown callbackId %d", id)
				assert.Equal(t, expected, poppedSched[i],
					"schedGen mismatch for callbackId %d at pop %d", id, i)
			}

			if tt.want != nil {
				assert.Equal(t, tt.want, poppedIds)
			}
			if tt.wantPrefix != nil {
				require.GreaterOrEqual(t, len(poppedIds), len(tt.wantPrefix))
				assert.ElementsMatch(t, tt.wantPrefix, poppedIds[:len(tt.wantPrefix)])
			}
		})
	}
}

func TestDueHeap_Compact(t *testing.T) {
	base := time.Now().UnixNano()
	// entries builds n entries across a range of due times so a surviving subset
	// forms a multi-level heap for compaction to rebuild.
	entries := func(n int) []dueEntry {
		es := make([]dueEntry, n)
		for i := 0; i < n; i++ {
			es[i] = dueEntry{callbackId: uint32(i), due: base + int64(i%7)*int64(time.Second), schedGen: 1}
		}
		return es
	}

	tests := []struct {
		name    string
		entries []dueEntry
		keep    func(dueEntry) bool
		wantIds []uint32 // surviving callbackIds, in any order (nil = none)
	}{
		{
			name:    "empty heap",
			entries: nil,
			keep:    func(dueEntry) bool { return true },
			wantIds: nil,
		},
		{
			name:    "keep none",
			entries: entries(10),
			keep:    func(dueEntry) bool { return false },
			wantIds: nil,
		},
		{
			name:    "keep all",
			entries: entries(5),
			keep:    func(dueEntry) bool { return true },
			wantIds: []uint32{0, 1, 2, 3, 4},
		},
		{
			name:    "keep one",
			entries: entries(6),
			keep:    func(e dueEntry) bool { return e.callbackId == 3 },
			wantIds: []uint32{3},
		},
		{
			name:    "keep even across multi-level heap",
			entries: entries(20),
			keep:    func(e dueEntry) bool { return e.callbackId%2 == 0 },
			wantIds: []uint32{0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &dueHeap{}
			for _, e := range tt.entries {
				h.push(e)
			}

			h.compact(tt.keep)
			checkHeapInvariant(t, *h)

			gotIds := make([]uint32, 0, len(*h))
			for _, e := range *h {
				gotIds = append(gotIds, e.callbackId)
			}
			assert.ElementsMatch(t, tt.wantIds, gotIds, "survivors")

			var prev int64 = -1
			for len(*h) > 0 {
				e := h.pop()
				assert.GreaterOrEqual(t, e.due, prev, "pop out of order")
				prev = e.due
			}
		})
	}
}

// TestDueHeap_PushAfterCompact checks that a push onto a just-compacted heap
// sifts correctly, since compact rebuilds the heap and push assumes a valid one.
func TestDueHeap_PushAfterCompact(t *testing.T) {
	base := time.Now().UnixNano()
	h := &dueHeap{}
	for i := 0; i < 12; i++ {
		h.push(dueEntry{callbackId: uint32(i), due: base + int64(i%5)*int64(time.Second), schedGen: 1})
	}

	h.compact(func(e dueEntry) bool { return e.callbackId%3 == 0 })
	checkHeapInvariant(t, *h)

	h.push(dueEntry{callbackId: 100, due: base - int64(time.Second), schedGen: 1}) // new minimum
	checkHeapInvariant(t, *h)
	h.push(dueEntry{callbackId: 101, due: base + int64(10*time.Second), schedGen: 1}) // new maximum
	checkHeapInvariant(t, *h)

	require.Equal(t, uint32(100), (*h)[0].callbackId, "new minimum must sift to the root")

	var prev int64 = -1
	for len(*h) > 0 {
		e := h.pop()
		assert.GreaterOrEqual(t, e.due, prev, "pop out of order after compact+push")
		prev = e.due
	}
}

// FuzzDueHeap drives random push/pop/compact sequences, checking after every
// operation that the heap invariant holds and that the live set matches an
// independent reference model.
func FuzzDueHeap(f *testing.F) {
	f.Add([]byte{0, 5, 1, 3, 2, 0, 0, 7, 1, 9})
	f.Add([]byte{0, 0, 0, 0, 0, 2, 1, 1, 2, 3, 3, 3})

	f.Fuzz(func(t *testing.T, ops []byte) {
		h := &dueHeap{}
		var model []dueEntry // reference multiset of live entries
		var nextId uint32

		for i := 0; i+1 < len(ops); i += 2 {
			switch ops[i] % 3 {
			case 0: // push
				e := dueEntry{callbackId: nextId, due: int64(ops[i+1]), schedGen: 1}
				nextId++
				h.push(e)
				model = append(model, e)
			case 1: // pop (min due)
				if len(model) == 0 {
					continue
				}
				got := h.pop()
				// Ties may resolve to any entry sharing the min due, so match on
				// due rather than identity.
				minDue := model[0].due
				for _, e := range model {
					if e.due < minDue {
						minDue = e.due
					}
				}
				require.Equal(t, minDue, got.due, "pop did not return the minimum due")
				removed := false
				for j, e := range model {
					if e == got {
						model = append(model[:j], model[j+1:]...)
						removed = true
						break
					}
				}
				require.True(t, removed, "popped an entry not in the model")
			case 2: // compact: keep entries with even due
				keep := func(e dueEntry) bool { return e.due%2 == 0 }
				h.compact(keep)
				kept := model[:0]
				for _, e := range model {
					if keep(e) {
						kept = append(kept, e)
					}
				}
				model = kept
			}

			checkHeapInvariant(t, *h)
			require.Equal(t, len(model), len(*h), "heap size diverged from model")
		}

		// Final drain must equal the model sorted by due.
		wantDue := make([]int64, len(model))
		for i, e := range model {
			wantDue[i] = e.due
		}
		sort.Slice(wantDue, func(i, j int) bool { return wantDue[i] < wantDue[j] })
		gotDue := make([]int64, 0, len(*h))
		for len(*h) > 0 {
			gotDue = append(gotDue, h.pop().due)
		}
		require.Equal(t, wantDue, gotDue, "final drain order")
	})
}

func TestComputeNextDue(t *testing.T) {
	g := &cycleCallbackGroup{epoch: time.Now()}
	started := time.Now()
	interval := 5 * time.Second

	tests := []struct {
		name string
		meta *cycleCallbackMeta
		want int64
	}{
		{
			name: "nil interval is always due at started",
			meta: &cycleCallbackMeta{started: started, intervals: nil},
			want: started.Sub(g.epoch).Nanoseconds(),
		},
		{
			name: "interval adds Get() to started",
			meta: &cycleCallbackMeta{started: started, intervals: NewFixedIntervals(interval)},
			want: started.Add(interval).Sub(g.epoch).Nanoseconds(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, g.computeNextDue(tt.meta))
		})
	}
}
