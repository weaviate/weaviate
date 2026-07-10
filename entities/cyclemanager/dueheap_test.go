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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDueHeap_PopOrder(t *testing.T) {
	base := time.Now().UnixNano()

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &dueHeap{}
			for _, e := range tt.entries {
				h.push(e)
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

	h := &dueHeap{}
	for i := 0; i < 20; i++ {
		// Interleave "keep" (even) and "drop" (odd) ids across a range of due
		// times so compaction has a multi-level heap to rebuild.
		h.push(dueEntry{callbackId: uint32(i), due: base + int64(i%7)*int64(time.Second), schedGen: 1})
	}

	h.compact(func(e dueEntry) bool { return e.callbackId%2 == 0 })

	// Only even ids survive.
	require.Len(t, *h, 10)
	for _, e := range *h {
		assert.Zero(t, e.callbackId%2, "odd id %d should have been dropped", e.callbackId)
	}

	// The invariant is restored: pops come out in non-decreasing due order.
	var prev int64 = -1
	for len(*h) > 0 {
		e := h.pop()
		assert.GreaterOrEqual(t, e.due, prev, "pop out of order")
		prev = e.due
	}
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
