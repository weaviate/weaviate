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
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDueHeap_PopOrder(t *testing.T) {
	base := time.Now()

	type entry struct {
		name    string
		nextDue time.Time
	}

	tests := []struct {
		name    string
		entries []entry
		want    []string // names in exact pop order, when order is fully determined
		// names that must occupy the first len(wantPrefix) pops, in any order
		// (used when some entries share a due time)
		wantPrefix []string
	}{
		{
			name:    "empty",
			entries: nil,
			want:    nil,
		},
		{
			name:    "single",
			entries: []entry{{"a", base}},
			want:    []string{"a"},
		},
		{
			name: "ascending input",
			entries: []entry{
				{"a", base},
				{"b", base.Add(time.Second)},
				{"c", base.Add(2 * time.Second)},
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "descending input",
			entries: []entry{
				{"c", base.Add(2 * time.Second)},
				{"b", base.Add(time.Second)},
				{"a", base},
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "shuffled with equal due times",
			entries: []entry{
				{"late", base.Add(3 * time.Second)},
				{"early1", base},
				{"mid", base.Add(time.Second)},
				{"early2", base},
			},
			// two entries share base; they must pop before mid/late, but their
			// relative order within the tie is unspecified.
			wantPrefix: []string{"early1", "early2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &dueHeap{}
			heap.Init(h)
			for _, e := range tt.entries {
				heap.Push(h, &cycleCallbackMeta{customId: e.name, nextDue: e.nextDue})
			}

			var popped []time.Time
			var names []string
			for h.Len() > 0 {
				meta := heap.Pop(h).(*cycleCallbackMeta)
				popped = append(popped, meta.nextDue)
				names = append(names, meta.customId)
			}

			// pop order is always non-decreasing in nextDue
			for i := 1; i < len(popped); i++ {
				assert.False(t, popped[i].Before(popped[i-1]),
					"pop %d (%v) before pop %d (%v)", i, popped[i], i-1, popped[i-1])
			}
			if tt.want != nil {
				assert.Equal(t, tt.want, names)
			}
			if tt.wantPrefix != nil {
				require.GreaterOrEqual(t, len(names), len(tt.wantPrefix))
				assert.ElementsMatch(t, tt.wantPrefix, names[:len(tt.wantPrefix)])
			}
		})
	}
}

func TestDueHeap_HeapIndexConsistency(t *testing.T) {
	base := time.Now()
	h := &dueHeap{}
	heap.Init(h)

	metas := []*cycleCallbackMeta{
		{callbackId: 3, customId: "d", nextDue: base.Add(3 * time.Second)},
		{callbackId: 0, customId: "a", nextDue: base},
		{callbackId: 2, customId: "c", nextDue: base.Add(2 * time.Second)},
		{callbackId: 1, customId: "b", nextDue: base.Add(time.Second)},
	}
	for _, m := range metas {
		heap.Push(h, m)
	}

	// every queued meta's heapIndex points back at its slot
	for i, m := range *h {
		assert.Equal(t, i, m.heapIndex, "heapIndex mismatch for %s", m.customId)
	}

	// the top of the heap is the earliest-due entry, carrying its callbackId
	// intact for the lazy map lookup on pop
	assert.Equal(t, uint32(0), (*h)[0].callbackId)

	// remove the middle element by its tracked index; remaining indices stay valid
	victim := metas[2] // "c"
	require.GreaterOrEqual(t, victim.heapIndex, 0)
	heap.Remove(h, victim.heapIndex)
	assert.Equal(t, -1, victim.heapIndex, "removed meta keeps heapIndex -1")
	for i, m := range *h {
		assert.Equal(t, i, m.heapIndex, "heapIndex mismatch after remove for %s", m.customId)
	}

	// draining sets heapIndex -1 on every popped meta and preserves due order
	var prev time.Time
	first := true
	for h.Len() > 0 {
		meta := heap.Pop(h).(*cycleCallbackMeta)
		assert.Equal(t, -1, meta.heapIndex)
		if !first {
			assert.False(t, meta.nextDue.Before(prev))
		}
		prev, first = meta.nextDue, false
	}
}

func TestComputeNextDue(t *testing.T) {
	started := time.Now()
	interval := 5 * time.Second

	tests := []struct {
		name string
		meta *cycleCallbackMeta
		want time.Time
	}{
		{
			name: "nil interval is always due at started",
			meta: &cycleCallbackMeta{started: started, intervals: nil},
			want: started,
		},
		{
			name: "interval adds Get() to started",
			meta: &cycleCallbackMeta{started: started, intervals: NewFixedIntervals(interval)},
			want: started.Add(interval),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, computeNextDue(tt.meta))
		})
	}
}
