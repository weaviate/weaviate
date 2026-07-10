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

// dueEntry is a single heap slot. The schedGen field lets drainDue discard
// stale entries without an explicit remove.
//
// due is stored as Unix-nanoseconds (int64) rather than time.Time on purpose:
// time.Time embeds a *Location pointer, which turns every heap swap into a GC
// write barrier — a large cost when a tick sifts many entries. An int64 key
// makes dueEntry pointer-free, so swaps are plain memory moves and comparisons
// are integer compares.
type dueEntry struct {
	callbackId uint32
	due        int64
	schedGen   uint64
}

// dueHeap is a min-heap of dueEntry ordered by due (earliest first). The
// heap operations are implemented directly on the concrete slice, rather than
// through container/heap, so push/pop pass dueEntry by value and never box it
// into an any — container/heap's any-based Push/Pop allocate on every call.
type dueHeap []dueEntry

// push adds e and sifts it up to restore the min-heap invariant.
func (h *dueHeap) push(e dueEntry) {
	*h = append(*h, e)
	a := *h
	i := len(a) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if a[i].due >= a[parent].due {
			break
		}
		a[i], a[parent] = a[parent], a[i]
		i = parent
	}
}

// pop removes and returns the earliest-due entry. Caller must check Len() > 0.
func (h *dueHeap) pop() dueEntry {
	a := *h
	n := len(a) - 1
	a[0], a[n] = a[n], a[0]
	e := a[n]
	*h = a[:n]
	if n > 0 {
		h.down(0)
	}
	return e
}

// down sifts the element at index i down to restore the min-heap invariant.
func (h dueHeap) down(i int) {
	n := len(h)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && h[right].due < h[left].due {
			smallest = right
		}
		if h[smallest].due >= h[i].due {
			break
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}

// computeNextDue returns when meta is next eligible to run, as Unix-nanoseconds.
func computeNextDue(m *cycleCallbackMeta) int64 {
	if m.intervals == nil {
		return m.started.UnixNano()
	}
	return m.started.Add(m.intervals.Get()).UnixNano()
}
