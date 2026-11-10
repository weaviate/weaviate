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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotLayoutGroupsVectorsByPosting(t *testing.T) {
	tests := []struct {
		name       string
		primary    []uint32
		postings   int
		wantStart  []uint32
		wantOrder  []uint32
		wantSlotOf []uint32
	}{
		{
			name:       "already grouped",
			primary:    []uint32{0, 0, 1, 2},
			postings:   3,
			wantStart:  []uint32{0, 2, 3, 4},
			wantOrder:  []uint32{0, 1, 2, 3},
			wantSlotOf: []uint32{0, 1, 2, 3},
		},
		{
			name:       "interleaved, input order kept within a posting",
			primary:    []uint32{2, 0, 2, 1, 0},
			postings:   3,
			wantStart:  []uint32{0, 2, 3, 5},
			wantOrder:  []uint32{1, 4, 3, 0, 2},
			wantSlotOf: []uint32{3, 0, 4, 2, 1},
		},
		{
			name:       "a posting without vectors owns an empty range",
			primary:    []uint32{2, 0},
			postings:   3,
			wantStart:  []uint32{0, 1, 1, 2},
			wantOrder:  []uint32{1, 0},
			wantSlotOf: []uint32{1, 0},
		},
		{
			name:       "a single posting",
			primary:    []uint32{0, 0, 0},
			postings:   1,
			wantStart:  []uint32{0, 3},
			wantOrder:  []uint32{0, 1, 2},
			wantSlotOf: []uint32{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := newSlotLayout(tt.primary, tt.postings)
			assert.Equal(t, tt.wantStart, layout.start)
			assert.Equal(t, tt.wantOrder, layout.order)
			assert.Equal(t, tt.wantSlotOf, layout.slotOf)

			postingOf := postingOfSlots(layout.start)
			require.Len(t, postingOf, len(tt.primary))
			for input, p := range tt.primary {
				assert.Equalf(t, p, postingOf[layout.slotOf[input]],
					"the slot of input %d must fall in its posting", input)
			}
		})
	}
}

func TestPostingStoreLoadsOnePostingAtATime(t *testing.T) {
	// 3 postings of 2, 0 and 3 members, codes of 2 words.
	store := &postingStore{
		codeWords: 2,
		start:     []uint32{0, 2, 2, 5},
		docIDs:    []uint64{10, 11, 12, 13, 14},
		codes:     []uint64{100, 101, 110, 111, 120, 121, 130, 131, 140, 141},
		edgeStart: []uint32{0, 1, 3, 3, 4, 6},
		edges:     []uint32{2, 3, 4, 0, 0, 1},
	}
	side := &sidecar{codeWords: 2, codes: store.codes, postingOf: postingOfSlots(store.start)}

	require.Equal(t, 3, store.postings())
	require.Equal(t, 5, store.vectors())

	first := store.load(0)
	assert.Equal(t, uint32(0), first.first)
	assert.Equal(t, []uint64{10, 11}, first.docIDs)
	assert.Equal(t, []uint64{100, 101, 110, 111}, first.codes)
	assert.Equal(t, []uint32{2, 3, 4}, first.edges)

	empty := store.load(1)
	assert.Empty(t, empty.docIDs)
	assert.Empty(t, empty.codes)
	assert.Empty(t, empty.edges)

	last := store.load(2)
	assert.Equal(t, uint32(2), last.first)
	assert.Equal(t, []uint64{12, 13, 14}, last.docIDs)
	assert.Equal(t, []uint64{120, 121, 130, 131, 140, 141}, last.codes)
	assert.Equal(t, []uint32{0, 0, 1}, last.edges)

	assert.Equal(t, []uint32{3, 4}, store.edgesOf(1))
	assert.Empty(t, store.edgesOf(2))

	// The sidecar serves the very same codes by slot, and cannot be
	// appended through into the next code.
	assert.Equal(t, []uint64{130, 131}, side.code(3))
	assert.Equal(t, 2, cap(side.code(3)))
	assert.Equal(t, []uint32{0, 0, 2, 2, 2}, side.postingOf)
}

func TestSidecarTouchReadsEveryCacheLineOfACode(t *testing.T) {
	tests := []struct {
		name      string
		codeWords int
		wantRead  []int // words of a code that must be read
	}{
		{name: "a code within one cache line", codeWords: 5, wantRead: []int{0, 4}},
		{name: "a code of exactly one cache line", codeWords: 8, wantRead: []int{0, 7}},
		{name: "a 1536 dimension code", codeWords: 25, wantRead: []int{0, 8, 16, 24}},
		{name: "a single word code", codeWords: 1, wantRead: []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const slots = 6
			// Every word is a distinct bit, so the digest names exactly the
			// words that were read.
			side := &sidecar{codeWords: tt.codeWords, codes: make([]uint64, slots*tt.codeWords)}
			bit := func(slot uint32, word int) uint64 {
				return 1 << (uint(int(slot)*tt.codeWords+word) % 64)
			}
			require.LessOrEqual(t, 2*tt.codeWords, 64, "the fixture needs one bit per word of two codes")

			for _, picked := range [][]uint32{{0}, {1}, {0, 1}, {}} {
				clear(side.codes)
				var want uint64
				for _, slot := range picked {
					for word := 0; word < tt.codeWords; word++ {
						side.codes[int(slot)*tt.codeWords+word] = bit(slot, word)
					}
					for _, word := range tt.wantRead {
						want |= bit(slot, word)
					}
				}
				assert.Equalf(t, want, side.touch(picked), "slots %v", picked)
			}
		})
	}
}

func TestTargetPostingSize(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		codeBytes  int
		want       int
	}{
		{name: "an explicit size wins", configured: 50, codeBytes: 200, want: 50},
		{name: "1536 dimensions fill the budget with ~236 vectors", configured: 0, codeBytes: 200, want: 236},
		{name: "768 dimensions", configured: 0, codeBytes: 104, want: 438},
		{name: "a code larger than the budget still yields a posting", configured: 0, codeBytes: 1 << 20, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, targetPostingSize(tt.configured, tt.codeBytes))
		})
	}
}
