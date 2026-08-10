//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
)

// Keys here are shuffled, never strided. An ascending fixture puts pdqsort on
// its already-sorted fast path and reports a cost that does not exist in
// production, which is how a 9ms sort stayed invisible to the DocIDs
// benchmarks for as long as it did.

// sortSlabInterface is the pre-radix implementation, kept as the oracle every
// arm is checked against and as the baseline the benchmarks quote.
func sortSlabInterface(slab []byte, w int) {
	n := len(slab) / w
	if n < 2 {
		return
	}
	sort.Sort(&fixedWidthKeys{slab: slab, w: w, n: n, scratch: make([]byte, w)})
}

func randomFixedSlab(tb testing.TB, n, w int) []byte {
	tb.Helper()
	slab := make([]byte, n*w)
	rng := rand.New(rand.NewSource(20260807))
	for i := 0; i < n; i++ {
		key := slab[i*w : (i+1)*w]
		if w == 8 {
			binary.BigEndian.PutUint64(key, rng.Uint64())
			continue
		}
		for j := range key {
			key[j] = byte(rng.Intn(256))
		}
	}
	return slab
}

func narrowSlab(n, span int) []byte {
	slab := make([]byte, n*8)
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(slab[i*8:], uint64(rng.Intn(span)))
	}
	return slab
}

func shapeKeys(n int, f func(i int) string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = f(i)
	}
	rng := rand.New(rand.NewSource(9))
	rng.Shuffle(n, func(a, b int) { out[a], out[b] = out[b], out[a] })
	return out
}

func buildVar(keys []string) ([]byte, []uint32) {
	offs := make([]uint32, len(keys)+1)
	var slab []byte
	for i, k := range keys {
		slab = append(slab, k...)
		offs[i+1] = uint32(len(slab))
	}
	return slab, offs
}

func keysOf(slab []byte, offs []uint32) []string {
	out := make([]string, len(offs)-1)
	for i := range out {
		out[i] = string(slab[offs[i]:offs[i+1]])
	}
	return out
}

func requireSlabEqual(tb testing.TB, got, want []byte, w int) {
	tb.Helper()
	if bytes.Equal(got, want) {
		return
	}
	for i := 0; i < len(want)/w; i++ {
		g, e := got[i*w:(i+1)*w], want[i*w:(i+1)*w]
		if !bytes.Equal(g, e) {
			tb.Fatalf("slab differs at key %d: got %x want %x", i, g, e)
		}
	}
	tb.Fatal("slabs differ")
}

// TestSortFixedWidthMatchesInterface covers every fixed-width arm against the
// implementation it replaces.
func TestSortFixedWidthMatchesInterface(t *testing.T) {
	widths := []int{1, 8, 14, 16, 20}
	// Sizes straddle radixCutoff so both the comparison and radix arms are
	// exercised. Update these if the constant moves — they were {255, 256}
	// when it was 256, and silently stopped covering the boundary when it
	// became 64.
	sizes := []int{0, 1, 2, 3, 63, 64, 65, 255, 256, 10_000}
	for _, w := range widths {
		for _, n := range sizes {
			slab := randomFixedSlab(t, n, w)
			want := bytes.Clone(slab)
			sortSlabInterface(want, w)
			got := bytes.Clone(slab)
			sortFixedWidth(got, w)
			requireSlabEqual(t, got, want, w)
		}
	}
	// Narrow ranges drive radixU64's skip-constant-byte path; span 2 is the
	// boolean-like extreme where all but one byte is constant.
	for _, span := range []int{2, 1_000, 1_000_000} {
		slab := narrowSlab(50_000, span)
		want := bytes.Clone(slab)
		sortSlabInterface(want, 8)
		got := bytes.Clone(slab)
		sortFixedWidth(got, 8)
		requireSlabEqual(t, got, want, 8)
	}
}

// TestSortKeysAcrossShapes walks the dispatch: shared prefix or not, narrow or
// wide, fixed or variable length.
func TestSortKeysAcrossShapes(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	shapes := []struct {
		name string
		keys []string
	}{
		{"fixed 14B, shared prefix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("hotel_%08d", i) })},
		{"fixed 8B, no shared prefix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("%08d", rng.Intn(99999999)) })},
		{"fixed 20B, no shared prefix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("%020d", rng.Int63()) })},
		{"fixed 20B, shared prefix, wide suffix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("pre_%016d", rng.Int63()) })},
		{"fixed 16B, uuid-shaped", shapeKeys(5000, func(i int) string { return fmt.Sprintf("%016x", rng.Int63()) })},
		{"variable length, shared prefix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("item_%d", i) })},
		{"variable length, no shared prefix", shapeKeys(5000, func(i int) string { return fmt.Sprintf("%c%d", rune('a'+i%26), i) })},
		{"variable, one key prefixes another", []string{"ab", "abc", "a", "abcd", "zz"}},
		{"all identical", shapeKeys(500, func(i int) string { return "same" })},
		{"below the radix cutoff", shapeKeys(radixCutoff-1, func(i int) string { return fmt.Sprintf("k%04d", i) })},
		{"at the radix cutoff", shapeKeys(radixCutoff, func(i int) string { return fmt.Sprintf("k%04d", i) })},
		{"two keys", []string{"b", "a"}},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			want := slices.Clone(sh.keys)
			slices.Sort(want)

			slab, offs := buildVar(sh.keys)
			sortKeys(slab, offs)
			got := keysOf(slab, offs)

			if !slices.Equal(got, want) {
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("first mismatch at %d: got %q want %q", i, got[i], want[i])
					}
				}
				t.Fatal("mismatch")
			}
		})
	}
}

func TestUniformWidthOf(t *testing.T) {
	cases := []struct {
		name string
		offs []uint32
		want int
	}{
		{"uniform 14", []uint32{0, 14, 28, 42}, 14},
		{"varying", []uint32{0, 2, 5, 6}, 0},
		{"empty keys", []uint32{0, 0, 0}, 0},
		{"single key", []uint32{0, 7}, 7},
		{"too short", []uint32{0}, 0},
		{"uniform then not", []uint32{0, 4, 8, 11}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uniformWidthOf(tc.offs); got != tc.want {
				t.Fatalf("uniformWidthOf(%v) = %d, want %d", tc.offs, got, tc.want)
			}
		})
	}
}
