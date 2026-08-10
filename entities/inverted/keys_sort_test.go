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

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keys here must be shuffled, never strided. An ascending fixture puts pdqsort
// on its already-sorted fast path and reports a cost no production batch pays.

// sortSlabInterface is the oracle every arm is checked against.
//
// It shares no code with production, which is the point: an oracle built on the
// comparison sort would leave every case that dispatches to that branch —
// d > 16 below the cutoff — asserting only that the branch agrees with itself.
// Materializing the keys and sorting the slice is slower and obviously correct,
// which is the trade an oracle wants.
func sortSlabInterface(slab []byte, w int) {
	n := len(slab) / w
	if n < 2 {
		return
	}
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = bytes.Clone(slab[i*w : (i+1)*w])
	}
	slices.SortFunc(keys, bytes.Compare)
	for i, k := range keys {
		copy(slab[i*w:], k)
	}
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

// fixedShapeKeys is shapeKeys for a fixture that claims one width, and fails if
// the keys do not have it.
//
// %0Nd pads to a MINIMUM width, so a fixture written with it stops being
// fixed-width the moment a value needs more digits. uniformWidthOf then routes
// the case to sortVariableWidth, and it passes while exercising an arm other
// than the one its name claims. Checking the width here is what keeps a case
// honest about which arm it covers.
func fixedShapeKeys(tb testing.TB, n, w int, f func(i int) string) []string {
	tb.Helper()
	keys := shapeKeys(n, f)
	for _, k := range keys {
		if len(k) != w {
			tb.Fatalf("fixture claims %d-byte keys, got %q (%d bytes)", w, k, len(k))
		}
	}
	return keys
}

// randHex draws n hex digits, every one of them significant — unlike %0Nx of an
// integer, whose leading zeros become a shared prefix and shrink d.
func randHex(rng *rand.Rand, n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hex[rng.Intn(16)]
	}
	return string(b)
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

// matchesInterfaceSizes straddles both cutoffs, so every arm is exercised on
// each side of the constant that gates it. TestCutoffsAreBracketed pins that
// it still does: a list that stops covering a cutoff keeps passing while
// testing only one side of it.
var matchesInterfaceSizes = []int{0, 1, 2, 3, 63, 64, 65, 127, 128, 129, 191, 192, 193, 255, 256, 10_000}

// TestSortFixedWidthMatchesInterface checks every fixed-width arm against a
// plain comparison sort over materialized keys.
func TestSortFixedWidthMatchesInterface(t *testing.T) {
	widths := []int{1, 8, 14, 16, 20}
	// Sizes straddle radixCutoff so both the packed and radix arms are
	// exercised. They must be updated with the constant: sizes that no longer
	// bracket it still pass, while covering only one side.
	sizes := matchesInterfaceSizes
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
		// Each fixed case names the arm it is here for. d is the width minus
		// the prefix every key shares, and it — not the width — picks the arm.
		{ // d=8 -> packedRadix, with a prefix
			"fixed 14B, 6-byte prefix",
			fixedShapeKeys(t, 5000, 14, func(i int) string { return fmt.Sprintf("hotel_%08d", i) }),
		},
		{ // d=8 -> packedRadix, no prefix
			"fixed 8B, no shared prefix",
			fixedShapeKeys(t, 5000, 8, func(i int) string { return fmt.Sprintf("%08d", rng.Intn(99999999)) }),
		},
		{ // d=16 -> widePackedRadix, no prefix
			"fixed 16B, uuid-shaped",
			fixedShapeKeys(t, 5000, 16, func(i int) string { return fmt.Sprintf("%016x", rng.Int63()) }),
		},
		{ // d=12 -> widePackedRadix WITH a prefix, which nothing else reaches
			"fixed 16B, 4-byte prefix",
			fixedShapeKeys(t, 5000, 16, func(i int) string { return fmt.Sprintf("pre_%012x", rng.Int63n(1<<48)) }),
		},
		{ // d=19 -> americanFlagSort; %020d cannot fill 20 digits from an
			// Int63, so every key shares the leading zero
			"fixed 20B, one-byte prefix",
			fixedShapeKeys(t, 5000, 20, func(i int) string { return fmt.Sprintf("%020d", rng.Int63()) }),
		},
		{ // d=20 -> americanFlagSort with a prefix worth skipping. The suffix
			// is drawn byte by byte because %020x of an int63 can only fill 16
			// digits and pads the rest, which drops d back to 16.
			"fixed 27B, 7-byte prefix",
			fixedShapeKeys(t, 5000, 27, func(i int) string { return "prefix_" + randHex(rng, 20) }),
		},
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
			slab, offs, _ = sortKeys(slab, offs)
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

// TestSortKeysCollisionRepair covers the keys the packed word cannot separate.
//
// The variable-width arm packs 8 bytes past the prefix EVERY key shares, so a
// prefix shared by only a subset survives into the packed word and that subset
// arrives from the stable radix in input order. These shapes all produce such a
// run; a repair that no-ops, or that resumes comparing past the packed bytes,
// returns them unsorted.
func TestSortKeysCollisionRepair(t *testing.T) {
	rng := rand.New(rand.NewSource(31))
	// Keys must differ in length or uniformWidthOf diverts them to the fixed
	// arm, which is not the code under test here.
	repeatTo := func(keys []string, n int) []string {
		out := make([]string, 0, n)
		for len(out) < n {
			out = append(out, keys...)
		}
		return out
	}
	cases := []struct {
		name string
		keys []string
	}{
		{"two prefix groups, shuffled", shapeKeys(5000, func(i int) string {
			if i%2 == 0 {
				return fmt.Sprintf("user_profile_settings_%d", i)
			}
			return fmt.Sprintf("user_profile_avatars_%d", i)
		})},
		{"group prefix outlives several packed words", shapeKeys(2000, func(i int) string {
			return fmt.Sprintf("shared_%s_%d",
				[]string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"}[i%2], i)
		})},
		{"many groups, few keys each", shapeKeys(5000, func(i int) string {
			return fmt.Sprintf("g%03d_aaaaaaaa_%d", i%500, i)
		})},
		{"random, no group structure", shapeKeys(5000, func(i int) string {
			return fmt.Sprintf("%08d_%d", rng.Intn(99999999), i)
		})},
		// packSuffix pads a short key with zeros, so these pack alike without
		// agreeing on 8 bytes. A repair that resumes comparing past the packed
		// word sees nil against nil, calls them equal, and leaves them as they
		// came in.
		{"zero padding against a short key", repeatTo(
			[]string{"ab", "ab\x00", "ab\x00\x00", "a", "ab\x00b", "ab\x00a"}, 6)},
		{"zero padding, run past the radix cutoff", repeatTo(
			[]string{"ab", "ab\x00", "ab\x00\x00", "a", "ab\x00b", "ab\x00a"}, 4*radixCutoff)},
		{"one key is a prefix of a long run", append(
			shapeKeys(200, func(i int) string { return "pfx" + strings.Repeat("z", 40) }),
			"pfx", "pfx"+strings.Repeat("z", 41))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := slices.Clone(tc.keys)
			slices.Sort(want)

			slab, offs := buildVar(tc.keys)
			slab, offs, _ = sortKeys(slab, offs)
			got := keysOf(slab, offs)

			// Checked before the per-key loop, which would otherwise ignore a
			// rebuild that duplicated keys rather than dropping them.
			require.Equal(t, len(want), len(got), "sortKeys must neither drop nor add keys")
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("first mismatch at %d: got %q want %q", i, got[i], want[i])
				}
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

// TestAmericanFlagSortDeepAgreement pins a stack overflow.
//
// The MSD arm advances one byte at a time whenever every key agrees on that
// byte, and must do so iteratively. One frame per byte carries three 256-entry
// arrays, about 6.2KB, so a key wide enough to hold a long agreement run would
// exhaust the goroutine stack — a fatal error, not a panic: recover cannot
// catch it and the process dies.
//
// It is reachable from a filter value. Only the text path produces keys wider
// than 16 discriminating bytes, tokenizeField trims whitespace and passes the
// value through as the key, and nothing caps its length.
//
// The shape: 64 keys (radixCutoff) of one width, all but one sharing byte 0,
// the rest zero. The global prefix is empty so the dispatch picks this arm,
// and the large bucket then agrees on every remaining byte.
func TestAmericanFlagSortDeepAgreement(t *testing.T) {
	for _, w := range []int{1_000, 20_000, 200_000} {
		n := radixCutoff
		slab := make([]byte, n*w)
		for i := 0; i < n; i++ {
			slab[i*w] = 'A'
		}
		slab[(n-1)*w] = 'B'

		want := bytes.Clone(slab)
		sortSlabInterface(want, w)
		got := bytes.Clone(slab)
		sortFixedWidth(got, w)
		requireSlabEqual(t, got, want, w)
	}
}

// prefixedSlab builds n keys of width w that all share their first lcp bytes,
// drawing the rest from a three-symbol alphabet so ties, equal keys and runs the
// pack cannot separate all occur.
func prefixedSlab(rng *rand.Rand, n, w, lcp int) []byte {
	prefix := make([]byte, lcp)
	rng.Read(prefix)
	slab := make([]byte, n*w)
	for i := 0; i < n; i++ {
		copy(slab[i*w:], prefix)
		for j := lcp; j < w; j++ {
			slab[i*w+j] = byte(rng.Intn(3))
		}
	}
	return slab
}

// TestSortFixedWidthSharedPrefix covers every arm with a prefix each key shares.
//
// The random slabs the other tests use leave the shared prefix empty almost
// always, so on their own they exercise each arm only at lcp == 0 — where the
// pack has nothing to skip and the discriminating width is the whole key. A
// prefix moves a batch to a different arm, and the small-batch arms in
// particular are only reachable with one for widths above 8.
func TestSortFixedWidthSharedPrefix(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, w := range []int{2, 5, 8, 9, 12, 14, 16, 17, 20, 24} {
		for _, lcp := range []int{0, 1, 3, 7, 8, 15} {
			if lcp >= w {
				continue
			}
			for _, n := range []int{0, 1, 2, 3, 5, 23, 24, 25, 63, 64, 65, 127, 128, 129, 191, 192, 193, 300} {
				slab := prefixedSlab(rng, n, w, lcp)
				want := bytes.Clone(slab)
				sortSlabInterface(want, w)
				got := bytes.Clone(slab)
				sortFixedWidth(got, w)
				if !bytes.Equal(got, want) {
					t.Fatalf("w=%d lcp=%d n=%d: got %x want %x", w, lcp, n, got, want)
				}
			}
		}
	}
}

// TestCountingSort1 covers the boolean arm directly. It is the one arm that
// compares nothing, so an error in it cannot show up as a mis-ordered
// comparison — only as the wrong number of each value.
func TestCountingSort1(t *testing.T) {
	tests := []struct {
		name string
		slab []byte
	}{
		{"empty", nil},
		{"one", []byte{7}},
		{"already ordered", []byte{0, 1}},
		{"reversed", []byte{1, 0}},
		{"booleans as encoded", []byte{1, 0, 1, 1, 0}},
		{"every byte value", func() []byte {
			s := make([]byte, 256)
			for i := range s {
				s[i] = byte(255 - i)
			}
			return s
		}()},
		{"all equal", bytes.Repeat([]byte{42}, 100)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := bytes.Clone(tt.slab)
			slices.Sort(want)
			got := bytes.Clone(tt.slab)
			countingSort1(got)
			assert.Equal(t, want, got)
		})
	}
}

// TestInsertionSortFixed covers the arm americanFlagSort drops into for short
// ranges, including that it starts comparing at d rather than at the key start.
func TestInsertionSortFixed(t *testing.T) {
	const w = 4
	tests := []struct {
		name string
		keys []string
		d    int
		want []string
	}{
		{"empty", nil, 0, nil},
		{"one key", []string{"abcd"}, 0, []string{"abcd"}},
		{
			"reversed",
			[]string{"dddd", "cccc", "bbbb", "aaaa"},
			0,
			[]string{"aaaa", "bbbb", "cccc", "dddd"},
		},
		{
			"equal keys hold",
			[]string{"bbbb", "aaaa", "bbbb"},
			0,
			[]string{"aaaa", "bbbb", "bbbb"},
		},
		{
			"ordered from d, not from 0",
			[]string{"zzab", "aazz"},
			2,
			[]string{"zzab", "aazz"},
		},
		{
			"depth equal to width leaves order alone",
			[]string{"bbbb", "aaaa"},
			w,
			[]string{"bbbb", "aaaa"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slab := []byte(strings.Join(tt.keys, ""))
			insertionSortFixed(slab, w, tt.d, make([]byte, w))
			assert.Equal(t, strings.Join(tt.want, ""), string(slab))
		})
	}
}

// TestSortFixedWidthAllIdentical pins d == 0, where every key is its own shared
// prefix so the pack shifts by 64 and Go's defined behaviour for an over-wide
// shift is the only thing making it a no-op.
//
// It is a case of its own because the other fixtures reach it by chance: the
// random slabs never do, and the small-alphabet ones only when a draw happens
// to come out uniform, so reordering a fixture list can silently stop covering
// it.
func TestSortFixedWidthAllIdentical(t *testing.T) {
	for _, w := range []int{1, 2, 8, 9, 16, 17, 20} {
		for _, n := range []int{2, 3, 63, 64, 65, 300} {
			slab := bytes.Repeat([]byte(strings.Repeat("k", w)), n)
			want := bytes.Clone(slab)
			sortFixedWidth(slab, w)
			requireSlabEqual(t, slab, want, w)
		}
	}
}

// TestDedupRejectsAnInversion covers the check that stands in for verifying the
// sort.
//
// It is dead under the rest of the suite by construction — a correct sort never
// produces an inversion — so the only way to show it fires, and names the pair
// it found, is to hand it a slab the sort could not have produced. Without this
// the branch could be deleted, or made to drop the smaller key silently, with
// everything still green.
func TestDedupRejectsAnInversion(t *testing.T) {
	t.Run("fixed width", func(t *testing.T) {
		tests := []struct {
			name string
			keys []string
			want string
		}{
			{"adjacent pair swapped", []string{"bb", "aa"}, "key 1 of 2"},
			{"inversion after a duplicate run", []string{"bb", "bb", "aa"}, "key 2 of 3"},
			{"inversion in the middle", []string{"aa", "cc", "bb", "dd"}, "key 2 of 4"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				slab := []byte(strings.Join(tt.keys, ""))
				n, err := dedupFixed(slab, 2, len(tt.keys))
				require.ErrorIs(t, err, ErrInternal,
					"a caller must be able to tell this from a bad filter value")
				assert.Zero(t, n, "a rejected batch must not report a count")
				assert.Contains(t, err.Error(), tt.want)
				assert.Contains(t, err.Error(), "sorts before its predecessor")
				assert.Contains(t, err.Error(), "2-byte fixed-width")
			})
		}

		// The two keys that disagreed are the only evidence of which encoding
		// broke, so the message must carry both.
		_, err := dedupFixed([]byte("bbaa"), 2, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "6161 after 6262")
	})

	t.Run("variable width", func(t *testing.T) {
		tests := []struct {
			name string
			keys []string
			want string
		}{
			{"adjacent pair swapped", []string{"bbb", "a"}, "key 1 of 2"},
			{"a prefix sorting after its extension", []string{"abc", "ab"}, "key 1 of 2"},
			{"inversion after a dropped duplicate", []string{"bb", "bb", "a"}, "key 2 of 3"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				slab, offs := buildVar(tt.keys)
				n, err := dedupVariable(slab, offs, len(tt.keys))
				require.ErrorIs(t, err, ErrInternal)
				assert.Zero(t, n)
				assert.Contains(t, err.Error(), tt.want)
				assert.Contains(t, err.Error(), "variable-width branch")
			})
		}

		_, err := dedupVariable(buildVarSlab(t, "bbb", "a"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "61 after 626262")
	})
}

// buildVarSlab is buildVar plus the key count, shaped for dedupVariable's
// signature.
func buildVarSlab(tb testing.TB, keys ...string) ([]byte, []uint32, int) {
	tb.Helper()
	slab, offs := buildVar(keys)
	return slab, offs, len(keys)
}

// TestSortFixedWidthWidthZeroWritesNothing covers d == 0, where every key is its
// own shared prefix.
//
// A sort over identical keys cannot observe this: any permutation of equal keys
// is byte-identical whatever the pack computed. So the pack's behaviour at zero
// width is asserted directly, and the slab is bracketed with sentinels to catch
// a write landing outside it.
//
// The shift itself is beyond reach: at d == 0 loadBE reads no bytes, so the
// value shifted is zero and Go's over-wide shift and the hardware's masked one
// agree. What is pinned here is that nothing is read, written, or written
// outside the slab.
func TestSortFixedWidthWidthZeroWritesNothing(t *testing.T) {
	assert.Zero(t, loadBE([]byte("kk"), 0), "a zero-width load reads nothing")
	assert.Zero(t, packSuffix([]byte("kk"), 2), "a key with no bytes past the prefix packs to zero")

	dst := []byte{0xAA}
	storeBE(dst, ^uint64(0), 0)
	assert.Equal(t, []byte{0xAA}, dst, "a zero-width store writes nothing")

	for _, w := range []int{1, 2, 8, 9, 16, 17, 20} {
		for _, n := range []int{2, 3, 63, 64, 65, 300} {
			buf := bytes.Repeat([]byte{0xAA}, w+n*w+w)
			slab := buf[w : w+n*w]
			copy(slab, bytes.Repeat([]byte(strings.Repeat("k", w)), n))
			want := bytes.Clone(slab)

			sortFixedWidth(slab, w)

			requireSlabEqual(t, slab, want, w)
			// Only the trailing sentinel can catch anything: the slab has spare
			// capacity behind it, so a write can land there without touching a
			// key. Nothing can address bytes before the slab's start.
			assert.Equal(t, bytes.Repeat([]byte{0xAA}, w), buf[w+n*w:],
				"w=%d n=%d: wrote past the slab", w, n)
		}
	}
}

// TestCutoffsAreBracketed pins that the size lists either side of each cutoff
// still straddle it. They are not decorative: raising a cutoff past every size
// tested drops the arm it gates to zero coverage while the suite stays green.
func TestCutoffsAreBracketed(t *testing.T) {
	for _, cutoff := range []int{radixCutoff, wideRadixCutoff, varRadixCutoff} {
		assert.Contains(t, matchesInterfaceSizes, cutoff-1, "no size below cutoff %d", cutoff)
		assert.Contains(t, matchesInterfaceSizes, cutoff, "no size at cutoff %d", cutoff)
		assert.Contains(t, matchesInterfaceSizes, cutoff+1, "no size above cutoff %d", cutoff)
	}
}
