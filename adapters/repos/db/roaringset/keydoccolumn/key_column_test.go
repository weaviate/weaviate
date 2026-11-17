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

package keydoccolumn

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	entinverted "github.com/weaviate/weaviate/entities/inverted"

	"github.com/stretchr/testify/require"
)

// prefixCorpus builds key_000.. under docID i, so buildKeyColumn selects prefixKeyColumn.
func prefixCorpus(t *testing.T, n int) *columnRun {
	t.Helper()
	keys := make([][]byte, n)
	docs := make([]uint64, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%03d", i))
		docs[i] = uint64(i)
	}
	run := runFromPairs(keys, docs)
	_, ok := run.keys.(*prefixKeyColumn)
	require.True(t, ok, "corpus must select prefixKeyColumn for this test to mean anything")
	return run
}

// TestPrefixColumnShortQueryKeys pins query keys shorter than the elided prefix resolving in range on both scanInto branches.
func TestPrefixColumnShortQueryKeys(t *testing.T) {
	const corpusSize = 64 // small enough that the dense queries below step
	run := prefixCorpus(t, corpusSize)
	require.Equal(t, []byte("key_0"), run.keys.(*prefixKeyColumn).prefix)

	// dense brackets the first 16 corpus keys with below and above, enough for the scan to step
	dense := func(below, above []string) (entinverted.SortedKeys, []uint64) {
		kb := entinverted.NewVarKeyBuilder(len(below)+16+len(above), 0)
		for _, b := range below {
			kb.AppendString(b)
		}
		docs := make([]uint64, 16)
		for i := 0; i < 16; i++ {
			kb.AppendString(fmt.Sprintf("key_%03d", i))
			docs[i] = uint64(i)
		}
		for _, a := range above {
			kb.AppendString(a)
		}
		keys, err := kb.Build()
		require.NoError(t, err)
		return keys, docs
	}

	denseBelow, denseBelowWant := dense([]string{"ke"}, nil)
	denseBoth, denseBothWant := dense([]string{"", "ke", "key_"}, []string{"kez", "kf"})

	tests := []struct {
		name string
		keys entinverted.SortedKeys
		want []uint64
		step bool
	}{
		{
			name: "short key alone",
			keys: testKeys(t, "ke"),
		},
		{
			name: "short key below the prefix, mixed with a hit",
			keys: testKeys(t, "ke", "key_007"),
			want: []uint64{7},
		},
		{
			name: "key exactly as long as the prefix",
			keys: testKeys(t, "key_0", "key_007"),
			want: []uint64{7},
		},
		{
			name: "empty key",
			keys: testKeys(t, "", "key_007"),
			want: []uint64{7},
		},
		{
			name: "short key above the prefix",
			keys: testKeys(t, "key_007", "kez"),
			want: []uint64{7},
		},
		{
			name: "short key below the prefix, dense query",
			keys: denseBelow,
			want: denseBelowWant,
			step: true,
		},
		{
			name: "short keys on both sides of the prefix, dense query",
			keys: denseBoth,
			want: denseBothWant,
			step: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gallop, _ := scanStrategy(run.keys, tt.keys.Len(), corpusSize)
			require.Equal(t, tt.step, !gallop,
				"test case must exercise the branch it claims")

			res := newResolution(tt.keys.Len(), 1_000_000)
			run.scanInto(tt.keys, res, true)

			got := heldDocs(res)
			if len(got) == 0 {
				got = nil // the cases that match nothing want no documents, not an empty slice
			}
			require.Equal(t, tt.want, got)
		})
	}
}

// TestComparePrefix pins comparePrefix agreeing with byte order, which queryWindow's binary search relies on.
func TestComparePrefix(t *testing.T) {
	prefix := []byte("key_0")

	tests := []struct {
		name string
		key  string
		want int
	}{
		{name: "empty", key: "", want: -1},
		{name: "shorter, below", key: "ke", want: -1},
		{name: "shorter, proper prefix of the prefix", key: "key_", want: -1},
		{name: "shorter, above", key: "kf", want: 1},
		{name: "exactly the prefix", key: "key_0", want: 0},
		{name: "carries the prefix", key: "key_012", want: 0},
		{name: "longer, below", key: "key_-xx", want: -1},
		{name: "longer, above", key: "key_1xx", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, comparePrefix([]byte(tt.key), prefix))
		})
	}
}

// TestQueryHeadMatchesLoadHead pins queryHead reading the same head as loadHead of the copied suffix.
func TestQueryHeadMatchesLoadHead(t *testing.T) {
	tests := []struct {
		name string
		byte func(i int) byte
	}{
		{name: "ascending from 0x00", byte: func(i int) byte { return byte(i) }},
		{name: "descending from 0xff", byte: func(i int) byte { return 0xff - byte(i) }},
		{name: "alternating 0x00 and 0xff", byte: func(i int) byte { return byte(-(i % 2)) }},
		{name: "all 0xff", byte: func(int) byte { return 0xff }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for n := 0; n <= 12; n++ {
				full := make([]byte, n)
				for i := range full {
					full[i] = tt.byte(i)
				}
				for skip := 0; skip <= n; skip++ {
					r := n - skip
					require.Equal(t, loadHead(full[skip:], 0, r, headMask(r)), queryHead(full, skip),
						"len %d skip %d", n, skip)
				}
				if n >= 8 {
					require.Zero(t, queryHead(full, n), "len %d: an empty suffix shifts by 64, which must give 0", n)
				}
			}
		})
	}
}

// TestFixedColumnWideKeys pins rank access on fixedKeyColumn above one byte wide.
func TestFixedColumnWideKeys(t *testing.T) {
	var keys [][]byte
	var docs []uint64
	for i := -32; i < 32; i++ { // spanning zero flips the first byte, so no prefix is shared
		k, err := entinverted.LexicographicallySortableInt64(int64(i))
		require.NoError(t, err)
		keys = append(keys, k)
		docs = append(docs, uint64(i+32))
	}
	run := runFromPairs(keys, docs)
	_, ok := run.keys.(*fixedKeyColumn)
	require.True(t, ok, "corpus must select fixedKeyColumn for this test to mean anything")

	// a sparse and a dense query, one per scanInto branch
	for _, picks := range [][]int{{3, 40}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 63}} {
		kb := entinverted.NewVarKeyBuilder(len(picks), 0)
		want := make([]uint64, 0, len(picks))
		for _, p := range picks {
			kb.AppendString(string(keys[p]))
			want = append(want, uint64(p))
		}
		q, err := kb.Build()
		require.NoError(t, err)
		gallop, _ := scanStrategy(run.keys, q.Len(), len(keys))
		require.Equal(t, len(picks) <= 2, gallop, "each query must exercise the branch it claims")
		res := newResolution(q.Len(), 1_000)
		run.scanInto(q, res, true)
		require.Equal(t, want, heldDocs(res))
	}
}

// keyColumnCase is a sorted corpus and the backing buildKeyColumn must pick for it.
type keyColumnCase struct {
	name    string
	keys    []string
	backing keyColumn
}

// keyColumnCases cover each backing, keys whose first eight bytes tie, and corpora long enough to gallop.
func keyColumnCases() []keyColumnCase {
	// wide is a 16-byte key whose first eight bytes are head and zero padding
	wide := func(head string, tail byte) string { return head + strings.Repeat("\x00", 14) + string([]byte{tail}) }
	// many returns n keys from key, sorted, enough for a gallop to double several times
	many := func(n int, key func(i int) string) []string {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = key(i)
		}
		sort.Strings(keys)
		return keys
	}

	return []keyColumnCase{
		{
			name:    "blob, padding ties",
			keys:    []string{"", "a", "ab", "ab\x00", "ab\x00\x01", "ab\x01", "abcdefgh", "abcdefgh\x00", "abcdefghi", "b"},
			backing: &blobKeyColumn{},
		},
		{
			name:    "fixed, narrower than a head",
			keys:    []string{"\x00\x00\x01", "\x00\x01\x00", "\x01\x00\xff", "\xff\xff\xff"},
			backing: &fixedKeyColumn{},
		},
		{
			name:    "fixed, exactly a head",
			keys:    []string{"\x00abcdefg", "\x00abcdefh", "\x7fzzzzzzz", "\xff\xff\xff\xff\xff\xff\xff\xff"},
			backing: &fixedKeyColumn{},
		},
		{
			name:    "fixed, wider than a head with shared heads",
			keys:    []string{wide("\x00", 1), wide("\x00", 2), wide("\x00", 3), wide("\x01", 0), wide("\xff", 0), wide("\xff", 0xff)},
			backing: &fixedKeyColumn{},
		},
		{
			name:    "prefix, narrow suffix",
			keys:    []string{"key_000", "key_001", "key_010", "key_0ff"},
			backing: &prefixKeyColumn{},
		},
		{
			name:    "prefix, wide suffix with shared heads",
			keys:    []string{"p" + wide("a", 1), "p" + wide("a", 2), "p" + wide("b", 0), "p" + wide("c", 0xff)},
			backing: &prefixKeyColumn{},
		},
		{
			name:    "blob, enough keys to gallop",
			keys:    many(400, func(i int) string { return fmt.Sprintf("%d", i) }),
			backing: &blobKeyColumn{},
		},
		{
			name:    "fixed, enough keys to gallop",
			keys:    many(300, func(i int) string { return string([]byte{byte(i * 200 >> 8), byte(i * 200)}) }),
			backing: &fixedKeyColumn{},
		},
	}
}

// run builds the case's corpus with key i under docID i.
func (tc keyColumnCase) run(t *testing.T) (*columnRun, [][]byte) {
	t.Helper()
	keys := make([][]byte, len(tc.keys))
	docs := make([]uint64, len(tc.keys))
	for i, k := range tc.keys {
		keys[i], docs[i] = []byte(k), uint64(i)
	}
	require.True(t, sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }))
	run := runFromPairs(keys, docs)
	require.IsType(t, tc.backing, run.keys, "corpus must select the backing this case names")
	return run, keys
}

// seekColumn runs the backing's search for one key from from.
func seekColumn(col keyColumn, from int, q []byte) (int, bool) {
	switch c := col.(type) {
	case *blobKeyColumn:
		return c.seek(from, q, loadHead(q, 0, len(q), headMask(len(q))))
	case *fixedKeyColumn:
		return seekPacked(c.data, c.w, c.len(), headMask(c.w), from, q, loadHead(q, 0, len(q), headMask(len(q))))
	case *prefixKeyColumn:
		q = q[len(c.prefix):]
		return seekPacked(c.data, c.w, c.len(), headMask(c.w), from, q, loadHead(q, 0, len(q), headMask(len(q))))
	}
	panic(fmt.Sprintf("unknown backing %T", col))
}

// TestSeekMatchesByteOrder pins a backing's seek, from every valid start, agreeing with bytes.Compare on keys whose first eight bytes tie.
func TestSeekMatchesByteOrder(t *testing.T) {
	for _, tt := range keyColumnCases() {
		t.Run(tt.name, func(t *testing.T) {
			run, keys := tt.run(t)
			col := run.keys

			var prefix []byte
			if pc, ok := col.(*prefixKeyColumn); ok {
				prefix = pc.prefix
			}
			for _, q := range searchProbes(keys) {
				if !bytes.HasPrefix(q, prefix) {
					continue // queryWindow keeps these from a prefixKeyColumn
				}
				want := sort.Search(col.len(), func(i int) bool { return bytes.Compare(col.appendKey(i, nil), q) >= 0 })
				wantMatch := want < len(keys) && bytes.Equal(keys[want], q)
				for from := 0; from <= want; from++ {
					if got, match := seekColumn(col, from, q); got != want || match != wantMatch {
						require.Equal(t, []any{want, wantMatch}, []any{got, match}, "query %q from %d", q, from)
					}
				}
			}
		})
	}
}

// TestScanMatchesStoredProbes pins both scan strategies, on every backing, matching each query key to its stored row and no other.
func TestScanMatchesStoredProbes(t *testing.T) {
	for _, tt := range keyColumnCases() {
		t.Run(tt.name, func(t *testing.T) {
			run, keys := tt.run(t)
			stored := make(map[string]uint64, len(keys))
			for i, k := range keys {
				stored[string(k)] = uint64(i)
			}
			q := testKeys(t, bytesToStrings(searchProbes(keys))...)
			lo, hi := run.keys.queryWindow(q)

			// a wrong match can return a doc another position also holds, so want is per position
			want := make([]uint32, q.Len())
			for i := range want {
				want[i] = noDoc32
				if d, ok := stored[string(q.At(i))]; ok {
					want[i] = uint32(d)
				}
			}

			strategies := map[string]func(res *Resolution){
				"step":   func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, false, 0) },
				"gallop": func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, true, maxBlocksBeforeGallop) },
			}
			for name, scan := range strategies {
				res := newResolution(q.Len(), 1_000)
				scan(res)
				require.Equal(t, want, res.docs32, name)
			}
		})
	}
}

// TestScansStepOverKeys pins both strategies, on every backing, finding each query key past runs of skipped corpus keys.
func TestScansStepOverKeys(t *testing.T) {
	for _, tt := range keyColumnCases() {
		t.Run(tt.name, func(t *testing.T) {
			run, keys := tt.run(t)
			for _, stride := range []int{2, 3, 5} {
				var picked []string
				want := []uint64{}
				for i := stride - 1; i < len(keys); i += stride {
					picked = append(picked, string(keys[i]))
					want = append(want, run.docs.at(i))
				}
				if len(picked) == 0 {
					continue
				}
				q := testKeys(t, picked...)
				lo, hi := run.keys.queryWindow(q)
				for _, gallop := range []bool{false, true} {
					res := newResolution(q.Len(), 1_000)
					run.keys.scan(run, q, lo, hi, res, true, false, gallop, maxBlocksBeforeGallop)
					require.ElementsMatch(t, want, heldDocs(res), "stride %d, gallop %v", stride, gallop)
				}
			}
		})
	}
}

// TestGallopBlockStep pins galloping with a block step, on each packed backing, matching the stored rows around block edges.
func TestGallopBlockStep(t *testing.T) {
	const n = rowsPerBlock*maxBlocksBeforeGallop*8 + 5 // a partial last block
	reach := rowsPerBlock * maxBlocksBeforeGallop
	corpora := []struct {
		name string
		key  func(i int) string
	}{
		{name: "fixed", key: func(i int) string { return string([]byte{byte(i >> 7), byte(i << 1)}) }},
		{name: "prefix", key: func(i int) string { return fmt.Sprintf("key_%05d", i) }},
		// three rows per head, so heads tie and compareTied decides
		{name: "fixed, tied heads", key: func(i int) string {
			return string([]byte{byte(i / 3 >> 7), byte(i / 3 << 1)}) + "abcdef" + string([]byte{byte(i % 3)}) + strings.Repeat("\x00", 7)
		}},
	}
	// probe is a stored row, or with absent a key sorting just after it, or with past a key whose head sorts after the row's
	type probe struct {
		row    int
		absent bool
		past   bool
	}
	tests := []struct {
		name   string
		probes []probe
	}{
		{name: "present and absent keys", probes: []probe{{row: 10}, {row: 13, absent: true}, {row: 30}, {row: 50, absent: true}, {row: 51}}},
		{name: "first row", probes: []probe{{row: 0}, {row: 8}}},
		{name: "target at the block's last row", probes: []probe{{row: 10}, {row: 18}}},
		{name: "target one past the block", probes: []probe{{row: 10}, {row: 19}}},
		{name: "absent key at the block's last row", probes: []probe{{row: 10}, {row: 17, absent: true}}},
		{name: "last row", probes: []probe{{row: 10}, {row: n - 1}}},
		{name: "absent key past the last row", probes: []probe{{row: 10}, {row: n - 1, absent: true}}},
		{name: "target in a tail shorter than a block", probes: []probe{{row: n - 8}, {row: n - 1}}},
		{name: "key past the last row's head, from inside the tail", probes: []probe{{row: n - 3}, {row: n - 1, past: true}}},
		{name: "blocks ending exactly at the last row", probes: []probe{{row: n - 17}, {row: n - 1, past: true}}},
		{name: "every block, then a gallop landing on the target", probes: []probe{{row: 10}, {row: 11 + reach}, {row: 13 + reach}}},
		{name: "every block, then a gallop past them", probes: []probe{{row: 10}, {row: 16 + reach}}},
		{name: "absent key past every block", probes: []probe{{row: 10}, {row: 15 + reach, absent: true}, {row: 20 + reach}}},
		{
			name:   "far key, then near keys",
			probes: []probe{{row: 10}, {row: 11 + 3*reach}, {row: 14 + 3*reach}, {row: 25 + 3*reach}, {row: 26 + 3*reach, absent: true}},
		},
		{
			name:   "near, far, near",
			probes: []probe{{row: 1}, {row: 9}, {row: 9 + 2*reach}, {row: 10 + 2*reach}, {row: 30 + 2*reach}, {row: 30 + 4*reach}},
		},
	}

	for _, c := range corpora {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = c.key(i)
		}
		tc := keyColumnCase{keys: keys, backing: &fixedKeyColumn{}}
		if c.name == "prefix" {
			tc.backing = &prefixKeyColumn{}
		}
		// row i holds docID i, and n+i in overflow, so the scan applies overflow too
		rb := newColumnRunBuilder()
		for i, k := range keys {
			rb.append([]byte(k), uint64(i))
			rb.appendExtra(uint64(n + i))
		}
		run := rb.columnRun()
		require.IsType(t, tc.backing, run.keys, "corpus must select the backing this case names")
		for _, tt := range tests {
			t.Run(c.name+"/"+tt.name, func(t *testing.T) {
				picked := make([]string, len(tt.probes))
				want := make([]uint32, len(tt.probes))
				for i, p := range tt.probes {
					picked[i], want[i] = keys[p.row], uint32(p.row)
					if p.absent {
						picked[i], want[i] = picked[i]+"\x00", noDoc32
					}
					if p.past {
						picked[i], want[i] = picked[i]+"\xff", noDoc32
					}
				}
				q := testKeys(t, picked...)
				lo, hi := run.keys.queryWindow(q)
				// plain gallop, and gallop with blocks
				for _, s := range []struct {
					gallop    bool
					maxBlocks int
				}{{true, 0}, {true, maxBlocksBeforeGallop}} {
					res := newResolution(q.Len(), 1_000)
					run.keys.scan(run, q, lo, hi, res, true, true, s.gallop, s.maxBlocks)
					require.Equal(t, want, res.docs32, "gallop %v, maxBlocks %d", s.gallop, s.maxBlocks)
					var held []uint64
					for _, w := range want {
						if w != noDoc32 {
							held = append(held, uint64(w), uint64(n)+uint64(w))
						}
					}
					require.ElementsMatch(t, held, heldDocs(res), "overflow, gallop %v, maxBlocks %d", s.gallop, s.maxBlocks)
					// retiring what a scan added leaves nothing held
					res = newResolution(q.Len(), 1_000)
					run.keys.scan(run, q, lo, hi, res, true, true, s.gallop, s.maxBlocks)
					run.keys.scan(run, q, lo, hi, res, false, true, s.gallop, s.maxBlocks)
					require.Empty(t, heldDocs(res), "retired, gallop %v, maxBlocks %d", s.gallop, s.maxBlocks)
				}
			})
		}
	}
}

// bytesToStrings converts keys for testKeys.
func bytesToStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

// TestGallopResumesFromPreviousHit pins a galloping scan matching every hit when each search resumes at the previous answer.
func TestGallopResumesFromPreviousHit(t *testing.T) {
	const corpusSize = 512
	run := prefixCorpus(t, corpusSize)

	tests := []struct {
		name string
		keys []string
		want []uint64
	}{
		{name: "adjacent hits", keys: []string{"key_100", "key_101", "key_102", "key_103"}, want: []uint64{100, 101, 102, 103}},
		{name: "hits far apart", keys: []string{"key_000", "key_255", "key_511"}, want: []uint64{0, 255, 511}},
		{
			name: "absent keys between hits",
			keys: []string{"key_100", "key_1005", "key_101", "key_300x", "key_301", "key_4000", "key_450"},
			want: []uint64{100, 101, 301, 450},
		},
		{name: "absent key before the first", keys: []string{"key_", "key_000"}, want: []uint64{0}},
		{name: "absent keys past the last", keys: []string{"key_511", "key_6", "key_7"}, want: []uint64{511}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := testKeys(t, tt.keys...)
			gallop, _ := scanStrategy(run.keys, keys.Len(), corpusSize)
			require.True(t, gallop, "test case must gallop")

			res := newResolution(keys.Len(), 1_000)
			run.scanInto(keys, res, true)
			require.Equal(t, tt.want, heldDocs(res))
		})
	}
}

// TestScanStrategy pins the density at which scanInto switches to galloping, per backing, and the block limit it gallops with.
func TestScanStrategy(t *testing.T) {
	blob, prefix, fixed := &blobKeyColumn{}, &prefixKeyColumn{}, &fixedKeyColumn{}
	stepCorpus := 100 * maxKeySpacingForStepping
	gate := rowsPerBlock * maxBlocksBeforeGallop / 2
	tests := []struct {
		name            string
		backing         keyColumn
		numKeys, corpus int
		want            bool
		wantBlocks      int // checked only when want is set
	}{
		{name: "blob, empty corpus", backing: blob, numKeys: 5, corpus: 0, want: false},
		{name: "blob, one key under its spacing", backing: blob, numKeys: 99, corpus: stepCorpus, want: true},
		{name: "blob, at its spacing", backing: blob, numKeys: 100, corpus: stepCorpus, want: false},
		{name: "blob, one key over its spacing", backing: blob, numKeys: 101, corpus: stepCorpus, want: false},
		{name: "blob, every key", backing: blob, numKeys: 1_000, corpus: 1_000, want: false},
		{name: "blob, one key, small corpus", backing: blob, numKeys: 1, corpus: maxKeySpacingForStepping, want: false},
		{name: "blob, one key, corpus past its spacing", backing: blob, numKeys: 1, corpus: maxKeySpacingForStepping + 1, want: true},
		{name: "prefix, one key under the spacing", backing: prefix, numKeys: 99, corpus: stepCorpus, want: true, wantBlocks: maxBlocksBeforeGallop},
		{name: "prefix, at the spacing", backing: prefix, numKeys: 100, corpus: stepCorpus, want: false},
		{name: "prefix, one key over the spacing", backing: prefix, numKeys: 101, corpus: stepCorpus, want: false},
		{name: "prefix, one key at the block gate", backing: prefix, numKeys: 1, corpus: gate, want: true, wantBlocks: maxBlocksBeforeGallop},
		{name: "prefix, one key past the block gate", backing: prefix, numKeys: 1, corpus: gate + 1, want: true},
		{name: "fixed, one key under the spacing", backing: fixed, numKeys: 99, corpus: stepCorpus, want: true, wantBlocks: maxBlocksBeforeGallop},
		{name: "fixed, at the spacing", backing: fixed, numKeys: 100, corpus: stepCorpus, want: false},
		{name: "fixed, one key over the spacing", backing: fixed, numKeys: 101, corpus: stepCorpus, want: false},
		{name: "fixed, one key short of the block gate", backing: fixed, numKeys: 99, corpus: 100 * gate, want: true},
		{name: "fixed, at the block gate", backing: fixed, numKeys: 100, corpus: 100 * gate, want: true, wantBlocks: maxBlocksBeforeGallop},
		{name: "fixed, one key past the block gate", backing: fixed, numKeys: 101, corpus: 100 * gate, want: true, wantBlocks: maxBlocksBeforeGallop},
		{name: "fixed, every key", backing: fixed, numKeys: 1_000, corpus: 1_000, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gallop, maxBlocks := scanStrategy(tt.backing, tt.numKeys, tt.corpus)
			require.Equal(t, tt.want, gallop)
			if gallop {
				require.Equal(t, tt.wantBlocks, maxBlocks)
			}
		})
	}
}

// searchProbes returns every key, the key truncated, extended by a zero and a
// max byte, and with its last byte stepped either way.
func searchProbes(keys [][]byte) [][]byte {
	probes := [][]byte{{}, bytes.Repeat([]byte{0xff}, 20)}
	for _, k := range keys {
		probes = append(probes, k, append(bytes.Clone(k), 0), append(bytes.Clone(k), 0xff))
		if n := len(k); n > 0 {
			probes = append(probes, k[:n-1])
			if last := k[n-1]; last > 0 {
				probes = append(probes, append(bytes.Clone(k[:n-1]), last-1))
			}
			if last := k[n-1]; last < 0xff {
				probes = append(probes, append(bytes.Clone(k[:n-1]), last+1))
			}
		}
	}
	return probes
}

// testKeys builds ascending query keys from literals.
func testKeys(tb testing.TB, keys ...string) entinverted.SortedKeys {
	tb.Helper()
	kb := entinverted.NewVarKeyBuilder(len(keys), 0)
	for _, k := range keys {
		kb.AppendString(k)
	}
	built, err := kb.Build()
	require.NoError(tb, err)
	return built
}

// BenchmarkScanStrategies times stepping and galloping scans, each forced, by query density
// and shape, corpus size, key layout and warm or cold memory, over docIDs shuffled against key
// order. It sweeps maxKeySpacingForStepping, maxBlocksBeforeGallop and the gallopBlocks gate.
//
// Run:
//
//	go test -run '^$' -bench 'ScanStrategies/warm' -benchtime 0.3s -count 3 \
//	    ./adapters/repos/db/roaringset/keydoccolumn/
func BenchmarkScanStrategies(b *testing.B) {
	// enough columns of the largest corpus to outgrow the CPU caches
	const coldColumns = 16
	// the densest cold query, past where the packed backings stop galloping
	const maxColdDensityBP = 2000
	layouts := []struct {
		name    string
		keys    func(n int) [][]byte
		backing keyColumn
	}{
		{"prefix", func(n int) [][]byte {
			keys := make([][]byte, n)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("val_%08d", i))
			}
			return keys
		}, &prefixKeyColumn{}},
		{"fixed16", func(n int) [][]byte {
			return sortedUniqueKeys(n, func(rnd *rand.Rand) []byte {
				k := make([]byte, 16)
				rnd.Read(k)
				return k
			})
		}, &fixedKeyColumn{}},
		{"blob", func(n int) [][]byte {
			return sortedUniqueKeys(n, func(rnd *rand.Rand) []byte {
				k := make([]byte, 3+rnd.Intn(18))
				for i := range k {
					k[i] = 'a' + byte(rnd.Intn(26))
				}
				return k
			})
		}, &blobKeyColumn{}},
	}
	densitiesBP := []int{1, 3, 10, 30, 100, 200, 300, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 2000, 3300, 5000, 10000}
	// shapes pick numKeys ascending rows of n, or fewer when the density leaves no room for the shape
	shapes := []struct {
		name string
		rows func(n, numKeys int) []int
	}{
		{"even", func(n, numKeys int) []int {
			rows := make([]int, numKeys)
			for j := range rows {
				rows[j] = j * n / numKeys
			}
			return rows
		}},
		{"clustered", func(n, numKeys int) []int {
			const runLen = 10 // consecutive rows per run, runs evenly spread
			runs := (numKeys + runLen - 1) / runLen
			rows := make([]int, 0, numKeys)
			for r := 0; r < runs; r++ {
				for j := 0; j < runLen && len(rows) < numKeys; j++ {
					rows = append(rows, r*n/runs+j)
				}
			}
			return rows
		}},
		{"alternating", func(n, numKeys int) []int {
			const near = 8 // rows between a pair's keys; pairs are evenly spread
			pairs := (numKeys + 1) / 2
			rows := make([]int, 0, numKeys)
			for p := 0; p < pairs; p++ {
				rows = append(rows, p*n/pairs)
				if len(rows) < numKeys {
					rows = append(rows, p*n/pairs+near)
				}
			}
			return rows
		}},
		// a ContainsAny over near-unique values
		{"random", func(n, numKeys int) []int {
			rows := rand.New(rand.NewSource(1)).Perm(n)[:numKeys]
			slices.Sort(rows)
			return rows
		}},
	}

	// benchShapes runs every density and shape over runs, all holding keys
	benchShapes := func(b *testing.B, runs []*columnRun, keys [][]byte) {
		corpus := len(keys)
		for _, bp := range densitiesBP {
			numKeys := max(1, corpus*bp/10_000)
			if len(runs) > 1 && bp > maxColdDensityBP {
				continue // a resolution per column per goroutine outgrows memory
			}
			for _, sh := range shapes {
				rows := sh.rows(corpus, numKeys)
				if !slices.IsSorted(rows) || len(slices.Compact(slices.Clone(rows))) < len(rows) || rows[len(rows)-1] >= corpus {
					continue // too dense for this shape
				}
				name := fmt.Sprintf("density=%g%%/%s", float64(bp)/100, sh.name)
				benchScanStrategies(b, name, runs, keys, rows, nil)
				if len(runs) == 1 && corpus == 300_000 && sh.name == "even" {
					// a key one zero byte longer sorts just after its stored key, so it is absent
					benchScanStrategies(b, name+"-absent", runs, keys, rows, []byte{0})
				}
			}
		}
	}

	for _, l := range layouts {
		for _, corpus := range []int{30_000, 300_000, 3_000_000} {
			keys := l.keys(corpus)
			// columns are built inside b.Run, so a -bench filter skips building those it excludes
			b.Run(fmt.Sprintf("warm/%s/keys=%d", l.name, corpus), func(b *testing.B) {
				run := shuffledRun(keys, 0)
				require.IsType(b, l.backing, run.keys, "corpus must select the backing this layout names")
				benchShapes(b, []*columnRun{run}, keys)
			})

			if _, ok := l.backing.(*blobKeyColumn); ok || corpus != 3_000_000 {
				continue
			}
			b.Run(fmt.Sprintf("cold/%s/keys=%dx%d", l.name, corpus, coldColumns), func(b *testing.B) {
				cold := make([]*columnRun, coldColumns)
				for c := range cold {
					cold[c] = shuffledRun(keys, int64(c+1))
				}
				benchShapes(b, cold, keys)
			})
		}
	}
}

// shuffledRun builds a column run of keys under docIDs shuffled against key order,
// so each insert into a resolution lands somewhere new.
func shuffledRun(keys [][]byte, seed int64) *columnRun {
	keyBytes := 0
	for _, k := range keys {
		keyBytes += len(k)
	}
	b := newColumnRunBuilderSized(len(keys), keyBytes, bytesForCount(uint64(len(keys))))
	for i, d := range rand.New(rand.NewSource(seed)).Perm(len(keys)) {
		b.append(keys[i], uint64(d))
	}
	return b.columnRun()
}

// benchScanStrategies times stepping, and galloping at each block limit, over the keys at
// rows, each extended by suffix, scanning several runs in parallel. A limit gallopBlocks
// turns off runs anyway, named below-gate, so the gate itself can be re-decided.
func benchScanStrategies(b *testing.B, name string, runs []*columnRun, keys [][]byte, rows []int, suffix []byte) {
	kb := entinverted.NewVarKeyBuilder(len(rows), 0)
	for _, r := range rows {
		kb.AppendString(string(keys[r]) + string(suffix))
	}
	q, err := kb.Build()
	require.NoError(b, err)
	lo, hi := runs[0].keys.queryWindow(q)
	require.Equal(b, len(rows), hi-lo, "every query key must fall inside the column's window")

	type strategy struct {
		name      string
		gallop    bool
		maxBlocks int
	}
	strategies := []strategy{{name: "step"}}
	if _, ok := runs[0].keys.(*blobKeyColumn); ok {
		strategies = append(strategies, strategy{name: "gallop", gallop: true})
	} else {
		// blocks=0 is the plain gallop, the baseline for the others
		for _, mb := range []int{0, 4, 8, 16, 32} {
			s := strategy{name: fmt.Sprintf("gallop-blocks=%d", mb), gallop: true, maxBlocks: mb}
			if mb > 0 && gallopBlocks(hi-lo, len(keys), mb) == 0 {
				s.name += "-below-gate"
			}
			strategies = append(strategies, s)
		}
	}
	for _, s := range strategies {
		b.Run(name+"/"+s.name, func(b *testing.B) {
			scan := func(run *columnRun, res *Resolution) {
				run.keys.scan(run, q, lo, hi, res, true, false, s.gallop, s.maxBlocks)
			}
			// insert keeps a doc it already holds, so one resolution serves every iteration
			if len(runs) == 1 {
				res := newResolution(len(rows), uint64(len(keys)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scan(runs[0], res)
				}
				return
			}
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				ress := make([]*Resolution, len(runs))
				for c := range ress {
					ress[c] = newResolution(len(rows), uint64(len(keys)))
				}
				for pb.Next() {
					c := int(next.Add(1)) % len(runs)
					scan(runs[c], ress[c])
				}
			})
		})
	}
}

// sortedUniqueKeys draws keys from gen until n are distinct, sorted.
func sortedUniqueKeys(n int, gen func(rnd *rand.Rand) []byte) [][]byte {
	rnd := rand.New(rand.NewSource(int64(n)))
	seen := make(map[string]struct{}, n)
	keys := make([][]byte, 0, n)
	for len(keys) < n {
		k := gen(rnd)
		if _, ok := seen[string(k)]; !ok {
			seen[string(k)] = struct{}{}
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}
