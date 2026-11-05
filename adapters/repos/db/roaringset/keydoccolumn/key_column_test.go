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
	const corpusSize = 64 // stepping takes over at 5+ query keys
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
			require.Equal(t, tt.step, !shouldGallop(tt.keys.Len(), corpusSize),
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
		require.Equal(t, len(picks) <= 2, shouldGallop(q.Len(), len(keys)),
			"each query must exercise the branch it claims")
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
				"step":   func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, false) },
				"gallop": func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, true) },
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
					run.keys.scan(run, q, lo, hi, res, true, false, gallop)
					require.ElementsMatch(t, want, heldDocs(res), "stride %d, gallop %v", stride, gallop)
				}
			}
		})
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
			require.True(t, shouldGallop(keys.Len(), corpusSize), "test case must gallop")

			res := newResolution(keys.Len(), 1_000)
			run.scanInto(keys, res, true)
			require.Equal(t, tt.want, heldDocs(res))
		})
	}
}

// TestShouldGallop pins the density at which scanInto switches to galloping.
func TestShouldGallop(t *testing.T) {
	tests := []struct {
		name            string
		numKeys, corpus int
		want            bool
	}{
		{name: "empty corpus", numKeys: 5, corpus: 0, want: false},
		{name: "just below one key in fifteen", numKeys: 99, corpus: 1_500, want: true},
		{name: "one key in fifteen", numKeys: 100, corpus: 1_500, want: false},
		{name: "every key", numKeys: 1_000, corpus: 1_000, want: false},
		{name: "one key, small corpus", numKeys: 1, corpus: 15, want: false},
		{name: "one key, corpus past fifteen", numKeys: 1, corpus: 16, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldGallop(tt.numKeys, tt.corpus))
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

// BenchmarkScanStrategies times stepping and galloping scans, each
// forced, across query density, corpus size and key layout; it sweeps
// maxKeySpacingForStepping.
//
// Run:
//
//	go test -run '^$' -bench 'ScanStrategies' -benchtime 0.3s -count 3 \
//	    ./adapters/repos/db/roaringset/keydoccolumn/
func BenchmarkScanStrategies(b *testing.B) {
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
	densities := []int{1, 2, 5, 7, 10, 15, 20, 33, 50, 100}

	for _, l := range layouts {
		for _, corpus := range []int{30_000, 300_000, 3_000_000} {
			keys := l.keys(corpus)
			docs := make([]uint64, corpus)
			for i := range docs {
				docs[i] = uint64(i)
			}
			run := runFromPairs(keys, docs)
			require.IsType(b, l.backing, run.keys, "corpus must select the backing this layout names")

			for _, d := range densities {
				benchScanStrategies(b, fmt.Sprintf("%s/keys=%d/density=%d%%", l.name, corpus, d), run, keys, d, nil)
				if corpus == 300_000 {
					// a key one zero byte longer sorts just after its stored key, so it is absent
					benchScanStrategies(b, fmt.Sprintf("%s/keys=%d/density=%d%%/absent", l.name, corpus, d), run, keys, d, []byte{0})
				}
			}
		}
	}
}

// benchScanStrategies times both strategies over density percent of keys, evenly
// spread, each extended by suffix.
func benchScanStrategies(b *testing.B, name string, run *columnRun, keys [][]byte, density int, suffix []byte) {
	numKeys := max(1, len(keys)*density/100)
	kb := entinverted.NewVarKeyBuilder(numKeys, 0)
	for j := 0; j < numKeys; j++ {
		kb.AppendString(string(keys[j*len(keys)/numKeys]) + string(suffix))
	}
	q, err := kb.Build()
	require.NoError(b, err)
	lo, hi := run.keys.queryWindow(q)
	require.Equal(b, numKeys, hi-lo, "every query key must fall inside the column's window")

	strategies := []struct {
		name string
		scan func(res *Resolution)
	}{
		{"step", func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, false) }},
		{"gallop", func(res *Resolution) { run.keys.scan(run, q, lo, hi, res, true, false, true) }},
	}
	for _, s := range strategies {
		b.Run(name+"/"+s.name, func(b *testing.B) {
			// insert keeps a doc it already holds, so one resolution serves every iteration
			res := newResolution(numKeys, uint64(len(keys)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.scan(res)
			}
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
