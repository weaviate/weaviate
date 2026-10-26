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
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedKeysLayouts pins that both layouts, built by different types
// sharing no code, answer identically through every accessor.
func TestSortedKeysLayouts(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		build     func(tb testing.TB, keys []string) SortedKeys
		wantFixed bool
	}{
		{
			name:  "variable width",
			keys:  []string{"a", "bb", "ccc"},
			build: buildVariable,
		},
		{
			// One key is trivially of one width, so the variable builder hands
			// back the layout that carries no offsets.
			name:      "variable width, one key",
			keys:      []string{"only"},
			build:     buildVariable,
			wantFixed: true,
		},
		{
			name:  "variable width, empty key among real ones",
			keys:  []string{"", "b"},
			build: buildVariable,
		},
		{
			name:      "fixed width",
			keys:      []string{"aa", "bb", "cc"},
			build:     fixedBuilder(2),
			wantFixed: true,
		},
		{
			name:      "fixed width, one key",
			keys:      []string{"xyz"},
			build:     fixedBuilder(3),
			wantFixed: true,
		},
		{
			name:      "fixed width of one byte",
			keys:      []string{"a", "b"},
			build:     fixedBuilder(1),
			wantFixed: true,
		},
		{
			name:      "fixed width of sixteen, the widest key there is",
			keys:      []string{"0123456789abcdef", "fedcba9876543210"},
			build:     fixedBuilder(16),
			wantFixed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := tt.build(t, tt.keys)

			require.Equal(t, tt.wantFixed, keys.offs == nil, "layout")
			require.Equal(t, len(tt.keys), keys.Len())

			for i, want := range tt.keys {
				assert.Equalf(t, want, string(keys.At(i)), "At(%d)", i)
				assert.Equalf(t, len(want), cap(keys.At(i)),
					"key %d capacity must stop at its own end", i)
			}

			var iterated []string
			for i, k := range keys.All() {
				require.Equal(t, len(iterated), i, "All must yield positions in order")
				iterated = append(iterated, string(k))
			}
			assert.Equal(t, tt.keys, iterated, "All must agree with At")
		})
	}
}

// TestSortedKeysAllStopsEarly pins that a consumer can abandon the iteration
// by returning false from yield, even though nothing on the query path does
// so today.
func TestSortedKeysAllStopsEarly(t *testing.T) {
	for name, keys := range map[string]SortedKeys{
		"variable width": buildVariable(t, []string{"a", "bb", "ccc", "dddd"}),
		"fixed width":    buildFixed(t, 2)([]string{"aa", "bb", "cc", "dd"}),
	} {
		t.Run(name, func(t *testing.T) {
			var seen int
			for range keys.All() {
				seen++
				if seen == 2 {
					break
				}
			}
			assert.Equal(t, 2, seen, "All must stop where the consumer stopped")
		})
	}
}

// TestSortedKeysEmpty covers the lists that hold no keys: one from each builder,
// and the zero value a leaf carries when it is not a batched Contains.
func TestSortedKeysEmpty(t *testing.T) {
	for name, keys := range map[string]SortedKeys{
		"variable builder, nothing appended": mustBuildVar(t, NewVarKeyBuilder(4, 16)),
		"fixed builder, nothing appended":    mustBuildFixed(t, NewFixedKeyBuilder(4, 8)),
		"zero value":                         {},
		// Not a shape a builder produces; Len must not report -1 for it.
		"offsets array with no terminator": {offs: []uint32{}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Zero(t, keys.Len())
			assert.True(t, keys.isAscending())
			for range keys.All() {
				t.Fatal("an empty list must yield nothing")
			}
			// The empty range of an empty list is legal.
			var sub SortedKeys
			assert.NotPanics(t, func() { sub = keys.Sub(0, 0) })
			assert.Zero(t, sub.Len())
		})
	}
}

// TestSortedKeysDegenerate covers builders reached without a usable width or
// without their constructor — each refuses, rather than quietly returning a
// list that reads as a legitimate one.
func TestSortedKeysDegenerate(t *testing.T) {
	cases := map[string]struct {
		build func()
		// Matched against the panic's text, not just its presence: these inputs
		// panic incidentally anyway (nil slice, zero-width division).
		wantPanic string
	}{
		"fixed builder, zero width": {
			build:     func() { NewFixedKeyBuilder(0, 0) },
			wantPanic: "key width 0 is not in 1..",
		},
		"fixed builder, width given as a batch total": {
			build:     func() { NewFixedKeyBuilder(3, 3*8) },
			wantPanic: "width of one key, not the batch total",
		},
		"variable builder, negative key count": {
			build:     func() { NewVarKeyBuilder(-1, 10) },
			wantPanic: "must not be negative",
		},
		// Without this the encoder dies writing into an empty buffer, several
		// frames from the mistake, and Build's guard is never reached.
		"append to a builder with no width": {
			build:     func() { (&FixedKeyBuilder{}).AppendBuf() },
			wantPanic: "NewFixedKeyBuilder was not used",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			recovered := func() (r any) {
				defer func() { r = recover() }()
				tc.build()
				return nil
			}()
			require.NotNil(t, recovered, "a misused constructor must refuse, not hand back a builder")
			err, ok := recovered.(error)
			require.True(t, ok, "a panic must carry an error so a recovered one can be classified")
			assert.ErrorIs(t, err, ErrInternal)
			assert.Contains(t, err.Error(), tc.wantPanic,
				"the panic must name the mistake, not just fail somewhere downstream")
		})
	}

	t.Run("build refuses a builder that skipped its constructor", func(t *testing.T) {
		for name, build := range map[string]func() (SortedKeys, error){
			"variable, nothing appended": (&VarKeyBuilder{}).Build,
			"variable, filled": func() (SortedKeys, error) {
				b := &VarKeyBuilder{}
				b.AppendString("alpha")
				b.AppendString("be")
				return b.Build()
			},
			"fixed, no width": (&FixedKeyBuilder{}).Build,
		} {
			t.Run(name, func(t *testing.T) {
				got, err := build()
				require.ErrorIs(t, err, ErrInternal,
					"a caller must be able to tell this from a bad filter value")
				assert.Contains(t, err.Error(), "was not used")
				assert.Zero(t, got.Len(), "a refused build must not hand back keys")
			})
		}
	})

	// A zero width reaching the sort directly must not divide by zero.
	assert.NotPanics(t, func() { sortFixedWidth(nil, 0) })
	assert.NotPanics(t, func() { sortFixedWidth([]byte("abc"), 0) })
}

// TestFixedKeyBuilderBuild pins that ordering happens in the slab itself — the
// keys move, and nothing indexes them that would have to move too.
func TestFixedKeyBuilderBuild(t *testing.T) {
	t.Run("orders the keys", func(t *testing.T) {
		keys := buildFixed(t, 2)([]string{"dd", "bb", "cc", "aa"})
		require.True(t, keys.isAscending())
		assert.Equal(t, []string{"aa", "bb", "cc", "dd"}, collect(keys))
	})

	t.Run("a single key needs no ordering", func(t *testing.T) {
		assert.Equal(t, []string{"zz"}, collect(buildFixed(t, 2)([]string{"zz"})))
	})

	t.Run("equal keys collapse", func(t *testing.T) {
		keys := buildFixed(t, 2)([]string{"bb", "aa", "bb"})
		assert.Equal(t, []string{"aa", "bb"}, collect(keys))
	})
}

func TestIsAscending(t *testing.T) {
	cases := []struct {
		name string
		keys []string
		want bool
	}{
		{"ascending", []string{"aa", "bb", "cc"}, true},
		{"descending", []string{"bb", "aa"}, false},
		{"equal keys are ordered", []string{"aa", "aa"}, true},
		{"shorter is not smaller", []string{"b", "aaa"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, inOrderGiven(tc.keys).isAscending())
		})
	}
}

// inOrderGiven assembles a list in the order it is given, bypassing the
// builders — which order what they are handed, so a list out of order cannot be
// produced through them at all.
func inOrderGiven(keys []string) SortedKeys {
	var slab []byte
	offs := []uint32{0}
	for _, k := range keys {
		slab = append(slab, k...)
		offs = append(offs, uint32(len(slab)))
	}
	return SortedKeys{slab: slab, offs: offs}
}

func buildVariable(tb testing.TB, keys []string) SortedKeys {
	tb.Helper()
	total := 0
	for _, k := range keys {
		total += len(k)
	}
	b := NewVarKeyBuilder(len(keys), total)
	for _, k := range keys {
		b.AppendString(k)
	}
	built, err := b.Build()
	require.NoError(tb, err)
	return built
}

// buildFixed appends through the encoder-shaped path — write into the buffer the
// builder hands out — as the fixed-width encoders do.
func buildFixed(tb testing.TB, width int) func(keys []string) SortedKeys {
	return func(keys []string) SortedKeys {
		tb.Helper()
		b := NewFixedKeyBuilder(len(keys), width)
		for _, k := range keys {
			copy(b.AppendBuf(), k)
		}
		built, err := b.Build()
		require.NoError(tb, err)
		return built
	}
}

func collect(keys SortedKeys) []string {
	out := make([]string, 0, keys.Len())
	for _, k := range keys.All() {
		out = append(out, string(k))
	}
	return out
}

// TestSortedKeysRange covers both layouts through every accessor, because a
// subrange is read back the same ways the whole list is and the two layouts
// narrow by different means — the fixed one cuts the slab, the variable one
// keeps it whole and cuts only the offsets.
func TestSortedKeysRange(t *testing.T) {
	// Two fixtures, because Build collapses equal-width keys into the layout
	// carrying no offsets — a same-width list would run the fixed arm twice and
	// leave the offsets arm untested.
	varyingWidths := []string{"a", "bb", "ccc", "dddd", "eeeee"}
	oneWidth := []string{"aa", "bb", "cc", "dd", "ee"}

	layouts := []struct {
		name  string
		all   []string
		build func(keys []string) SortedKeys
		// wantOffsets is which arm the fixture must reach, stated rather than
		// derived from the subtest name
		wantOffsets bool
	}{
		{
			name: "variable width", all: varyingWidths, wantOffsets: true,
			build: func(keys []string) SortedKeys { return buildVariable(t, keys) },
		},
		{name: "fixed width", all: oneWidth, build: buildFixed(t, 2)},
	}

	ranges := []struct {
		name     string
		from, to int
	}{
		{"a middle range", 1, 4},
		{"from the start", 0, 2},
		{"to the end", 3, 5},
		{"the whole list", 0, 5},
		{"one key", 2, 3},
		{"none", 2, 2},
	}

	for _, lay := range layouts {
		t.Run(lay.name, func(t *testing.T) {
			keys := lay.build(lay.all)
			// the fixture has to reach the arm the subtest is named for
			require.Equal(t, lay.wantOffsets, keys.offs != nil,
				"fixture must build the layout under test")

			for _, r := range ranges {
				t.Run(r.name, func(t *testing.T) {
					want := lay.all[r.from:r.to]
					sub := keys.Sub(r.from, r.to)

					require.Equal(t, len(want), sub.Len())
					assert.Equal(t, want, collect(sub), "All must yield the subrange")
					for i, w := range want {
						assert.Equalf(t, w, string(sub.At(i)), "At(%d)", i)
					}
					// a subrange is a list in its own right: a reader handed one
					// walks it from 0, so it has to be ascending on its own terms
					assert.True(t, sub.isAscending())
				})
			}

			t.Run("the pieces of a split cover the whole list", func(t *testing.T) {
				var rejoined []string
				for _, bound := range [][2]int{{0, 2}, {2, 2}, {2, 5}} {
					rejoined = append(rejoined, collect(keys.Sub(bound[0], bound[1]))...)
				}
				assert.Equal(t, lay.all, rejoined)
			})

			t.Run("subranging does not disturb the list it came from", func(t *testing.T) {
				_ = keys.Sub(1, 3)
				assert.Equal(t, lay.all, collect(keys))
			})

			// Both layouts have the parent's remaining keys past a subrange's
			// end, and reaching them must fail rather than answer with a
			// neighbour's key.
			t.Run("reading past a subrange panics", func(t *testing.T) {
				sub := keys.Sub(0, 2)
				assert.Panics(t, func() { _ = sub.At(sub.Len()) },
					"At past the subrange must not reach the next one")
				assert.Panics(t, func() { _ = keys.Sub(0, keys.Len()+1) },
					"a subrange wider than the list must not be built")
			})

			// A subrange is a list in its own right, so it refuses what the list
			// it came from refuses.
			t.Run("a subrange of a subrange cannot reach its parent's keys", func(t *testing.T) {
				sub := keys.Sub(1, 3)
				require.Equal(t, 2, sub.Len())

				assert.Panics(t, func() { _ = sub.Sub(0, sub.Len()+1) },
					"one key past the subrange")
				assert.Panics(t, func() { _ = sub.Sub(0, sub.Len()+3) },
					"several keys past the subrange, all of them the parent's")
				assert.Equal(t, lay.all[1:3], collect(sub.Sub(0, sub.Len())),
					"the whole subrange is still legal")
			})

			t.Run("an inverted range is refused rather than answering empty", func(t *testing.T) {
				// empty by arithmetic, so an unchecked reslice builds it
				assert.Panics(t, func() { _ = keys.Sub(2, 1) })
				assert.Panics(t, func() { _ = keys.Sub(-1, 2) })
			})
		})
	}

	t.Run("the zero value refuses every range, as At does", func(t *testing.T) {
		// w == 0 slices to [0:0:0] for any argument pair
		var zero SortedKeys
		assert.Panics(t, func() { _ = zero.Sub(0, 5) })
		assert.Panics(t, func() { _ = zero.Sub(3, 9) })
		assert.Panics(t, func() { _ = zero.Sub(0, -1) })
		assert.NotPanics(t, func() { _ = zero.Sub(0, 0) },
			"the empty range of an empty list is legal")
	})
}

// TestVarKeyBuilderBuild is the variable-width counterpart to
// [TestFixedKeyBuilderBuild]: ordering happens in Build, and which layout comes
// back is decided by the widths that were appended rather than by the builder.
func TestVarKeyBuilderBuild(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		want      []string
		wantFixed bool
	}{
		{
			name: "orders mixed widths",
			keys: []string{"ccc", "a", "bb"},
			want: []string{"a", "bb", "ccc"},
		},
		{
			name:      "one width drops the offsets",
			keys:      []string{"dd", "bb", "cc", "aa"},
			want:      []string{"aa", "bb", "cc", "dd"},
			wantFixed: true,
		},
		{
			name: "a prefix sorts before its extension",
			keys: []string{"abc", "ab", "abcd", "a"},
			want: []string{"a", "ab", "abc", "abcd"},
		},
		{
			name: "an empty key sorts first",
			keys: []string{"b", "", "a"},
			want: []string{"", "a", "b"},
		},
		{
			name:      "a single key needs no ordering",
			keys:      []string{"zz"},
			want:      []string{"zz"},
			wantFixed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := buildVariable(t, tt.keys)
			assert.Equal(t, tt.want, collect(keys))
			assert.True(t, keys.isAscending())
			assert.Equal(t, tt.wantFixed, keys.offs == nil, "layout")
		})
	}
}

// TestBuildDropsDuplicates covers the dedup pass Build runs after ordering,
// with duplicates seeded at the front, the back, and throughout.
func TestBuildDropsDuplicates(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want []string
	}{
		{"none to drop", []string{"cc", "aa", "bb"}, []string{"aa", "bb", "cc"}},
		{"one adjacent pair", []string{"bb", "aa", "bb"}, []string{"aa", "bb"}},
		{"every key equal", []string{"aa", "aa", "aa", "aa"}, []string{"aa"}},
		{"duplicates at the front", []string{"aa", "aa", "bb", "cc"}, []string{"aa", "bb", "cc"}},
		{"duplicates at the back", []string{"aa", "bb", "cc", "cc"}, []string{"aa", "bb", "cc"}},
		{"runs throughout", []string{"bb", "aa", "cc", "bb", "aa", "cc", "bb"}, []string{"aa", "bb", "cc"}},
		{"two keys, both equal", []string{"zz", "zz"}, []string{"zz"}},
		{"one key", []string{"qq"}, []string{"qq"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("fixed", func(t *testing.T) {
				keys := buildFixed(t, 2)(tt.keys)
				assert.Equal(t, tt.want, collect(keys))
				assert.Equal(t, len(tt.want), keys.Len())
			})
			t.Run("variable", func(t *testing.T) {
				// Widened so the list keeps offsets, exercising that arm.
				keys := append([]string{"aaa"}, tt.keys...)
				want := append([]string{"aaa"}, tt.want...)
				slices.Sort(want)
				got := buildVariable(t, keys)
				assert.Equal(t, want, collect(got))
				assert.NotNil(t, got.offs, "the widened list must keep its offsets")
			})
		})
	}
}

// TestBuildDropsDuplicatesAcrossShapes checks the compaction against
// slices.Compact over alphabets narrow enough to collide constantly, at sizes
// either side of every arm boundary.
func TestBuildDropsDuplicatesAcrossShapes(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for _, alphabet := range []int{1, 2, 3, 8} {
		for _, maxLen := range []int{1, 2, 5, 12} {
			for _, n := range []int{0, 1, 2, 3, 5, 63, 64, 65, 500} {
				keys := make([]string, n)
				fixed := make([]string, n)
				for i := range keys {
					b := make([]byte, 1+rng.Intn(maxLen))
					for j := range b {
						b[j] = byte('a' + rng.Intn(alphabet))
					}
					keys[i] = string(b)
					fixed[i] = fmt.Sprintf("%-*s", maxLen, b)
				}
				for name, got := range map[string][]string{
					"variable": collect(buildVariable(t, keys)),
					"fixed":    collect(buildFixed(t, maxLen)(fixed)),
				} {
					src := keys
					if name == "fixed" {
						src = fixed
					}
					want := slices.Clone(src)
					slices.Sort(want)
					want = slices.Compact(want)
					if len(got) != len(want) {
						t.Fatalf("%s alphabet=%d maxLen=%d n=%d: got %d keys, want %d",
							name, alphabet, maxLen, n, len(got), len(want))
					}
					for i := range want {
						if got[i] != want[i] {
							t.Fatalf("%s alphabet=%d maxLen=%d n=%d: key %d is %q, want %q",
								name, alphabet, maxLen, n, i, got[i], want[i])
						}
					}
				}
			}
		}
	}
}

// TestFirstAtOrAfter pins the gallop against a linear scan, over every
// (from, to, target) the list admits and both layouts, since the two index
// the slab differently. to is swept along with from since it is the caller's
// window, not the list's end, and a stride running past it must not answer
// with a key outside that window.
func TestFirstAtOrAfter(t *testing.T) {
	t.Parallel()

	layouts := []struct {
		name  string
		build func(tb testing.TB, keys []string) SortedKeys
	}{
		{name: "variable width", build: buildVariable},
		{name: "fixed width", build: fixedBuilder(1)},
		// Wider than the keys and zero-padded, so the slab is strided rather
		// than packed.
		{name: "fixed width, padded", build: fixedBuilder(8)},
	}
	// Every other printable byte, so a target can fall on a key or in the gap
	// between two, with enough gaps that the gallop's stride doubles a few times.
	var letters []string
	for c := byte('!'); c <= '~'; c += 2 {
		letters = append(letters, string(c))
	}

	for _, layout := range layouts {
		t.Run(layout.name, func(t *testing.T) {
			t.Parallel()

			keys := layout.build(t, letters)
			n := keys.Len()

			// First key, its gap, middle, end, and one before/after all of them.
			last := len(letters) - 1
			targets := []string{
				"", letters[0], next(letters[0]),
				letters[last/2], next(letters[last/2]),
				letters[last], next(letters[last]),
			}
			for _, target := range targets {
				for from := 0; from <= n; from++ {
					for _, to := range []int{from, (from + n) / 2, n} {
						want := to
						for i := from; i < to; i++ {
							if string(keys.At(i)) >= target {
								want = i
								break
							}
						}
						assert.Equalf(t, want, keys.FirstAtOrAfter(from, to, []byte(target)),
							"FirstAtOrAfter(from=%d, to=%d, target=%q)", from, to, target)
					}
				}
			}
		})
	}
}

// TestFirstAtOrAfterOutsideItsRange pins the two caller errors the godoc
// warns about but nothing checks, so the documented behaviour is the tested
// one rather than whatever the arithmetic happens to produce.
func TestFirstAtOrAfterOutsideItsRange(t *testing.T) {
	t.Parallel()

	letters := []string{"a", "c", "e", "g", "i"}

	t.Run("an inverted range answers from, which is past to", func(t *testing.T) {
		t.Parallel()

		keys := buildVariable(t, letters)
		for _, target := range []string{"", "a", "e", "z"} {
			assert.Equalf(t, 4, keys.FirstAtOrAfter(4, 2, []byte(target)),
				"target %q", target)
		}
	})

	t.Run("a range past the end reads past the keys", func(t *testing.T) {
		t.Parallel()

		keys := buildVariable(t, letters)
		require.Panics(t, func() {
			keys.FirstAtOrAfter(0, keys.Len()+8, []byte("z"))
		})
	})
}

// fixedBuilder adapts buildFixed to the table's build signature, which takes the
// TB so a builder error fails the case rather than being dropped.
func fixedBuilder(width int) func(tb testing.TB, keys []string) SortedKeys {
	return func(tb testing.TB, keys []string) SortedKeys {
		return buildFixed(tb, width)(keys)
	}
}

func mustBuildVar(tb testing.TB, b *VarKeyBuilder) SortedKeys {
	tb.Helper()
	built, err := b.Build()
	require.NoError(tb, err)
	return built
}

func mustBuildFixed(tb testing.TB, b *FixedKeyBuilder) SortedKeys {
	tb.Helper()
	built, err := b.Build()
	require.NoError(tb, err)
	return built
}

// TestSortedKeysAllDoesNotAllocate pins the escape-analysis claim
// [SortedKeys.All]'s godoc rests on.
func TestSortedKeysAllDoesNotAllocate(t *testing.T) {
	for name, keys := range map[string]SortedKeys{
		"offsets layout": buildVariable(t, []string{"a", "bb", "ccc"}),
		"width layout":   buildFixed(t, 2)([]string{"aa", "bb", "cc"}),
	} {
		t.Run(name, func(t *testing.T) {
			var sink int
			allocs := testing.AllocsPerRun(100, func() {
				for _, k := range keys.All() {
					sink += len(k)
				}
			})
			assert.Zero(t, allocs, "iterating the keys must not allocate")
			require.NotZero(t, sink, "the loop body must have run")
		})
	}
}

// TestSortedKeysAtRefusesOutOfRange pins that every layout panics on an
// out-of-range index, asserted by occurrence rather than message (see
// [SortedKeys.At] for why) — except the zero value, whose message this
// package owns and does pin.
func TestSortedKeysAtRefusesOutOfRange(t *testing.T) {
	// Enough duplicates to force Build's copy path: append rounds the
	// allocation up, and an uncapped copy would answer past-the-end indices
	// with a zero key instead of panicking.
	dupes := make([]string, 40_000)
	for i := range dupes {
		dupes[i] = fmt.Sprintf("%08d", i%251)
	}

	for name, keys := range map[string]SortedKeys{
		"offsets layout":       buildVariable(t, []string{"a", "bb"}),
		"width layout":         buildFixed(t, 2)([]string{"aa", "bb"}),
		"width layout, copied": buildFixed(t, 8)(dupes),
		"empty, width":         mustBuildFixed(t, NewFixedKeyBuilder(4, 8)),
		"empty, offsets":       mustBuildVar(t, NewVarKeyBuilder(4, 16)),
	} {
		t.Run(name, func(t *testing.T) {
			n := keys.Len()
			assert.Panics(t, func() { keys.At(n) }, "one past the last key")
			assert.Panics(t, func() { keys.At(n + 1) }, "well past the last key")
			assert.Panics(t, func() { keys.At(-1) }, "before the first key")
			if n > 0 {
				assert.NotPanics(t, func() { keys.At(n - 1) }, "the last key must be readable")
			}
		})
	}

	t.Run("zero value", func(t *testing.T) {
		var keys SortedKeys
		for _, i := range []int{0, 1, -1} {
			assert.PanicsWithError(t,
				"inverted: internal fault: keys were not made by a builder",
				func() { keys.At(i) },
				"index %d into a list that was never built", i)
		}
	})
}

// TestShrinkKeysReleasesTheDedupedTail asserts on array identity, not cap():
// cap() reads the same whether the dead tail was released or merely hidden
// behind it.
func TestShrinkKeysReleasesTheDedupedTail(t *testing.T) {
	sameArray := func(a, b []byte) bool { return &a[:1][0] == &b[:1][0] }

	tests := []struct {
		name       string
		cap, end   int
		wantCopied bool
	}{
		{"most of a large array is dead", 100_000, 8, true},
		{"exactly at the ratio", 4096, 1024, false},
		{"just inside the ratio", 4096, 1025, false},
		{"below the floor, however dead", 4095, 1, false},
		{"nothing was dropped", 4096, 4096, false},
		// Either side of 4*end == cap on a large array, so loosening the ratio
		// as well as tightening it changes an answer.
		{"exactly at the ratio, large array", 100_000, 25_000, false},
		{"one key past the ratio", 100_000, 24_999, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := make([]byte, tt.cap)
			for i := range src {
				src[i] = byte(i)
			}
			got := shrinkKeys(src, tt.end)

			require.Len(t, got, tt.end)
			assert.Equal(t, src[:tt.end], got, "shrinking must not alter a key")
			assert.Equal(t, tt.end, cap(got), "a result must stop at the last key")
			assert.Equal(t, tt.wantCopied, !sameArray(src, got),
				"copied-vs-aliased decides whether the array is released")
		})
	}

	// Compared as a bool, not with NotEqual on the two pointers: testify would
	// dereference them and find both elements zero.
	t.Run("offsets shrink on the same rule", func(t *testing.T) {
		aliased := func(a, b []uint32) bool { return &a[:1][0] == &b[:1][0] }

		src := make([]uint32, 25_000)
		assert.False(t, aliased(src, shrinkOffs(src, 3)), "a dead offset array must be released")
		assert.True(t, aliased(src, shrinkOffs(src, 6_250)), "at the ratio, nothing is reclaimed")
		assert.False(t, aliased(src, shrinkOffs(src, 6_249)), "one entry past it, the copy is worth it")

		// The floor is counted in bytes, not entries, so these two straddle it
		// at a quarter of the element count shrinkKeys would need.
		atFloor := make([]uint32, 1024)
		assert.False(t, aliased(atFloor, shrinkOffs(atFloor, 1)), "1024 offsets is 4096 bytes")
		belowFloor := make([]uint32, 1023)
		assert.True(t, aliased(belowFloor, shrinkOffs(belowFloor, 1)), "1023 offsets is below it")
	})

	// Pins that Build still calls shrinkKeys/shrinkOffs, since nothing else
	// would notice a regression to a plain three-index slice.
	t.Run("Build hands the batch array back", func(t *testing.T) {
		const n = 100_000

		t.Run("fixed layout", func(t *testing.T) {
			b := NewFixedKeyBuilder(n, 8)
			for i := 0; i < n; i++ {
				binary.BigEndian.PutUint64(b.AppendBuf(), uint64(i%2))
			}
			raw := b.slab
			built, err := b.Build()
			require.NoError(t, err)
			require.Equal(t, 2, built.Len())
			assert.False(t, &raw[:1][0] == &built.slab[:1][0],
				"a batch that dedups to two keys must not keep the array it filled")
		})

		t.Run("offsets layout", func(t *testing.T) {
			b := NewVarKeyBuilder(n, 3*n)
			for i := 0; i < n; i++ {
				if i%2 == 0 {
					b.AppendString("aa")
				} else {
					b.AppendString("bbb")
				}
			}
			rawSlab, rawOffs := b.slab, b.offs
			built, err := b.Build()
			require.NoError(t, err)
			require.Equal(t, 2, built.Len())
			require.NotNil(t, built.offs, "mixed widths must keep the offsets layout")
			assert.False(t, &rawSlab[:1][0] == &built.slab[:1][0], "the slab must be released")
			assert.False(t, &rawOffs[:1][0] == &built.offs[:1][0], "the offsets must be released")
		})
	})
}

// next is the single-byte key after k, which for a list of every other byte is a
// target sitting in the gap that follows it.
func next(k string) string { return string(k[0] + 1) }
