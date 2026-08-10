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

// TestSortedKeysLayouts reads both layouts back through every accessor. They
// are built by different types and share no code, so what a key reads back as
// must not depend on which one produced it.
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

// TestSortedKeysAllStopsEarly pins that a consumer can abandon the iteration.
//
// All hands out one func literal for both layouts so the compiler can keep the
// caller's loop body on the stack, and the literal has to honour a false from
// yield. Nothing on the query path breaks out today — the fold visits every key
// — so without this the early return is only reachable through a caller that
// does not exist yet.
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
		// No builder produces this — offsets always carry their leading zero —
		// but deriving the count from len(offs)-1 would report -1 for it, which
		// passes every guard that tests for zero.
		"offsets array with no terminator": {offs: []uint32{}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Zero(t, keys.Len())
			assert.True(t, keys.isAscending())
			for range keys.All() {
				t.Fatal("an empty list must yield nothing")
			}
		})
	}
}

// TestSortedKeysDegenerate covers the builders reached without a usable width or
// without their constructor. Each refuses, because the alternative is a list
// that reads as a legitimate one: a fixed builder with no width would report no
// keys, and a variable builder with no leading offset reads every key one
// position over and loses the last — a narrower filter result that nothing
// reports.
//
// The constructors panic and Build returns: a constructor can see the mistake
// in the arguments it was handed, where Build only sees it in state that has
// already accumulated, on a path where a panic would take the node down.
func TestSortedKeysDegenerate(t *testing.T) {
	cases := map[string]struct {
		build func()
		// wantPanic is matched against the recovered value. Asserting only that
		// something panicked is satisfied by the incidental panics these inputs
		// cause anyway — a nil offsets slice, a division by a zero width — so
		// the guard could be deleted with the test still green.
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

	// Build is handed accumulated state rather than a constant, and sits where a
	// panic would end the process, so it reports the same class of mistake as an
	// error a caller can distinguish from a bad filter value.
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

// TestBuildDropsDuplicates covers the pass Build runs after ordering.
//
// Both layouts compact in place, and the variable one rewrites offsets as it
// goes, so a survivor that moves must take its own offsets with it. Duplicates
// are seeded at the front, the back and throughout because the compaction reads
// ahead of the cursor it writes to.
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
				// Widen one key per case so the list cannot take the
				// fixed-width layout and the offset-rewriting arm is the one
				// under test.
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

// TestSortedKeysAllDoesNotAllocate pins the claim All's godoc rests on: one func
// literal covers both layouts, so the compiler can tell which iterator a caller
// received, devirtualize the yield, and keep the caller's loop body on the
// stack. Splitting All per layout would put an allocation in every fold with
// nothing else to notice.
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

// TestSortedKeysAtRefusesOutOfRange covers the range check At makes.
//
// The message is asserted, not merely the panic: an out-of-range index panics
// somewhere in every layout — on the offsets index, or on the slice bounds — so
// asserting only that something blew up would be satisfied with the check
// deleted. What the check buys is that all three layouts refuse the SAME way.
//
// One case spells the message out rather than building it from outOfRange,
// which would mutate both sides together and pin nothing about the wording.
func TestSortedKeysAtRefusesOutOfRange(t *testing.T) {
	t.Run("the message names the index and the count", func(t *testing.T) {
		keys := buildFixed(t, 2)([]string{"aa", "bb"})
		assert.PanicsWithError(t,
			"inverted: internal fault: key 2 requested from a list of 2",
			func() { keys.At(2) })
	})

	for name, keys := range map[string]SortedKeys{
		"offsets layout": buildVariable(t, []string{"a", "bb"}),
		"width layout":   buildFixed(t, 2)([]string{"aa", "bb"}),
		"zero value":     {},
		"empty, width":   mustBuildFixed(t, NewFixedKeyBuilder(4, 8)),
		"empty, offsets": mustBuildVar(t, NewVarKeyBuilder(4, 16)),
	} {
		t.Run(name, func(t *testing.T) {
			n := keys.Len()
			assert.PanicsWithError(t, outOfRange(n, n).Error(), func() { keys.At(n) })
			assert.PanicsWithError(t, outOfRange(n+1, n).Error(), func() { keys.At(n + 1) })
			assert.PanicsWithError(t, outOfRange(-1, n).Error(), func() { keys.At(-1) })
			if n > 0 {
				assert.NotPanics(t, func() { keys.At(n - 1) }, "the last key must be readable")
			}
		})
	}
}

// TestShrinkKeysReleasesTheDedupedTail covers the copy that returns a deduped
// batch's array to the collector.
//
// Asserted on the array identity, not on cap(): the returned slice is capped at
// the last surviving key either way, so cap() reads the same whether the tail
// was released or is merely hidden behind it. That is the trap the shrink code
// itself first fell into, and a test reading cap() falls into it too.
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
			if tt.wantCopied {
				// append rounds to a size class, so the spare bytes are fresh
				// zeros rather than the tail that was dropped.
				assert.GreaterOrEqual(t, cap(got), tt.end)
			} else {
				assert.Equal(t, tt.end, cap(got), "an aliased result must stop at the last key")
			}
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

	// The helpers above are only reached through Build, and nothing else pins
	// that Build still calls them: every call site could go back to a plain
	// three-index slice with the rest of the suite green.
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
