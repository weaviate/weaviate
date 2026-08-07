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
		build     func(keys []string) SortedKeys
		wantFixed bool
	}{
		{
			name:  "variable width",
			keys:  []string{"a", "bb", "ccc"},
			build: buildVariable,
		},
		{
			name:  "variable width, one key",
			keys:  []string{"only"},
			build: buildVariable,
		},
		{
			name:  "variable width, empty key among real ones",
			keys:  []string{"", "b"},
			build: buildVariable,
		},
		{
			name:      "fixed width",
			keys:      []string{"aa", "bb", "cc"},
			build:     buildFixed(2),
			wantFixed: true,
		},
		{
			name:      "fixed width, one key",
			keys:      []string{"xyz"},
			build:     buildFixed(3),
			wantFixed: true,
		},
		{
			name:      "fixed width of one byte",
			keys:      []string{"a", "b"},
			build:     buildFixed(1),
			wantFixed: true,
		},
		{
			name:      "fixed width of sixteen, the widest key there is",
			keys:      []string{"0123456789abcdef", "fedcba9876543210"},
			build:     buildFixed(16),
			wantFixed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := tt.build(tt.keys)

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

// TestSortedKeysEmpty covers the lists that hold no keys: one from each builder,
// and the zero value a leaf carries when it is not a batched Contains.
func TestSortedKeysEmpty(t *testing.T) {
	for name, keys := range map[string]SortedKeys{
		"variable builder, nothing appended": NewKeyBuilder(4, 16).Build(),
		"fixed builder, nothing appended":    NewFixedKeyBuilder(4, 8).Build(),
		"zero value":                         {},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Zero(t, keys.Len())
			assert.True(t, keys.IsAscending())
			for range keys.All() {
				t.Fatal("an empty list must yield nothing")
			}
		})
	}
}

// TestFixedKeyBuilderSort pins that ordering happens in the slab itself — the
// keys move, and nothing indexes them that would have to move too.
func TestFixedKeyBuilderSort(t *testing.T) {
	t.Run("orders the keys", func(t *testing.T) {
		keys := buildFixed(2)([]string{"dd", "bb", "cc", "aa"})
		require.True(t, keys.IsAscending())
		assert.Equal(t, []string{"aa", "bb", "cc", "dd"}, collect(keys))
	})

	t.Run("a single key needs no ordering", func(t *testing.T) {
		assert.Equal(t, []string{"zz"}, collect(buildFixed(2)([]string{"zz"})))
	})

	t.Run("equal keys survive", func(t *testing.T) {
		keys := buildFixed(2)([]string{"bb", "aa", "bb"})
		assert.Equal(t, []string{"aa", "bb", "bb"}, collect(keys))
	})
}

func TestIsAscending(t *testing.T) {
	assert.True(t, buildVariable([]string{"aa", "bb", "cc"}).IsAscending())
	assert.False(t, buildVariable([]string{"bb", "aa"}).IsAscending())
	assert.True(t, buildVariable([]string{"aa", "aa"}).IsAscending(), "equal keys are ordered")
	assert.False(t, buildVariable([]string{"b", "aaa"}).IsAscending(), "shorter is not smaller")
}

func buildVariable(keys []string) SortedKeys {
	total := 0
	for _, k := range keys {
		total += len(k)
	}
	b := NewKeyBuilder(len(keys), total)
	for _, k := range keys {
		b.AppendString(k)
	}
	return b.Build()
}

// buildFixed appends through the encoder-shaped path — write into the buffer the
// builder hands out — and orders the result, as the fixed-width encoders do.
func buildFixed(width int) func(keys []string) SortedKeys {
	return func(keys []string) SortedKeys {
		b := NewFixedKeyBuilder(len(keys), width)
		for _, k := range keys {
			copy(b.AppendBuf(), k)
		}
		b.Sort()
		return b.Build()
	}
}

func collect(keys SortedKeys) []string {
	out := make([]string, 0, keys.Len())
	for _, k := range keys.All() {
		out = append(out, string(k))
	}
	return out
}
