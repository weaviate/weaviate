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

// TestKeysLayouts reads both layouts back through every accessor. They
// are built by different types and share no code, so what a key reads back as
// must not depend on which one produced it.
func TestKeysLayouts(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		build     func(keys []string) Keys
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

// TestKeysEmpty covers the lists that hold no keys: one from each builder,
// and the zero value a leaf carries when it is not a batched Contains.
func TestKeysEmpty(t *testing.T) {
	for name, keys := range map[string]Keys{
		"variable builder, nothing appended": NewKeyBuilder(4, 16).Build(),
		"fixed builder, nothing appended":    NewFixedKeyBuilder(4, 8).Build(),
		"zero value":                         {},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Zero(t, keys.Len())
			for range keys.All() {
				t.Fatal("an empty list must yield nothing")
			}
		})
	}
}

func buildVariable(keys []string) Keys {
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

// buildFixed appends through the encoder-shaped path: write into the buffer the
// builder hands out, rather than copying in a key built elsewhere.
func buildFixed(width int) func(keys []string) Keys {
	return func(keys []string) Keys {
		b := NewFixedKeyBuilder(len(keys), width)
		for _, k := range keys {
			copy(b.AppendBuf(), k)
		}
		return b.Build()
	}
}
