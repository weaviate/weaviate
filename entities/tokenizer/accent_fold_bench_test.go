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

package tokenizer

import (
	"strings"
	"testing"
)

// Short inputs representative of typical property values
var benchInputs = []struct {
	name  string
	input string
}{
	{"french", "L'école est fermée pour les vacances d'été"},
	{"german", "Ärger über Öl und Straße in München"},
	{"mixed", "café résumé naïve São Paulo Łódź"},
	{"ligatures", "Æneas und œuvre mit ß und þ"},
	{"vietnamese", "Hà Nội là thủ đô của Việt Nam"},
	{"ascii_only", "The quick brown fox jumps over the lazy dog"},
}

// Longer input simulating a paragraph-sized property
var benchParagraph = strings.Repeat("L'école est fermée. Le café résumé naïve São Paulo. ", 50)

// Pre-built ignore sets
var (
	ignoreNil   map[rune]struct{}
	ignoreSmall = BuildIgnoreSet([]string{"é"})
	ignoreLarge = BuildIgnoreSet([]string{"é", "è", "ê", "ë", "ñ", "ü", "ö", "ä", "ß", "ø", "Ø", "æ", "Æ", "þ", "Þ", "å", "Å"})
)

// --- FoldAccents benchmarks ---

func BenchmarkFoldAccents(b *testing.B) {
	for _, tc := range benchInputs {
		b.Run(tc.name+"/no_fold", func(b *testing.B) {
			// Baseline: no folding at all (just return input)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = tc.input
			}
		})

		b.Run(tc.name+"/fold_no_ignore", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FoldAccents(tc.input, ignoreNil)
			}
		})

		b.Run(tc.name+"/fold_ignore_small", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FoldAccents(tc.input, ignoreSmall)
			}
		})

		b.Run(tc.name+"/fold_ignore_large", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FoldAccents(tc.input, ignoreLarge)
			}
		})
	}
}

// --- FoldAccentsSlice benchmarks (simulates analyzer path) ---

func BenchmarkFoldAccentsSlice(b *testing.B) {
	// Simulate tokenized input (word tokenization of the paragraph)
	tokens := Tokenize("word", benchParagraph)

	b.Run("no_fold", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tokens
		}
	})

	b.Run("fold_no_ignore", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := make([]string, len(tokens))
			copy(t, tokens)
			FoldAccentsSlice(t, ignoreNil)
		}
	})

	b.Run("fold_ignore_small", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := make([]string, len(tokens))
			copy(t, tokens)
			FoldAccentsSlice(t, ignoreSmall)
		}
	})

	b.Run("fold_ignore_large", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := make([]string, len(tokens))
			copy(t, tokens)
			FoldAccentsSlice(t, ignoreLarge)
		}
	})
}

// --- BuildIgnoreSet benchmark ---

func BenchmarkBuildIgnoreSet(b *testing.B) {
	b.Run("empty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			BuildIgnoreSet([]string{})
		}
	})

	b.Run("single_char", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			BuildIgnoreSet([]string{"é"})
		}
	})

	b.Run("large_set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			BuildIgnoreSet([]string{"é", "è", "ê", "ë", "ñ", "ü", "ö", "ä", "ß", "ø", "Ø", "æ", "Æ", "þ", "Þ", "å", "Å"})
		}
	})
}

// --- End-to-end analyzer path benchmark ---

func BenchmarkAnalyzerPath(b *testing.B) {
	input := benchParagraph

	b.Run("tokenize_only", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Tokenize("word", input)
		}
	})

	b.Run("fold_then_tokenize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			folded := FoldAccents(input, ignoreNil)
			Tokenize("word", folded)
		}
	})

	b.Run("fold_then_tokenize_ignore_small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			folded := FoldAccents(input, ignoreSmall)
			Tokenize("word", folded)
		}
	})

	b.Run("fold_then_tokenize_ignore_large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			folded := FoldAccents(input, ignoreLarge)
			Tokenize("word", folded)
		}
	})
}
