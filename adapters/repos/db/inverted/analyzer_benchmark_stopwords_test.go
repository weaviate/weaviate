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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// BenchmarkAnalyzeWithStopwords compares the query tokenization path
// (tokenizer.Analyze) with no stopwords vs built-in preset vs user-defined
// preset. This is the hot path during BM25 and filter queries.
func BenchmarkAnalyzeWithStopwords(b *testing.B) {
	// A realistic query containing a mix of stopwords and content words.
	shortQuery := "the quick brown fox jumps over a lazy dog"
	longQuery := "the quick brown fox jumps over a lazy dog and the cat is not in this text but it was there before with all the other animals that were running"

	b.Run("short_query/no_stopwords", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(shortQuery, models.PropertyTokenizationWord, "BenchClass", nil, nil)
		}
	})

	b.Run("short_query/builtin_en", func(b *testing.B) {
		d, _ := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(shortQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("short_query/user_defined_10_words", func(b *testing.B) {
		d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
		d.SetAdditions([]string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(shortQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("short_query/user_defined_100_words", func(b *testing.B) {
		words := make([]string, 100)
		for j := range words {
			words[j] = string(rune('a'+j/26)) + string(rune('a'+j%26))
		}
		d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
		d.SetAdditions(words)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(shortQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("long_query/no_stopwords", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(longQuery, models.PropertyTokenizationWord, "BenchClass", nil, nil)
		}
	})

	b.Run("long_query/builtin_en", func(b *testing.B) {
		d, _ := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(longQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("long_query/user_defined_10_words", func(b *testing.B) {
		d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
		d.SetAdditions([]string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(longQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("long_query/user_defined_100_words", func(b *testing.B) {
		words := make([]string, 100)
		for j := range words {
			words[j] = string(rune('a'+j/26)) + string(rune('a'+j%26))
		}
		d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
		d.SetAdditions(words)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(longQuery, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})
}

// BenchmarkDetectorCreationAndAnalyze measures the combined cost of creating
// a per-property stopword detector and running Analyze — this is what happens
// on each BM25 query when a property has a custom stopwordPreset.
func BenchmarkDetectorCreationAndAnalyze(b *testing.B) {
	query := "the quick brown fox jumps over a lazy dog"

	b.Run("collection_level_reuse", func(b *testing.B) {
		// Simulates the common case: collection-level detector created once, reused
		d, _ := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(query, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("cached_user_defined_preset", func(b *testing.B) {
		// Simulates a user-defined preset cached at index level (current implementation)
		d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
		d.SetAdditions([]string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tokenizer.Analyze(query, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("uncached_per_query_builtin_preset", func(b *testing.B) {
		// What it would cost if we created the detector on each query (old approach)
		for i := 0; i < b.N; i++ {
			d, _ := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
			tokenizer.Analyze(query, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})

	b.Run("uncached_per_query_user_defined_preset", func(b *testing.B) {
		// What it would cost if we created + populated on each query (old approach)
		words := []string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"}
		for i := 0; i < b.N; i++ {
			d, _ := stopwords.NewDetectorFromPreset(stopwords.NoPreset)
			d.SetAdditions(words)
			tokenizer.Analyze(query, models.PropertyTokenizationWord, "BenchClass", nil, d)
		}
	})
}
