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

package stopwords

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
)

// BenchmarkDetectorCreation measures the overhead of creating a stopword
// detector from different sources: built-in preset, full config, and
// user-defined preset (simulated via NoPreset + SetAdditions).
func BenchmarkDetectorCreation(b *testing.B) {
	b.Run("builtin_en", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = NewDetectorFromPreset(EnglishPreset)
		}
	})

	b.Run("builtin_none", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = NewDetectorFromPreset(NoPreset)
		}
	})

	b.Run("config_en_with_additions", func(b *testing.B) {
		cfg := models.StopwordConfig{
			Preset:    EnglishPreset,
			Additions: []string{"dog", "cat", "hello", "world", "test"},
		}
		for i := 0; i < b.N; i++ {
			_, _ = NewDetectorFromConfig(cfg)
		}
	})

	b.Run("user_defined_small_10_words", func(b *testing.B) {
		words := []string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"}
		for i := 0; i < b.N; i++ {
			d, _ := NewDetectorFromPreset(NoPreset)
			d.SetAdditions(words)
		}
	})

	b.Run("user_defined_large_100_words", func(b *testing.B) {
		words := make([]string, 100)
		for j := range words {
			words[j] = string(rune('a'+j/26)) + string(rune('a'+j%26))
		}
		for i := 0; i < b.N; i++ {
			d, _ := NewDetectorFromPreset(NoPreset)
			d.SetAdditions(words)
		}
	})
}

// BenchmarkIsStopword measures lookup performance on detectors created from
// different sources.
func BenchmarkIsStopword(b *testing.B) {
	tokens := []string{
		"the", "quick", "brown", "fox", "jumps", "over", "a", "lazy", "dog",
		"is", "not", "in", "this", "text", "but", "it", "was", "there",
	}

	b.Run("no_detector", func(b *testing.B) {
		// Baseline: no stopword check at all
		for i := 0; i < b.N; i++ {
			for range tokens {
				// no-op
			}
		}
	})

	b.Run("builtin_en", func(b *testing.B) {
		d, _ := NewDetectorFromPreset(EnglishPreset)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, t := range tokens {
				d.IsStopword(t)
			}
		}
	})

	b.Run("user_defined_10_words", func(b *testing.B) {
		d, _ := NewDetectorFromPreset(NoPreset)
		d.SetAdditions([]string{"le", "la", "les", "un", "une", "des", "du", "de", "au", "aux"})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, t := range tokens {
				d.IsStopword(t)
			}
		}
	})

	b.Run("user_defined_100_words", func(b *testing.B) {
		words := make([]string, 100)
		for j := range words {
			words[j] = string(rune('a'+j/26)) + string(rune('a'+j%26))
		}
		d, _ := NewDetectorFromPreset(NoPreset)
		d.SetAdditions(words)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, t := range tokens {
				d.IsStopword(t)
			}
		}
	})
}
