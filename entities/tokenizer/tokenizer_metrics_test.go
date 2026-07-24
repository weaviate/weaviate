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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TestTokenizeMetricsRecordedAtDispatch pins that tokenization metrics are
// recorded once, under the dispatched tokenization's own label. Regression
// coverage for the historical double-count: tokenizeLowercase delegated to
// tokenizeWhitespace, which ALSO recorded its own metrics, so one lowercase
// call inflated the "whitespace" token count (and "lowercase" itself never
// recorded any token count at all, only a duration).
func TestTokenizeMetricsRecordedAtDispatch(t *testing.T) {
	countFor := func(label string) float64 {
		return testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues(label))
	}

	lowercaseBefore := countFor(models.PropertyTokenizationLowercase)
	whitespaceBefore := countFor(models.PropertyTokenizationWhitespace)

	tokens := Tokenize(models.PropertyTokenizationLowercase, "Hello World")
	require.Equal(t, []string{"hello", "world"}, tokens)

	require.Equal(t, lowercaseBefore+2, countFor(models.PropertyTokenizationLowercase),
		"lowercase must record its own token count")
	require.Equal(t, whitespaceBefore, countFor(models.PropertyTokenizationWhitespace),
		"lowercase must not count its internal whitespace delegation under the whitespace label")

	whitespaceBefore = countFor(models.PropertyTokenizationWhitespace)
	tokens = Tokenize(models.PropertyTokenizationWhitespace, "Hello World")
	require.Equal(t, []string{"Hello", "World"}, tokens)
	require.Equal(t, whitespaceBefore+2, countFor(models.PropertyTokenizationWhitespace),
		"direct whitespace tokenization still records under whitespace")

	// The lowercase() helper is shared by the word (and wildcard) tokenizers;
	// it must not record anything itself, or word tokenization pollutes the
	// "lowercase" label (the historical behavior).
	lowercaseBefore = countFor(models.PropertyTokenizationLowercase)
	wordBefore := countFor(models.PropertyTokenizationWord)
	tokens = Tokenize(models.PropertyTokenizationWord, "Hello World")
	require.Equal(t, []string{"hello", "world"}, tokens)
	require.Equal(t, wordBefore+2, countFor(models.PropertyTokenizationWord),
		"word tokenization records under word")
	require.Equal(t, lowercaseBefore, countFor(models.PropertyTokenizationLowercase),
		"word tokenization must not count its lowercasing under the lowercase label")
}

// TestTrigramWithWildcardsMetricsAndOutput pins that a trigram-with-wildcards
// call records only under trigram_with_wildcards: its internal
// word-with-wildcards split must not count under word_with_wildcards (the
// historical double record). Doubles as the only output assertion for the
// trigram-wildcards path.
func TestTrigramWithWildcardsMetricsAndOutput(t *testing.T) {
	countFor := func(label string) float64 {
		return testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues(label))
	}

	wordBefore := countFor("word_with_wildcards")
	trigramBefore := countFor("trigram_with_wildcards")

	tokens := TokenizeWithWildcardsForClass(models.PropertyTokenizationTrigram, "Hello W?rld*", "")
	require.Equal(t, []string{"hel", "ell", "llo", "low", "ow?", "w?r", "?rl", "rld", "ld*"}, tokens)

	require.Equal(t, trigramBefore+float64(len(tokens)), countFor("trigram_with_wildcards"),
		"trigram wildcards must record its token count under its own label")
	require.Equal(t, wordBefore, countFor("word_with_wildcards"),
		"trigram wildcards must not count its internal word split under word_with_wildcards")
}

// TestGseChRecordsUnderOwnLabel pins the gse_ch relabel: Chinese tokenization
// records under its own "gse_ch" label, not under "gse" (the historical
// copy-paste), so operators can tell the two tokenizers' volume apart.
func TestGseChRecordsUnderOwnLabel(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	InitOptionalTokenizers()

	countFor := func(label string) float64 {
		return testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues(label))
	}

	gseBefore := countFor(models.PropertyTokenizationGse)
	gseChBefore := countFor(models.PropertyTokenizationGseCh)

	tokens := Tokenize(models.PropertyTokenizationGseCh, "你好世界")
	require.NotEmpty(t, tokens)

	require.Equal(t, gseChBefore+float64(len(tokens)), countFor(models.PropertyTokenizationGseCh),
		"gse_ch tokenization must record under gse_ch")
	require.Equal(t, gseBefore, countFor(models.PropertyTokenizationGse),
		"gse_ch tokenization must not record under gse")
}
