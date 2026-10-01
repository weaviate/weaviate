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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// tokenCount reads the TokenCount counter for label.
func tokenCount(label string) float64 {
	return testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues(label))
}

// tokensPerRequestObservations reads the number of TokenCountPerRequest
// histogram observations recorded for label.
func tokensPerRequestObservations(t *testing.T, label string) uint64 {
	t.Helper()
	var m dto.Metric
	observer := monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues(label)
	require.NoError(t, observer.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount()
}

// tokenizerRequests reads the TokenizerRequests counter for label.
func tokenizerRequests(label string) float64 {
	return testutil.ToFloat64(monitoring.GetMetrics().TokenizerRequests.WithLabelValues(label))
}

// TestTokenizeMetricsRecordedAtDispatch pins that tokenization metrics are
// recorded once, under the dispatched tokenization's own label: a lowercase
// call must not also add to the whitespace count, even though lowercase
// tokenization delegates to the whitespace splitter internally.
func TestTokenizeMetricsRecordedAtDispatch(t *testing.T) {
	lowercaseBefore := tokenCount(models.PropertyTokenizationLowercase)
	whitespaceBefore := tokenCount(models.PropertyTokenizationWhitespace)

	tokens := Tokenize(models.PropertyTokenizationLowercase, "Hello World")
	require.Equal(t, []string{"hello", "world"}, tokens)

	require.Equal(t, lowercaseBefore+2, tokenCount(models.PropertyTokenizationLowercase),
		"lowercase must record its own token count")
	require.Equal(t, whitespaceBefore, tokenCount(models.PropertyTokenizationWhitespace),
		"lowercase must not count its internal whitespace delegation under the whitespace label")

	whitespaceBefore = tokenCount(models.PropertyTokenizationWhitespace)
	tokens = Tokenize(models.PropertyTokenizationWhitespace, "Hello World")
	require.Equal(t, []string{"Hello", "World"}, tokens)
	require.Equal(t, whitespaceBefore+2, tokenCount(models.PropertyTokenizationWhitespace),
		"direct whitespace tokenization still records under whitespace")

	// The lowercase() helper is shared by the word (and wildcard) tokenizers;
	// it must not record anything itself, or word tokenization pollutes the
	// "lowercase" label.
	lowercaseBefore = tokenCount(models.PropertyTokenizationLowercase)
	wordBefore := tokenCount(models.PropertyTokenizationWord)
	tokens = Tokenize(models.PropertyTokenizationWord, "Hello World")
	require.Equal(t, []string{"hello", "world"}, tokens)
	require.Equal(t, wordBefore+2, tokenCount(models.PropertyTokenizationWord),
		"word tokenization records under word")
	require.Equal(t, lowercaseBefore, tokenCount(models.PropertyTokenizationLowercase),
		"word tokenization must not count its lowercasing under the lowercase label")
}

// TestTrigramWithWildcardsMetricsAndOutput pins that a trigram-with-wildcards
// call records only under trigram_with_wildcards: its internal
// word-with-wildcards split must not also count under word_with_wildcards.
// Doubles as the only output assertion for the trigram-wildcards path.
func TestTrigramWithWildcardsMetricsAndOutput(t *testing.T) {
	wordBefore := tokenCount("word_with_wildcards")
	trigramBefore := tokenCount("trigram_with_wildcards")

	tokens := TokenizeWithWildcardsForClass(models.PropertyTokenizationTrigram, "Hello W?rld*", "")
	require.Equal(t, []string{"hel", "ell", "llo", "low", "ow?", "w?r", "?rl", "rld", "ld*"}, tokens)

	require.Equal(t, trigramBefore+float64(len(tokens)), tokenCount("trigram_with_wildcards"),
		"trigram wildcards must record its token count under its own label")
	require.Equal(t, wordBefore, tokenCount("word_with_wildcards"),
		"trigram wildcards must not count its internal word split under word_with_wildcards")
}

// TestGseChRecordsUnderOwnLabel pins that Chinese tokenization records under
// its own "gse_ch" label, not under "gse", so operators can tell the two
// tokenizers' volume apart.
func TestGseChRecordsUnderOwnLabel(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	InitOptionalTokenizers()

	gseBefore := tokenCount(models.PropertyTokenizationGse)
	gseChBefore := tokenCount(models.PropertyTokenizationGseCh)

	tokens := Tokenize(models.PropertyTokenizationGseCh, "你好世界")
	require.NotEmpty(t, tokens)

	require.Equal(t, gseChBefore+float64(len(tokens)), tokenCount(models.PropertyTokenizationGseCh),
		"gse_ch tokenization must record under gse_ch")
	require.Equal(t, gseBefore, tokenCount(models.PropertyTokenizationGse),
		"gse_ch tokenization must not record under gse")
}

// TestTokenizerRequestsIncrementsPerCall pins tokenizer_requests_total, which
// was declared but never written: metricsFor wired three of the four bound
// metrics and omitted this one, so the counter could never leave zero.
//
// One public tokenization call must increment exactly once, under the
// dispatched label only — a tokenizer that delegates internally (word ->
// lowercase, trigram-with-wildcards -> word-with-wildcards) must not also
// increment its delegate, exactly as the token counters behave.
func TestTokenizerRequestsIncrementsPerCall(t *testing.T) {
	t.Run("increments once under the dispatched label", func(t *testing.T) {
		before := tokenizerRequests(models.PropertyTokenizationWhitespace)
		Tokenize(models.PropertyTokenizationWhitespace, "Hello World")
		require.Equal(t, before+1, tokenizerRequests(models.PropertyTokenizationWhitespace),
			"one call must record exactly one request")
	})

	t.Run("counts requests, not tokens", func(t *testing.T) {
		before := tokenizerRequests(models.PropertyTokenizationWhitespace)
		Tokenize(models.PropertyTokenizationWhitespace, "one two three four five")
		require.Equal(t, before+1, tokenizerRequests(models.PropertyTokenizationWhitespace),
			"a five-token input is still a single request")
	})

	t.Run("delegated paths do not double count", func(t *testing.T) {
		wordBefore := tokenizerRequests(models.PropertyTokenizationWord)
		lowercaseBefore := tokenizerRequests(models.PropertyTokenizationLowercase)

		Tokenize(models.PropertyTokenizationWord, "Hello World")

		require.Equal(t, wordBefore+1, tokenizerRequests(models.PropertyTokenizationWord),
			"word tokenization records its own request")
		require.Equal(t, lowercaseBefore, tokenizerRequests(models.PropertyTokenizationLowercase),
			"word tokenization must not record a request under its lowercase delegate")
	})

	t.Run("wildcard delegation does not double count", func(t *testing.T) {
		trigramBefore := tokenizerRequests("trigram_with_wildcards")
		wordBefore := tokenizerRequests("word_with_wildcards")

		TokenizeWithWildcardsForClass(models.PropertyTokenizationTrigram, "Hello W?rld*", "")

		require.Equal(t, trigramBefore+1, tokenizerRequests("trigram_with_wildcards"),
			"trigram wildcards records its own request")
		require.Equal(t, wordBefore, tokenizerRequests("word_with_wildcards"),
			"trigram wildcards must not record a request under its internal word split")
	})

	t.Run("tracks the duration observations one-for-one", func(t *testing.T) {
		label := models.PropertyTokenizationWhitespace
		reqBefore := tokenizerRequests(label)
		obsBefore := tokensPerRequestObservations(t, label)

		for i := 0; i < 3; i++ {
			Tokenize(label, "alpha beta")
		}

		require.Equal(t, reqBefore+3, tokenizerRequests(label))
		require.Equal(t, obsBefore+3, tokensPerRequestObservations(t, label),
			"requests and per-request observations must stay in step")
	})
}
