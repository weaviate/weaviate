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
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeStopwords map[string]struct{}

func (f fakeStopwords) IsStopword(word string) bool {
	_, ok := f[word]
	return ok
}

// assertBatchMatchesPerValue asserts that every value's tokens in the batch
// equal the Query result of a per-value Analyze call with the same inputs.
func assertBatchMatchesPerValue(t *testing.T, got *AnalyzedBatch, values []string,
	tokenization, className string, prepared *PreparedAnalyzer, stopwords StopwordDetector,
) {
	t.Helper()
	require.Equal(t, len(values), got.Len())
	for i, v := range values {
		want := Analyze(v, tokenization, className, prepared, stopwords).Query
		assert.Equal(t, want, append([]string{}, got.Tokens(i)...), "value %d (%q)", i, v)
	}
}

// TestAnalyzeBatchEquivalence is the drift guard for AnalyzeBatch: for every
// tokenization × stopword × folding combination, the batch result must equal
// Analyze called per value. A pipeline change applied to one entry point but
// not the other turns this red.
func TestAnalyzeBatchEquivalence(t *testing.T) {
	className := "EquivClass"
	// deliberately awkward inputs: plain multiword, mixed case + punctuation,
	// leading/trailing unicode whitespace, empty, whitespace-only,
	// stopword-only, foldable accents
	values := []string{
		"hello world",
		"Hello, World-Wide!",
		" \t padded value  ",
		"",
		"   ",
		"the",
		"café crème",
	}

	tokenizations := []string{
		models.PropertyTokenizationField,
		models.PropertyTokenizationWhitespace,
		models.PropertyTokenizationLowercase,
		models.PropertyTokenizationWord,
		models.PropertyTokenizationTrigram,
	}

	stopwordVariants := map[string]StopwordDetector{
		"no-stopwords": nil,
		"stopwords":    fakeStopwords{"the": {}, "world": {}},
	}

	analyzerVariants := map[string]*PreparedAnalyzer{
		"no-fold": nil,
		"fold":    NewPreparedAnalyzer(&models.TextAnalyzerConfig{ASCIIFold: true}),
	}

	for _, tok := range tokenizations {
		for swName, sw := range stopwordVariants {
			for prepName, prepared := range analyzerVariants {
				t.Run(fmt.Sprintf("%s/%s/%s", tok, swName, prepName), func(t *testing.T) {
					got := AnalyzeBatch(values, tok, className, prepared, sw)
					assertBatchMatchesPerValue(t, got, values, tok, className, prepared, sw)
				})
			}
		}
	}
}

// TestAnalyzeBatchEquivalenceCustomKagome covers both kagome custom
// user-dictionary branches, which AnalyzeBatch reaches through the same
// unrecorded dispatch, and pins that the batch's own throttle acquire is
// balanced.
//
// Named coverage gap: of the throttled global tokenizers, only gse_ch is
// exercised through the batch path (TestAnalyzeBatchEquivalenceThrottledGseCh
// below); gse and the env-enabled global kagome tokenizers are not.
func TestAnalyzeBatchEquivalenceCustomKagome(t *testing.T) {
	tests := []struct {
		name         string
		tokenization string
		dict         *models.TokenizerUserDictConfig
	}{
		{"custom KR dict", models.PropertyTokenizationKagomeKr, generateReplacementModel()},
		{"custom JA dict", models.PropertyTokenizationKagomeJa, jaCustomDict()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			className := "EquivClassKagome"
			require.NoError(t, AddCustomDict(className, []*models.TokenizerUserDictConfig{tt.dict}))
			defer func() {
				require.NoError(t, AddCustomDict(className, nil))
			}()

			values := []string{"Weaviate Semi Technologies", "We Aviate", ""}
			got := AnalyzeBatch(values, tt.tokenization, className, nil, nil)
			assertBatchMatchesPerValue(t, got, values, tt.tokenization, className, nil, nil)

			require.Zero(t, len(ApacTokenizerThrottle),
				"batch must release its throttle slot")
		})
	}
}

// TestAnalyzeBatchEquivalenceThrottledGseCh covers the throttled batch path:
// the batch acquires the ApacTokenizerThrottle in throttledBatchChunk-sized
// chunks, so the test spans several chunks and pins per-value equivalence
// with Analyze plus a balanced throttle afterwards.
//
// The inter-chunk slot yielding itself — a competing acquire getting in
// while the batch is mid-flight — is deliberately untested: asserting it
// races the batch's final release, so such a test could pass with a
// whole-batch hold.
func TestAnalyzeBatchEquivalenceThrottledGseCh(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	InitOptionalTokenizers()

	phrases := []string{"你好世界", "微维数据库", "向量搜索引擎", ""}
	values := make([]string, 3*throttledBatchChunk+1)
	for i := range values {
		values[i] = phrases[i%len(phrases)]
	}

	require.Zero(t, len(ApacTokenizerThrottle), "throttle must be empty before the batch")

	got := AnalyzeBatch(values, models.PropertyTokenizationGseCh, "C", nil, nil)
	assertBatchMatchesPerValue(t, got, values, models.PropertyTokenizationGseCh, "C", nil, nil)

	require.Zero(t, len(ApacTokenizerThrottle), "batch must release its throttle slot")
}

// TestAnalyzeBatchMetricsOncePerBatch pins the batch metric contract: one
// batch of N values adds exactly the summed token count under the
// tokenization's label — not one observation per value, and nothing under
// any other label.
func TestAnalyzeBatchMetricsOncePerBatch(t *testing.T) {
	countFor := func(label string) float64 {
		return testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues(label))
	}

	fieldBefore := countFor(models.PropertyTokenizationField)
	out := AnalyzeBatch([]string{" a ", "b", " c "}, models.PropertyTokenizationField, "C", nil, nil)
	require.Equal(t, 3, out.Len())
	require.Equal(t, fieldBefore+3, countFor(models.PropertyTokenizationField),
		"batch of 3 FIELD values must add exactly 3 tokens under field")

	wordBefore := countFor(models.PropertyTokenizationWord)
	AnalyzeBatch([]string{"one two", "three"}, models.PropertyTokenizationWord, "C", nil, nil)
	require.Equal(t, wordBefore+3, countFor(models.PropertyTokenizationWord),
		"batch token count is the summed indexed count")

	// empty batch records nothing
	fieldBefore = countFor(models.PropertyTokenizationField)
	out = AnalyzeBatch(nil, models.PropertyTokenizationField, "C", nil, nil)
	require.Zero(t, out.Len())
	require.Equal(t, fieldBefore, countFor(models.PropertyTokenizationField))
}

// TestAnalyzeBatchUnknownTokenization pins guard parity with the per-value
// path: an unknown tokenization yields empty results for every value and
// records nothing (no metric series under a bogus label).
func TestAnalyzeBatchUnknownTokenization(t *testing.T) {
	out := AnalyzeBatch([]string{"a", "b"}, "no-such-tokenization", "C", nil, nil)
	require.Equal(t, 2, out.Len())
	assert.Empty(t, out.Tokens(0))
	assert.Empty(t, out.Tokens(1))
	for _, v := range []string{"a", "b"} {
		assert.Empty(t, Analyze(v, "no-such-tokenization", "C", nil, nil).Query)
	}
	count := testutil.ToFloat64(monitoring.GetMetrics().TokenCount.WithLabelValues("no-such-tokenization"))
	assert.Zero(t, count, "unknown tokenization must not record metrics")
}

// TestAnalyzeBatchStopwordFilteredValue pins the shape contract the searcher
// relies on: a fully stopword-filtered value yields an EMPTY result at its
// index (not a shifted or missing entry).
func TestAnalyzeBatchStopwordFilteredValue(t *testing.T) {
	sw := fakeStopwords{"the": {}}
	out := AnalyzeBatch([]string{"keep", "the", "also-keep"},
		models.PropertyTokenizationField, "C", nil, sw)
	require.Equal(t, 3, out.Len())
	assert.Equal(t, []string{"keep"}, append([]string{}, out.Tokens(0)...))
	assert.Empty(t, out.Tokens(1))
	assert.Equal(t, []string{"also-keep"}, append([]string{}, out.Tokens(2)...))
}

// TestAnalyzedBatchAccessors pins the AnalyzedBatch API contract: All yields
// every value's index and tokens in order, each identical to Tokens(i);
// breaking out of All stops the iteration; views cannot clobber their
// neighbors through the shared backing array.
func TestAnalyzedBatchAccessors(t *testing.T) {
	out := AnalyzeBatch([]string{"one two", "", "three four five"},
		models.PropertyTokenizationWhitespace, "C", nil, nil)
	require.Equal(t, 3, out.Len())

	t.Run("All matches Tokens, in order", func(t *testing.T) {
		var seen []int
		for i, tokens := range out.All() {
			assert.Equal(t, out.Tokens(i), tokens)
			seen = append(seen, i)
		}
		assert.Equal(t, []int{0, 1, 2}, seen)
	})

	t.Run("Tokens per value", func(t *testing.T) {
		assert.Equal(t, []string{"one", "two"}, out.Tokens(0))
		assert.Empty(t, out.Tokens(1))
		assert.Equal(t, []string{"three", "four", "five"}, out.Tokens(2))
	})

	t.Run("break stops All early", func(t *testing.T) {
		var seen []int
		for i := range out.All() {
			seen = append(seen, i)
			if i == 1 {
				break
			}
		}
		assert.Equal(t, []int{0, 1}, seen)
	})

	t.Run("appending to one view cannot clobber the next value", func(t *testing.T) {
		grown := append(out.Tokens(0), "INJECTED")
		require.Equal(t, []string{"one", "two", "INJECTED"}, grown)
		assert.Equal(t, []string{"three", "four", "five"}, out.Tokens(2),
			"full-slice-capped views must force append to reallocate")
	})
}
