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
