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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestAnalyzerFingerprint(t *testing.T) {
	prop := func(schemaTokenization string, analyzer *models.TextAnalyzerConfig) *models.Property {
		return &models.Property{
			Name:         "prop",
			DataType:     []string{"text"},
			Tokenization: schemaTokenization,
			TextAnalyzer: analyzer,
		}
	}

	tests := []struct {
		name string
		// a and b stand for two properties searched by the same cross-property
		// AND query, each with the tokenization its shard resolved for it.
		propA, propB *models.Property
		effA, effB   string
		wantEqual    bool
	}{
		{
			name:      "same schema tokenization, overlay resolved them differently",
			propA:     prop(models.PropertyTokenizationWord, nil),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, nil),
			effB:      models.PropertyTokenizationField,
			wantEqual: false,
		},
		{
			name:      "different schema tokenization, overlay resolved them alike",
			propA:     prop(models.PropertyTokenizationWord, nil),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationField, nil),
			effB:      models.PropertyTokenizationWord,
			wantEqual: true,
		},
		{
			name:      "identical",
			propA:     prop(models.PropertyTokenizationWord, nil),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, nil),
			effB:      models.PropertyTokenizationWord,
			wantEqual: true,
		},
		{
			name:      "asciiFold differs",
			propA:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{ASCIIFold: true}),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, nil),
			effB:      models.PropertyTokenizationWord,
			wantEqual: false,
		},
		{
			name:      "asciiFoldIgnore differs only in order",
			propA:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"ø", "é"}}),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é", "ø"}}),
			effB:      models.PropertyTokenizationWord,
			wantEqual: true,
		},
		{
			name:      "asciiFoldIgnore contents differ",
			propA:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"ø"}}),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}}),
			effB:      models.PropertyTokenizationWord,
			wantEqual: false,
		},
		{
			name:      "stopwordPreset differs",
			propA:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{StopwordPreset: "en"}),
			effA:      models.PropertyTokenizationWord,
			propB:     prop(models.PropertyTokenizationWord, &models.TextAnalyzerConfig{StopwordPreset: "none"}),
			effB:      models.PropertyTokenizationWord,
			wantEqual: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := analyzerFingerprint(test.propA, test.effA)
			b := analyzerFingerprint(test.propB, test.effB)
			if test.wantEqual {
				require.Equal(t, a, b)
			} else {
				require.NotEqual(t, a, b)
			}
		})
	}
}

// The overlay resolves each property independently, so a query can span one
// property already reindexed to its target tokenization and another still on the
// old one. Their schema settings are identical, so the operator has nothing but
// the effective tokenization to tell the two live analyzers apart.
func TestSharedCrossPropQueryTermsRejectsOverlaySplitTokenization(t *testing.T) {
	prop := &models.Property{
		Name:         "prop",
		DataType:     []string{"text"},
		Tokenization: models.PropertyTokenizationWord,
	}

	analyzerByTokenization := map[string]string{
		models.PropertyTokenizationWord:  analyzerFingerprint(prop, models.PropertyTokenizationWord),
		models.PropertyTokenizationField: analyzerFingerprint(prop, models.PropertyTokenizationField),
	}
	propNamesByTokenization := map[string][]string{
		models.PropertyTokenizationWord:  {"migrated"},
		models.PropertyTokenizationField: {"pending"},
	}
	queryTermsByTokenization := map[string][]string{
		models.PropertyTokenizationWord:  {"alpha", "beta"},
		models.PropertyTokenizationField: {"alpha beta"},
	}
	duplicateBoostsByTokenization := map[string][]int{
		models.PropertyTokenizationWord:  {1, 1},
		models.PropertyTokenizationField: {1},
	}

	_, _, err := sharedCrossPropQueryTerms(analyzerByTokenization, propNamesByTokenization,
		queryTermsByTokenization, duplicateBoostsByTokenization)
	require.ErrorContains(t, err, "tokenization")
}

func TestSharedCrossPropQueryTerms(t *testing.T) {
	tests := []struct {
		name string
		// Groups that tokenize the query identically but hold separate keys,
		// which is what per-property ASCIIFoldIgnore and stopword presets produce.
		analyzerByTokenization        map[string]string
		propNamesByTokenization       map[string][]string
		queryTermsByTokenization      map[string][]string
		duplicateBoostsByTokenization map[string][]int
		wantTerms                     []string
		wantBoosts                    []int
		wantErr                       bool
	}{
		{
			name:                          "single group is sorted",
			analyzerByTokenization:        map[string]string{"word": "fp"},
			propNamesByTokenization:       map[string][]string{"word": {"title"}},
			queryTermsByTokenization:      map[string][]string{"word": {"beta", "alpha"}},
			duplicateBoostsByTokenization: map[string][]int{"word": {2, 1}},
			wantTerms:                     []string{"alpha", "beta"},
			wantBoosts:                    []int{1, 2},
		},
		{
			name:                          "matching fingerprints across separate keys",
			analyzerByTokenization:        map[string]string{"word:a": "fp", "word:b": "fp"},
			propNamesByTokenization:       map[string][]string{"word:a": {"title"}, "word:b": {"body"}},
			queryTermsByTokenization:      map[string][]string{"word:a": {"beta", "alpha"}, "word:b": {"alpha", "beta"}},
			duplicateBoostsByTokenization: map[string][]int{"word:a": {2, 1}, "word:b": {1, 2}},
			wantTerms:                     []string{"alpha", "beta"},
			wantBoosts:                    []int{1, 2},
		},
		{
			name:                          "empty group is ignored",
			analyzerByTokenization:        map[string]string{"word": "fp", "field": "other"},
			propNamesByTokenization:       map[string][]string{"word": {"title"}, "field": {}},
			queryTermsByTokenization:      map[string][]string{"word": {"alpha"}, "field": {"alpha"}},
			duplicateBoostsByTokenization: map[string][]int{"word": {1}, "field": {1}},
			wantTerms:                     []string{"alpha"},
			wantBoosts:                    []int{1},
		},
		{
			name:                          "diverging fingerprints are rejected",
			analyzerByTokenization:        map[string]string{"word": "fp", "field": "other"},
			propNamesByTokenization:       map[string][]string{"word": {"title"}, "field": {"body"}},
			queryTermsByTokenization:      map[string][]string{"word": {"alpha"}, "field": {"alpha"}},
			duplicateBoostsByTokenization: map[string][]int{"word": {1}, "field": {1}},
			wantErr:                       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Group iteration is map-ordered, so repeat to catch a result that
			// depends on which group happens to be visited first.
			for range 16 {
				terms, boosts, err := sharedCrossPropQueryTerms(test.analyzerByTokenization,
					test.propNamesByTokenization, test.queryTermsByTokenization,
					test.duplicateBoostsByTokenization)
				if test.wantErr {
					require.ErrorContains(t, err, "tokenization")
					continue
				}
				require.Nil(t, err)
				require.Equal(t, test.wantTerms, terms)
				require.Equal(t, test.wantBoosts, boosts)
			}
		})
	}
}
