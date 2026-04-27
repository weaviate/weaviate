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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"

	tokenizeops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/tokenize"
)

func strPtr(s string) *string { return &s }

func TestHandleGenericTokenize(t *testing.T) {
	tests := []struct {
		name        string
		body        *models.TokenizeRequest
		wantOK      bool
		wantIndexed []string
		wantQuery   []string
	}{
		{
			// Word tokenization defaults to the "en" stopword preset when
			// no analyzerConfig is supplied, matching the property-level
			// endpoint (which inherits the collection's default, also "en").
			name: "word tokenization defaults to en stopwords",
			body: &models.TokenizeRequest{
				Text:         strPtr("The quick brown fox"),
				Tokenization: strPtr("word"),
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"quick", "brown", "fox"},
		},
		{
			// Callers can opt out of the default by passing "none".
			name: "word tokenization with explicit stopwordPreset none disables default",
			body: &models.TokenizeRequest{
				Text:         strPtr("The quick brown fox"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "none",
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"the", "quick", "brown", "fox"},
		},
		{
			name: "lowercase tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("Hello World Test"),
				Tokenization: strPtr("lowercase"),
			},
			wantOK:      true,
			wantIndexed: []string{"hello", "world", "test"},
			wantQuery:   []string{"hello", "world", "test"},
		},
		{
			name: "whitespace tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("Hello World Test"),
				Tokenization: strPtr("whitespace"),
			},
			wantOK:      true,
			wantIndexed: []string{"Hello", "World", "Test"},
			wantQuery:   []string{"Hello", "World", "Test"},
		},
		{
			name: "field tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("  Hello World  "),
				Tokenization: strPtr("field"),
			},
			wantOK:      true,
			wantIndexed: []string{"Hello World"},
			wantQuery:   []string{"Hello World"},
		},
		{
			name: "trigram tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("Hello"),
				Tokenization: strPtr("trigram"),
			},
			wantOK:      true,
			wantIndexed: []string{"hel", "ell", "llo"},
			wantQuery:   []string{"hel", "ell", "llo"},
		},
		{
			name: "disabled tokenizer returns bad request",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("gse"),
			},
			wantOK: false,
		},
		{
			name: "ascii fold enabled",
			body: &models.TokenizeRequest{
				Text:         strPtr("L'école est fermée"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					ASCIIFold: true,
				},
			},
			wantOK:      true,
			wantIndexed: []string{"l", "ecole", "est", "fermee"},
			wantQuery:   []string{"l", "ecole", "est", "fermee"},
		},
		{
			name: "ascii fold with ignore",
			body: &models.TokenizeRequest{
				Text:         strPtr("L'école est fermée"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					ASCIIFold:       true,
					ASCIIFoldIgnore: []string{"é"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"l", "école", "est", "fermée"},
			wantQuery:   []string{"l", "école", "est", "fermée"},
		},
		{
			name: "stopword preset en",
			body: &models.TokenizeRequest{
				Text:         strPtr("The quick brown fox"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "en",
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"quick", "brown", "fox"},
		},
		{
			// Define a custom preset with a plain word list (collection shape)
			// and reference it via analyzerConfig.stopwordPreset.
			name: "custom stopword preset resolved by analyzerConfig",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello world test"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "custom",
				},
				StopwordPresets: map[string][]string{
					"custom": {"test"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"hello", "world", "test"},
			wantQuery:   []string{"hello", "world"},
		},
		{
			// "en minus the" via the stopwords fallback field: the caller sets
			// preset=en with a removal rather than composing a named preset.
			name: "stopwords fallback composes removals on top of en",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick"),
				Tokenization: strPtr("word"),
				Stopwords: &models.StopwordConfig{
					Preset:   "en",
					Removals: []string{"the"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick"},
			wantQuery:   []string{"the", "quick"},
		},
		{
			name: "ascii fold combined with stopwords",
			body: &models.TokenizeRequest{
				Text:         strPtr("The école est fermée"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					ASCIIFold:      true,
					StopwordPreset: "en",
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "ecole", "est", "fermee"},
			wantQuery:   []string{"ecole", "est", "fermee"},
		},
		{
			name: "unknown stopword preset is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello world"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "nonexistent",
				},
			},
			wantOK: false,
		},
		{
			// Word tokenization with no config still gets the default "en"
			// preset, even when no tokens happen to be English stopwords.
			name: "nil configs defaults to en",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello world"),
				Tokenization: strPtr("word"),
			},
			wantOK:      true,
			wantIndexed: []string{"hello", "world"},
			wantQuery:   []string{"hello", "world"},
		},
		{
			name: "ascii fold ignore without fold enabled is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					ASCIIFold:       false,
					ASCIIFoldIgnore: []string{"é"},
				},
			},
			wantOK: false,
		},
		{
			name: "multi-character ignore entry is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					ASCIIFold:       true,
					ASCIIFoldIgnore: []string{"ab"},
				},
			},
			wantOK: false,
		},
		{
			// A user-defined preset with the same name as a built-in ("en")
			// fully replaces the built-in, matching collection-level
			// override semantics.
			name: "user-defined preset replaces built-in of same name",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick hello world"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "en",
				},
				StopwordPresets: map[string][]string{
					// Only "hello" is an "en" stopword; "the" passes through
					// because the built-in en list is no longer applied.
					"en": {"hello"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "hello", "world"},
			wantQuery:   []string{"the", "quick", "world"},
		},
		{
			// Top-level stopwords is used as the fallback when
			// analyzerConfig.stopwordPreset is not set, matching the
			// property endpoint's inheritance from collection config.
			name: "stopwords used as fallback",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick brown fox"),
				Tokenization: strPtr("word"),
				Stopwords: &models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"quick"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"brown", "fox"},
		},
		{
			// analyzerConfig.stopwordPreset can reference a user-defined
			// preset declared in top-level stopwordPresets (plain word list,
			// collection-shape).
			name: "stopwordPresets resolved by analyzerConfig preset",
			body: &models.TokenizeRequest{
				Text:         strPtr("le chat et la souris"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "fr",
				},
				StopwordPresets: map[string][]string{
					"fr": {"le", "la", "et"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"le", "chat", "et", "la", "souris"},
			wantQuery:   []string{"chat", "souris"},
		},
		{
			// analyzerConfig.stopwordPreset overrides the stopwords
			// fallback, same as a property-level preset override at the
			// collection level.
			name: "analyzerConfig preset overrides stopwords fallback",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "none",
				},
				Stopwords: &models.StopwordConfig{Preset: "en"},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick"},
			wantQuery:   []string{"the", "quick"},
		},
		{
			// Unknown preset referenced by analyzerConfig is rejected.
			name: "unknown preset against stopwordPresets is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				AnalyzerConfig: &models.TextAnalyzerConfig{
					StopwordPreset: "missing",
				},
				StopwordPresets: map[string][]string{
					"other": {"hello"},
				},
			},
			wantOK: false,
		},
		{
			// A user override for "en" is respected even when the caller did
			// not explicitly reference it via analyzerConfig. This matches
			// collection-level semantics where a user-defined "en" replaces
			// the built-in entirely.
			name: "user override for built-in en applies to default",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick hello world"),
				Tokenization: strPtr("word"),
				StopwordPresets: map[string][]string{
					// Replaces the built-in "en" entirely — only "hello" is
					// now an "en" stopword; "the" passes through.
					"en": {"hello"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "hello", "world"},
			wantQuery:   []string{"the", "quick", "world"},
		},
		{
			// stopwords with an empty Preset must be defaulted to "en",
			// matching collection-level validation
			// (adapters/repos/db/inverted/config.go: validateStopwordConfig).
			// Without that defaulting, NewDetectorFromConfig would build an
			// empty detector and no stopwords would be filtered.
			name: "stopwords empty preset defaults to en",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick"),
				Tokenization: strPtr("word"),
				Stopwords:    &models.StopwordConfig{},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick"},
			wantQuery:   []string{"quick"},
		},
		{
			// Default "en" + additions: caller omits preset, passes only
			// additions. Validation defaults preset to "en" and the
			// detector is built from en + additions.
			name: "stopwords additions without preset default to en plus additions",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick hello world"),
				Tokenization: strPtr("word"),
				Stopwords:    &models.StopwordConfig{Additions: []string{"hello"}},
			},
			wantOK: true,
			// "the" filtered by default en, "hello" filtered by caller's addition.
			wantIndexed: []string{"the", "quick", "hello", "world"},
			wantQuery:   []string{"quick", "world"},
		},
		{
			// Default "en" - removals: caller omits preset, passes only
			// removals. "the" is removed from the en list so it passes
			// through the query.
			name: "stopwords removals without preset default to en minus removals",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick is fast"),
				Tokenization: strPtr("word"),
				Stopwords:    &models.StopwordConfig{Removals: []string{"the"}},
			},
			wantOK: true,
			// "is" is still an en stopword, "the" was removed.
			wantIndexed: []string{"the", "quick", "is", "fast"},
			wantQuery:   []string{"the", "quick", "fast"},
		},
		{
			// Passing both stopwords and stopwordPresets is rejected:
			// the two input shapes are mutually exclusive to keep the
			// mental model simple. Callers must pick one.
			name: "both stopwords and stopwordPresets is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				Stopwords:    &models.StopwordConfig{Preset: "en"},
				StopwordPresets: map[string][]string{
					"custom": {"hello"},
				},
			},
			wantOK: false,
		},
		{
			// Default "en" + additions + removals applied together.
			name: "stopwords additions and removals without preset compose on en",
			body: &models.TokenizeRequest{
				Text:         strPtr("the quick hello"),
				Tokenization: strPtr("word"),
				Stopwords: &models.StopwordConfig{
					Additions: []string{"hello"},
					Removals:  []string{"the"},
				},
			},
			wantOK:      true,
			wantIndexed: []string{"the", "quick", "hello"},
			wantQuery:   []string{"the", "quick"},
		},
		{
			// Unknown preset name in stopwords must be rejected the same
			// way collection creation would reject it.
			name: "stopwords unknown preset is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				Stopwords:    &models.StopwordConfig{Preset: "nonexistent"},
			},
			wantOK: false,
		},
		{
			// An empty word list for a user-defined preset is rejected by
			// collection-level validateStopwordPresets, and the tokenize
			// endpoint must reject it the same way.
			name: "stopwordPresets empty list is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				StopwordPresets: map[string][]string{
					"custom": {},
				},
			},
			wantOK: false,
		},
		{
			// Same as collection-level: the same word cannot appear in both
			// additions and removals of stopwords.
			name: "stopwords conflicting addition and removal is rejected",
			body: &models.TokenizeRequest{
				Text:         strPtr("hello"),
				Tokenization: strPtr("word"),
				Stopwords: &models.StopwordConfig{
					Preset:    "en",
					Additions: []string{"foo"},
					Removals:  []string{"foo"},
				},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tokenizeops.TokenizeParams{Body: tt.body}
			resp := genericTokenize(params)

			if tt.wantOK {
				okResp, ok := resp.(*tokenizeops.TokenizeOK)
				require.True(t, ok, "expected TokenizeOK response")
				assert.Equal(t, tt.wantIndexed, okResp.Payload.Indexed)
				assert.Equal(t, tt.wantQuery, okResp.Payload.Query)
			} else {
				_, ok := resp.(*tokenizeops.TokenizeUnprocessableEntity)
				assert.True(t, ok, "expected TokenizeUnprocessableEntity response")
			}
		})
	}
}

func TestAnalyzeStopwords(t *testing.T) {
	tests := []struct {
		name        string
		text        string
		config      models.StopwordConfig
		wantIndexed []string
		wantQuery   []string
	}{
		{
			name:        "english preset",
			text:        "the quick brown fox",
			config:      models.StopwordConfig{Preset: "en"},
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"quick", "brown", "fox"},
		},
		{
			name:        "no preset",
			text:        "the quick brown fox",
			config:      models.StopwordConfig{Preset: "none"},
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"the", "quick", "brown", "fox"},
		},
		{
			name:        "custom additions",
			text:        "hello world test",
			config:      models.StopwordConfig{Preset: "none", Additions: []string{"test"}},
			wantIndexed: []string{"hello", "world", "test"},
			wantQuery:   []string{"hello", "world"},
		},
		{
			name:        "english with removals",
			text:        "the quick",
			config:      models.StopwordConfig{Preset: "en", Removals: []string{"the"}},
			wantIndexed: []string{"the", "quick"},
			wantQuery:   []string{"the", "quick"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector, err := stopwords.NewDetectorFromConfig(tt.config)
			require.NoError(t, err)
			result := tokenizer.Analyze(tt.text, "word", "", nil, detector)
			assert.Equal(t, tt.wantIndexed, result.Indexed)
			assert.Equal(t, tt.wantQuery, result.Query)
		})
	}
}

func TestAnalyzeFoldAndTokenize(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		tokeniz      string
		textAnalyzer *models.TextAnalyzerConfig
		wantIndexed  []string
	}{
		{
			name:         "word: fold all accents",
			text:         "L'école est fermée",
			tokeniz:      "word",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true},
			wantIndexed:  []string{"l", "ecole", "est", "fermee"},
		},
		{
			name:         "word: fold with é ignored",
			text:         "L'école est fermée",
			tokeniz:      "word",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
			wantIndexed:  []string{"l", "école", "est", "fermée"},
		},
		{
			name:         "word: fold with multiple ignores",
			text:         "naïve café résumé",
			tokeniz:      "word",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
			wantIndexed:  []string{"naive", "café", "résumé"},
		},
		{
			name:        "word: no fold preserves all accents",
			text:        "L'école est fermée",
			tokeniz:     "word",
			wantIndexed: []string{"l", "école", "est", "fermée"},
		},
		{
			name:         "lowercase: fold all accents",
			text:         "L'école est fermée",
			tokeniz:      "lowercase",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true},
			wantIndexed:  []string{"l'ecole", "est", "fermee"},
		},
		{
			name:         "lowercase: fold with é ignored",
			text:         "L'école est fermée",
			tokeniz:      "lowercase",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
			wantIndexed:  []string{"l'école", "est", "fermée"},
		},
		{
			name:         "whitespace: fold all accents",
			text:         "São Paulo café",
			tokeniz:      "whitespace",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true},
			wantIndexed:  []string{"Sao", "Paulo", "cafe"},
		},
		{
			name:         "field: fold all accents",
			text:         "  café résumé  ",
			tokeniz:      "field",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true},
			wantIndexed:  []string{"cafe resume"},
		},
		{
			name:         "field: fold with é ignored",
			text:         "  café résumé  ",
			tokeniz:      "field",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
			wantIndexed:  []string{"café résumé"},
		},
		{
			name:         "trigram: fold all accents",
			text:         "école",
			tokeniz:      "trigram",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true},
			wantIndexed:  []string{"eco", "col", "ole"},
		},
		{
			name:         "trigram: fold with é ignored",
			text:         "école",
			tokeniz:      "trigram",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
			wantIndexed:  []string{"éco", "col", "ole"},
		},
		{
			name:         "word: uppercase ignored char also preserved",
			text:         "Ørsted ørsted",
			tokeniz:      "word",
			textAnalyzer: &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"ø"}},
			wantIndexed:  []string{"ørsted", "ørsted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared := tokenizer.NewPreparedAnalyzer(tt.textAnalyzer)
			result := tokenizer.Analyze(tt.text, tt.tokeniz, "", prepared, nil)
			assert.Equal(t, tt.wantIndexed, result.Indexed)
		})
	}
}

func TestHandleGenericTokenizeGSE(t *testing.T) {
	t.Setenv("USE_GSE", "true")
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	tokenizer.InitOptionalTokenizers()

	tests := []struct {
		name        string
		body        *models.TokenizeRequest
		wantIndexed []string
	}{
		{
			name: "gse Japanese tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("素早い茶色の狐が怠けた犬を飛び越えた"),
				Tokenization: strPtr("gse"),
			},
			wantIndexed: []string{"素早", "素早い", "早い", "茶色", "の", "狐", "が", "怠け", "けた", "犬", "を", "飛び", "飛び越え", "越え", "た"},
		},
		{
			name: "gse_ch Chinese tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("施氏食狮史"),
				Tokenization: strPtr("gse_ch"),
			},
			wantIndexed: []string{"施", "氏", "食", "狮", "史"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tokenizeops.TokenizeParams{Body: tt.body}
			resp := genericTokenize(params)

			okResp, ok := resp.(*tokenizeops.TokenizeOK)
			require.True(t, ok, "expected TokenizeOK response")
			assert.Equal(t, tt.wantIndexed, okResp.Payload.Indexed)
		})
	}
}

func TestHandleGenericTokenizeKagome(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	t.Setenv("ENABLE_TOKENIZER_KAGOME_JA", "true")
	tokenizer.InitOptionalTokenizers()

	tests := []struct {
		name        string
		body        *models.TokenizeRequest
		wantIndexed []string
	}{
		{
			name: "kagome_kr Korean tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("아버지가 방에 들어가신다"),
				Tokenization: strPtr("kagome_kr"),
			},
			wantIndexed: []string{"아버지", "가", "방", "에", "들어가", "신다"},
		},
		{
			name: "kagome_kr Korean without spaces",
			body: &models.TokenizeRequest{
				Text:         strPtr("한국어를처리하는예시입니다"),
				Tokenization: strPtr("kagome_kr"),
			},
			wantIndexed: []string{"한국어", "를", "처리", "하", "는", "예시", "입니다"},
		},
		{
			name: "kagome_ja Japanese tokenization",
			body: &models.TokenizeRequest{
				Text:         strPtr("素早い茶色の狐が怠けた犬を飛び越えた"),
				Tokenization: strPtr("kagome_ja"),
			},
			wantIndexed: []string{"素早い", "茶色", "の", "狐", "が", "怠け", "た", "犬", "を", "飛び越え", "た"},
		},
		{
			name: "kagome_ja English text passthrough",
			body: &models.TokenizeRequest{
				Text:         strPtr("The quick brown fox jumps over the lazy dog"),
				Tokenization: strPtr("kagome_ja"),
			},
			wantIndexed: []string{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tokenizeops.TokenizeParams{Body: tt.body}
			resp := genericTokenize(params)

			okResp, ok := resp.(*tokenizeops.TokenizeOK)
			require.True(t, ok, "expected TokenizeOK response")
			assert.Equal(t, tt.wantIndexed, okResp.Payload.Indexed)
		})
	}
}

func TestValidateAnalyzerConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *models.TextAnalyzerConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name: "nil config",
			cfg:  nil,
		},
		{
			name: "empty config",
			cfg:  &models.TextAnalyzerConfig{},
		},
		{
			name: "fold enabled no ignore",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true},
		},
		{
			name: "fold enabled with valid ignore",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é", "ñ"}},
		},
		{
			name: "fold enabled with NFD single char",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"e\u0301"}},
		},
		{
			name:      "ignore without fold",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: false, ASCIIFoldIgnore: []string{"é"}},
			wantErr:   true,
			errSubstr: "asciiFoldIgnore requires asciiFold",
		},
		{
			name:      "multi-character entry",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"ab"}},
			wantErr:   true,
			errSubstr: "single character",
		},
		{
			name:      "empty string entry",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{""}},
			wantErr:   true,
			errSubstr: "single character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAnalyzerConfig(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
