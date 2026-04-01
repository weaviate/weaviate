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
			name: "word tokenization without stopwords",
			body: &models.TokenizeRequest{
				Text:         strPtr("The quick brown fox"),
				Tokenization: strPtr("word"),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tokenizeops.TokenizeParams{Body: tt.body}
			resp := genericTokenize(params)

			if tt.wantOK {
				okResp, ok := resp.(*tokenizeops.TokenizeOK)
				require.True(t, ok, "expected TokenizeOK response")
				assert.Equal(t, *tt.body.Tokenization, okResp.Payload.Tokenization)
				assert.Equal(t, tt.wantIndexed, okResp.Payload.Indexed)
				assert.Equal(t, tt.wantQuery, okResp.Payload.Query)
			} else {
				_, ok := resp.(*tokenizeops.TokenizeUnprocessableEntity)
				assert.True(t, ok, "expected TokenizeUnprocessableEntity response")
			}
		})
	}
}

func TestFilterStopwords(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []string
		config   models.StopwordConfig
		expected []string
	}{
		{
			name:     "english preset",
			tokens:   []string{"the", "quick", "brown", "fox"},
			config:   models.StopwordConfig{Preset: "en"},
			expected: []string{"quick", "brown", "fox"},
		},
		{
			name:     "no preset",
			tokens:   []string{"the", "quick", "brown", "fox"},
			config:   models.StopwordConfig{Preset: "none"},
			expected: []string{"the", "quick", "brown", "fox"},
		},
		{
			name:     "custom additions",
			tokens:   []string{"hello", "world", "test"},
			config:   models.StopwordConfig{Preset: "none", Additions: []string{"test"}},
			expected: []string{"hello", "world"},
		},
		{
			name:     "english with removals",
			tokens:   []string{"the", "quick"},
			config:   models.StopwordConfig{Preset: "en", Removals: []string{"the"}},
			expected: []string{"the", "quick"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector, err := stopwords.NewDetectorFromConfig(tt.config)
			require.NoError(t, err)
			result := removeStopwords(tt.tokens, detector)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestASCIIFoldThenTokenize(t *testing.T) {
	tests := []struct {
		name        string
		text        string
		tokeniz     string
		asciiFold   bool
		ignore      []string
		wantIndexed []string
	}{
		{
			name:        "word: fold all accents",
			text:        "L'école est fermée",
			tokeniz:     "word",
			asciiFold:   true,
			wantIndexed: []string{"l", "ecole", "est", "fermee"},
		},
		{
			name:        "word: fold with é ignored",
			text:        "L'école est fermée",
			tokeniz:     "word",
			asciiFold:   true,
			ignore:      []string{"é"},
			wantIndexed: []string{"l", "école", "est", "fermée"},
		},
		{
			name:        "word: fold with multiple ignores",
			text:        "naïve café résumé",
			tokeniz:     "word",
			asciiFold:   true,
			ignore:      []string{"é"},
			wantIndexed: []string{"naive", "café", "résumé"},
		},
		{
			name:        "word: no fold preserves all accents",
			text:        "L'école est fermée",
			tokeniz:     "word",
			asciiFold:   false,
			wantIndexed: []string{"l", "école", "est", "fermée"},
		},
		{
			name:        "lowercase: fold all accents",
			text:        "L'école est fermée",
			tokeniz:     "lowercase",
			asciiFold:   true,
			wantIndexed: []string{"l'ecole", "est", "fermee"},
		},
		{
			name:        "lowercase: fold with é ignored",
			text:        "L'école est fermée",
			tokeniz:     "lowercase",
			asciiFold:   true,
			ignore:      []string{"é"},
			wantIndexed: []string{"l'école", "est", "fermée"},
		},
		{
			name:        "whitespace: fold all accents",
			text:        "São Paulo café",
			tokeniz:     "whitespace",
			asciiFold:   true,
			wantIndexed: []string{"Sao", "Paulo", "cafe"},
		},
		{
			name:        "field: fold all accents",
			text:        "  café résumé  ",
			tokeniz:     "field",
			asciiFold:   true,
			wantIndexed: []string{"cafe resume"},
		},
		{
			name:        "field: fold with é ignored",
			text:        "  café résumé  ",
			tokeniz:     "field",
			asciiFold:   true,
			ignore:      []string{"é"},
			wantIndexed: []string{"café résumé"},
		},
		{
			name:        "trigram: fold all accents",
			text:        "école",
			tokeniz:     "trigram",
			asciiFold:   true,
			wantIndexed: []string{"eco", "col", "ole"},
		},
		{
			name:        "trigram: fold with é ignored",
			text:        "école",
			tokeniz:     "trigram",
			asciiFold:   true,
			ignore:      []string{"é"},
			wantIndexed: []string{"éco", "col", "ole"},
		},
		{
			name:        "word: uppercase ignored char also preserved",
			text:        "Ørsted ørsted",
			tokeniz:     "word",
			asciiFold:   true,
			ignore:      []string{"ø"},
			wantIndexed: []string{"ørsted", "ørsted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text := tt.text
			if tt.asciiFold {
				ignore := tokenizer.BuildIgnoreSet(tt.ignore)
				text = tokenizer.FoldASCII(text, ignore)
			}
			indexed := tokenizer.Tokenize(tt.tokeniz, text)
			assert.Equal(t, tt.wantIndexed, indexed)
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
			assert.Equal(t, *tt.body.Tokenization, okResp.Payload.Tokenization)
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
			assert.Equal(t, *tt.body.Tokenization, okResp.Payload.Tokenization)
			assert.Equal(t, tt.wantIndexed, okResp.Payload.Indexed)
		})
	}
}
