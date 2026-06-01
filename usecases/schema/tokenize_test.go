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

package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

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
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
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

// assertTokenizeErr checks that err is a *TokenizeError of the expected kind
// and that its message contains errSubstr (if non-empty).
func assertTokenizeErr(t *testing.T, err error, wantKind TokenizeErrorKind, errSubstr string) {
	t.Helper()
	require.Error(t, err)
	var te *TokenizeError
	require.ErrorAs(t, err, &te)
	assert.Equal(t, wantKind, te.Kind)
	if errSubstr != "" {
		assert.Contains(t, err.Error(), errSubstr)
	}
}

// assertTokenizeResult checks the indexed and query slices of a successful result.
func assertTokenizeResult(t *testing.T, res *TokenizeResult, wantIndexed, wantQuery []string) {
	t.Helper()
	require.NotNil(t, res)
	if wantIndexed != nil {
		assert.Equal(t, wantIndexed, res.Indexed)
	}
	if wantQuery != nil {
		assert.Equal(t, wantQuery, res.Query)
	}
}

func TestGenericTokenize(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		tokenization string
		analyzerCfg  *models.TextAnalyzerConfig
		stopwordsCfg *models.StopwordConfig
		presets      map[string][]string
		wantErr      bool
		wantErrKind  TokenizeErrorKind
		errSubstr    string
		wantIndexed  []string
		wantQuery    []string
	}{
		{
			name:         "unsupported tokenization strategy",
			text:         "hello world",
			tokenization: "bogus",
			wantErr:      true,
			wantErrKind:  TokenizeErrInvalid,
			errSubstr:    "unsupported tokenization strategy",
		},
		{
			// Word tokenization with no analyzerConfig defaults the stopword
			// preset to "en" so Query filters out "the", while Indexed keeps it
			// — matches the production property-level path under the default
			// inverted-index config.
			name:         "word tokenization defaults to en stopwords",
			text:         "The quick brown fox",
			tokenization: models.PropertyTokenizationWord,
			wantIndexed:  []string{"the", "quick", "brown", "fox"},
			wantQuery:    []string{"quick", "brown", "fox"},
		},
		{
			name:         "unknown stopword preset rejected",
			text:         "hello",
			tokenization: models.PropertyTokenizationWord,
			analyzerCfg:  &models.TextAnalyzerConfig{StopwordPreset: "no_such_preset"},
			wantErr:      true,
			wantErrKind:  TokenizeErrInvalid,
			errSubstr:    "unknown stopword preset",
		},
		{
			name:         "rune-counted length cap rejects oversize ASCII",
			text:         strings.Repeat("a", 10001),
			tokenization: models.PropertyTokenizationWord,
			wantErr:      true,
			wantErrKind:  TokenizeErrInvalid,
			errSubstr:    "exceeds maximum",
		},
		{
			// Regression: a byte-length cap would reject this (3 bytes × 4000
			// runes = 12000 bytes > 10000) even though it's only 4000 runes.
			// Rune-count cap accepts it.
			name:         "non-ASCII payload under rune cap accepted",
			text:         strings.Repeat("漢", 4000),
			tokenization: models.PropertyTokenizationWord,
		},
		{
			name:         "mutually exclusive stopwords and stopwordPresets",
			text:         "hello",
			tokenization: models.PropertyTokenizationWord,
			stopwordsCfg: &models.StopwordConfig{Preset: "en"},
			presets:      map[string][]string{"custom": {"the"}},
			wantErr:      true,
			wantErrKind:  TokenizeErrInvalid,
			errSubstr:    "mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := GenericTokenize(tt.text, tt.tokenization, tt.analyzerCfg, tt.stopwordsCfg, tt.presets)
			if tt.wantErr {
				assertTokenizeErr(t, err, tt.wantErrKind, tt.errSubstr)
				return
			}
			require.NoError(t, err)
			assertTokenizeResult(t, res, tt.wantIndexed, tt.wantQuery)
		})
	}
}

func TestPropertyTokenize(t *testing.T) {
	tests := []struct {
		name         string
		class        *models.Class
		propertyName string
		text         string
		wantErr      bool
		wantErrKind  TokenizeErrorKind
		errSubstr    string
		wantIndexed  []string
		wantQuery    []string
	}{
		{
			name:         "nil class is NotFound",
			class:        nil,
			propertyName: "title",
			text:         "hello",
			wantErr:      true,
			wantErrKind:  TokenizeErrNotFound,
			errSubstr:    "class not found",
		},
		{
			name: "property missing on class is NotFound",
			class: &models.Class{
				Class: "MyClass",
				Properties: []*models.Property{
					{Name: "title", Tokenization: models.PropertyTokenizationWord},
				},
			},
			propertyName: "missing",
			text:         "hello",
			wantErr:      true,
			wantErrKind:  TokenizeErrNotFound,
			errSubstr:    "not found",
		},
		{
			name: "property with tokenization disabled is Invalid",
			class: &models.Class{
				Class: "MyClass",
				Properties: []*models.Property{
					{Name: "title", Tokenization: ""},
				},
			},
			propertyName: "title",
			text:         "hello",
			wantErr:      true,
			wantErrKind:  TokenizeErrInvalid,
			errSubstr:    "tokenization is not enabled",
		},
		{
			name: "happy path: word tokenization with no inverted config",
			class: &models.Class{
				Class: "MyClass",
				Properties: []*models.Property{
					{Name: "title", Tokenization: models.PropertyTokenizationWord},
				},
			},
			propertyName: "title",
			text:         "The quick brown fox",
			wantIndexed:  []string{"the", "quick", "brown", "fox"},
		},
		{
			name: "property name match is case-insensitive",
			class: &models.Class{
				Class: "MyClass",
				Properties: []*models.Property{
					{Name: "Title", Tokenization: models.PropertyTokenizationWord},
				},
			},
			propertyName: "title",
			text:         "hello",
			wantIndexed:  []string{"hello"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PropertyTokenize(tt.class, tt.propertyName, tt.text)
			if tt.wantErr {
				assertTokenizeErr(t, err, tt.wantErrKind, tt.errSubstr)
				return
			}
			require.NoError(t, err)
			assertTokenizeResult(t, res, tt.wantIndexed, tt.wantQuery)
		})
	}
}
