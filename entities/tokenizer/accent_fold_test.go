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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFoldAccents(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "French accents",
			input:    "école",
			expected: "ecole",
		},
		{
			name:     "naïve with diaeresis",
			input:    "naïve",
			expected: "naive",
		},
		{
			name:     "Portuguese São Paulo",
			input:    "São Paulo",
			expected: "Sao Paulo",
		},
		{
			name:     "German umlauts",
			input:    "Ärger über Öl",
			expected: "Arger uber Ol",
		},
		{
			name:     "Spanish ñ",
			input:    "señor",
			expected: "senor",
		},
		{
			name:     "mixed accents and plain ASCII",
			input:    "café résumé hello world",
			expected: "cafe resume hello world",
		},
		{
			name:     "no accents passthrough",
			input:    "hello world 123",
			expected: "hello world 123",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Vietnamese diacritics",
			input:    "Hà Nội",
			expected: "Ha Noi",
		},
		{
			name:     "Czech háčky and čárky",
			input:    "příliš žluťoučký kůň",
			expected: "prilis zlutoucky kun",
		},
		// Stroked / barred letters
		{
			name:     "Polish ł and Ł",
			input:    "Łódź złoty",
			expected: "Lodz zloty",
		},
		{
			name:     "Danish/Norwegian ø and Ø",
			input:    "Ørsted rødgrød",
			expected: "Orsted rodgrod",
		},
		{
			name:     "Croatian đ",
			input:    "Đurđevac",
			expected: "Durdevac",
		},
		{
			name:     "Maltese ħ",
			input:    "Ħal Balzan",
			expected: "Hal Balzan",
		},
		{
			name:     "Sami ŧ",
			input:    "ŧávvet",
			expected: "tavvet",
		},
		{
			name:     "stroked b, e, j, r, y, a",
			input:    "ƀɇɉɍɏⱥ",
			expected: "bejrya",
		},
		// Special letters
		{
			name:     "Icelandic eth ð",
			input:    "Norðurland",
			expected: "Nordurland",
		},
		{
			name:     "Turkish dotless i",
			input:    "dışarı",
			expected: "disari",
		},
		{
			name:     "long s",
			input:    "ſincerely",
			expected: "sincerely",
		},
		// Ligatures
		{
			name:     "ae ligature",
			input:    "Æneas æsthetic",
			expected: "AEneas aesthetic",
		},
		{
			name:     "oe ligature",
			input:    "Œuvre cœur",
			expected: "OEuvre coeur",
		},
		{
			name:     "Dutch ij ligature",
			input:    "ĳsland Ĳsselmeer",
			expected: "ijsland IJsselmeer",
		},
		{
			name:     "German sharp s",
			input:    "Straße groß",
			expected: "Strasse gross",
		},
		{
			name:     "capital sharp s",
			input:    "STRAẞE",
			expected: "STRASSE",
		},
		{
			name:     "Icelandic thorn",
			input:    "Þór þakka",
			expected: "THor thakka",
		},
		{
			name:     "typographic ligatures ff fi fl ffi ffl st",
			input:    "ﬀ ﬁ ﬂ ﬃ ﬄ ﬅ ﬆ",
			expected: "ff fi fl ffi ffl st st",
		},
		// Hooked / tailed letters
		{
			name:     "hooked letters",
			input:    "ɓƈɗƒɠɦƙɱɲƥʠɽʂƭʋⱳƴʐ",
			expected: "bcdfghkmnpqrstvwyz",
		},
		// Combined: NFD marks + table folding in same string
		{
			name:     "mixed NFD and table folding",
			input:    "Ångström straße",
			expected: "Angstrom strasse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FoldAccents(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFoldAccentsSlice(t *testing.T) {
	input := []string{"école", "café", "hello"}
	result := FoldAccentsSlice(input)
	require.Len(t, result, 3)
	assert.Equal(t, "ecole", result[0])
	assert.Equal(t, "cafe", result[1])
	assert.Equal(t, "hello", result[2])
}

func TestFoldAccentsIdempotent(t *testing.T) {
	input := "ecole"
	result := FoldAccents(input)
	assert.Equal(t, input, result, "folding already-ASCII text should be a no-op")
}

func TestFoldAccentsWithWordTokenization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "French school phrase",
			input:    "L'école est fermée",
			expected: []string{"l", "ecole", "est", "fermee"},
		},
		{
			name:     "Portuguese text",
			input:    "Ação e reação",
			expected: []string{"acao", "e", "reacao"},
		},
		{
			name:     "accent folding merges equivalent tokens",
			input:    "café cafe",
			expected: []string{"cafe", "cafe"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := Tokenize("word", tt.input)
			folded := FoldAccentsSlice(tokens)
			assert.Equal(t, tt.expected, folded)
		})
	}
}
