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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func ptr(s string) *string { return &s }

func generateReplacementModel() *models.TokenizerUserDictConfig {
	return &models.TokenizerUserDictConfig{
		Tokenizer: models.PropertyTokenizationKagomeKr,
		Replacements: []*models.TokenizerUserDictConfigReplacementsItems0{
			{
				Source: ptr("Weaviate"),
				Target: ptr("We Aviate"),
			},
			{
				Source: ptr("Semi Technologies"),
				Target: ptr("SemiTechnologies"),
			},
			{
				Source: ptr("Aviate"),
				Target: ptr("Aviate"),
			},
			{
				Source: ptr("We"),
				Target: ptr("We"),
			},
		},
	}
}

func TestKagomeUserTokenizerForClass(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	InitOptionalTokenizers()

	className := "TestClass"
	err := AddCustomDict(className, []*models.TokenizerUserDictConfig{generateReplacementModel()})
	assert.Nil(t, err)

	tokens := TokenizeForClass(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies", className)
	assert.Equal(t, []string{"We", "Aviate", "SemiTechnologies"}, tokens)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "We Aviate", className)
	assert.Equal(t, []string{"We", "Aviate"}, tokens)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies", "")
	assert.Equal(t, []string{"Weaviat", "e", "Sem", "i", "Technologie", "s"}, tokens)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "We", "")
	assert.Equal(t, []string{"W", "e"}, tokens)

	// Test removing the custom dictionary
	err = AddCustomDict(className, nil)
	assert.Nil(t, err)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies", className)
	assert.Equal(t, []string{"Weaviat", "e", "Sem", "i", "Technologie", "s"}, tokens)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "We", className)
	assert.Equal(t, []string{"W", "e"}, tokens)
}

// TestKagomeUserTokenizerForClassBalancesThrottle pins the throttle
// acquire/release pairing of the kagome custom-dictionary branches in
// TokenizeForClass, including the fallthrough to the global tokenizer when
// the class's dict has no tokenizer for the requested language. Each call is
// made from a guarded goroutine so an unbalanced throttle surfaces as a test
// failure instead of a suite timeout.
func TestKagomeUserTokenizerForClassBalancesThrottle(t *testing.T) {
	jaDict := &models.TokenizerUserDictConfig{
		Tokenizer: models.PropertyTokenizationKagomeJa,
		Replacements: []*models.TokenizerUserDictConfigReplacementsItems0{
			{
				Source: ptr("Weaviate"),
				Target: ptr("We Aviate"),
			},
		},
	}

	tests := []struct {
		name         string
		tokenization string
		dict         *models.TokenizerUserDictConfig
		// nil means: expect the output of the global Tokenize for the same
		// input (the custom-dict fallthrough)
		want []string
	}{
		{
			name:         "KagomeJa with custom JA dict",
			tokenization: models.PropertyTokenizationKagomeJa,
			dict:         jaDict,
			want:         []string{"We", "Aviate", "Semi", "Technologies"},
		},
		{
			name:         "KagomeKr with custom KR dict",
			tokenization: models.PropertyTokenizationKagomeKr,
			dict:         generateReplacementModel(),
			want:         []string{"We", "Aviate", "SemiTechnologies"},
		},
		{
			name:         "KagomeJa against KR-only dict falls through to global tokenizer",
			tokenization: models.PropertyTokenizationKagomeJa,
			dict:         generateReplacementModel(),
			want:         nil,
		},
		{
			name:         "KagomeKr against JA-only dict falls through to global tokenizer",
			tokenization: models.PropertyTokenizationKagomeKr,
			dict:         jaDict,
			want:         nil,
		},
	}

	const input = "Weaviate Semi Technologies"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			className := "TestClassThrottle"
			require.NoError(t, AddCustomDict(className, []*models.TokenizerUserDictConfig{tt.dict}))
			defer func() {
				require.NoError(t, AddCustomDict(className, nil))
			}()

			want := tt.want
			if want == nil {
				want = Tokenize(tt.tokenization, input)
			}

			require.Zero(t, len(ApacTokenizerThrottle), "throttle must be empty before the call")

			done := make(chan []string, 1)
			go func() {
				done <- TokenizeForClass(tt.tokenization, input, className)
			}()

			select {
			case tokens := <-done:
				assert.Equal(t, want, tokens)
			case <-time.After(30 * time.Second):
				t.Fatal("TokenizeForClass hung: throttle released without a matching acquire")
			}

			assert.Zero(t, len(ApacTokenizerThrottle), "throttle must be balanced (empty) after the call")
		})
	}
}

func TestKagomeUserTokenizerForClassValidate(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	InitOptionalTokenizers()
	className := "TestClass"
	format := generateReplacementModel()
	format.Replacements[2].Source = nil // invalid
	err := AddCustomDict(className, []*models.TokenizerUserDictConfig{format})
	assert.Error(t, err)

	format.Replacements[2].Source = ptr("Weaviate") // duplicate source
	err = AddCustomDict(className, []*models.TokenizerUserDictConfig{format})
	assert.Error(t, err)

	format.Replacements[2].Source = ptr("Aviate")
	format.Replacements[2].Target = ptr("") // empty target
	err = AddCustomDict(className, []*models.TokenizerUserDictConfig{format})
	// target can be empty, it will work as deletion
	assert.Nil(t, err)

	err = AddCustomDict(className, []*models.TokenizerUserDictConfig{format, format}) // duplicate tokenizer config
	assert.Error(t, err)
}
