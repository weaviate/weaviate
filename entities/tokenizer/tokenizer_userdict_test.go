//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func generateReplacementModel() *models.TokenizerUserDictConfig {
	ptr := func(s string) *string { return &s }
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
}

func TestKagomeUserTokenizerForClassValidate(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	InitOptionalTokenizers()
	ptr := func(s string) *string { return &s }
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
