//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
		},
	}
}

func TestKagomeUserTokenizerForClass(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	if tokenizers.Korean == nil {
		tokenizers.Korean, _ = initializeKagomeTokenizerKr(nil)
	}

	customTokenizers, err := InitUserDictTokenizers([]*models.TokenizerUserDictConfig{generateReplacementModel()})
	className := "SomeClass"
	assert.Nil(t, err)
	CustomTokenizersInitLock.Lock()
	CustomTokenizers[className] = customTokenizers
	CustomTokenizersInitLock.Unlock()

	tokens := TokenizeForClass(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies", className)
	assert.Equal(t, []string{"We", "Aviate", "SemiTechnologies"}, tokens)

	tokens = TokenizeForClass(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies", "")
	assert.Equal(t, []string{"Weaviat", "e", "Sem", "i", "Technologie", "s"}, tokens)
}
