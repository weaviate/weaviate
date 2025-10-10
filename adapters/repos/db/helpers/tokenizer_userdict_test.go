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

package helpers

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func createVirtualUserDict() string {
	file, err := os.CreateTemp("", "user_dict_*.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteString("Weaviate,We Aviate,We Aviate,NNP\nSemi Technologies,SemiTechnologies,SemiTechnologies,NNP\n")
	if err != nil {
		panic(err)
	}

	return file.Name()
}

func TestKagomeUserTokenizer(t *testing.T) {
	fileName := createVirtualUserDict()
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	t.Setenv("TOKENIZER_KAGOME_KR_USER_DICT_PATH", fileName)
	defer os.Remove(fileName)

	_ = initializeKagomeTokenizerKr()

	tokens := Tokenize(models.PropertyTokenizationKagomeKr, "Weaviate Semi Technologies")
	assert.Equal(t, []string{"We", "Aviate", "SemiTechnologies"}, tokens)
}
