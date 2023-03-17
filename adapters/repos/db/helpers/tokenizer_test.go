//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenize(t *testing.T) {
	input := " Hello You*-beautiful_world?!"

	t.Run("tokenize field", func(t *testing.T) {
		output := tokenizeField(input)

		assert.ElementsMatch(t, output, []string{"Hello You*-beautiful_world?!"})
	})

	t.Run("tokenize whitespace", func(t *testing.T) {
		output := tokenizeWhitespace(input)

		assert.ElementsMatch(t, output, []string{"Hello", "You*-beautiful_world?!"})
	})

	t.Run("tokenize lowercase", func(t *testing.T) {
		output := tokenizeLowercase(input)

		assert.ElementsMatch(t, output, []string{"hello", "you*-beautiful_world?!"})
	})

	t.Run("tokenize word", func(t *testing.T) {
		output := tokenizeWord(input)

		assert.ElementsMatch(t, output, []string{"hello", "you", "beautiful", "world"})
	})

	t.Run("tokenize word with wildcards", func(t *testing.T) {
		output := tokenizeWordWithWildcards(input)

		assert.ElementsMatch(t, output, []string{"hello", "you*", "beautiful", "world?"})
	})
}
