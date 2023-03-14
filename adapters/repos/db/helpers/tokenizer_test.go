package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenize(t *testing.T) {
	input := " Hello You*-beautiful_world?!"

	t.Run("tokenize field", func(t *testing.T) {
		output := TokenizeField(input)

		assert.ElementsMatch(t, output, []string{"Hello You*-beautiful_world?!"})
	})

	t.Run("tokenize whitespace", func(t *testing.T) {
		output := TokenizeWhitespace(input)

		assert.ElementsMatch(t, output, []string{"Hello", "You*-beautiful_world?!"})
	})

	t.Run("tokenize lowercase", func(t *testing.T) {
		output := TokenizeLowercase(input)

		assert.ElementsMatch(t, output, []string{"hello", "you*-beautiful_world?!"})
	})

	t.Run("tokenize word", func(t *testing.T) {
		output := TokenizeWord(input)

		assert.ElementsMatch(t, output, []string{"hello", "you", "beautiful", "world"})
	})

	t.Run("tokenize word with wildcards", func(t *testing.T) {
		output := TokenizeWordWithWildcards(input)

		assert.ElementsMatch(t, output, []string{"hello", "you*", "beautiful", "world?"})
	})
}
