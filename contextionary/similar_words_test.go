package contextionary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimilarWords(t *testing.T) {

	t.Run("with a word that's not in the c11y", func(t *testing.T) {
		c := newC11y()
		expectedWords := []string{"vehicle"}

		words := c.SafeGetSimilarWordsWithCertainty("vehicle", 0.8)

		assert.Equal(t, expectedWords, words)
	})

	t.Run("with a word thats present and a high certainty", func(t *testing.T) {
		c := newC11y()
		expectedWords := []string{"car", "automobile"}

		words := c.SafeGetSimilarWordsWithCertainty("car", 0.95)

		assert.Equal(t, expectedWords, words)
	})

	t.Run("with a word thats present and a medium certainty", func(t *testing.T) {
		c := newC11y()
		expectedWords := []string{"car", "automobile", "airplane"}

		words := c.SafeGetSimilarWordsWithCertainty("car", 0.7)

		assert.Equal(t, expectedWords, words)
	})

	t.Run("with a word thats present and a really low certainty", func(t *testing.T) {
		c := newC11y()
		expectedWords := []string{"car", "automobile", "airplane", "cabernetsauvignon"}

		words := c.SafeGetSimilarWordsWithCertainty("car", 0.001)

		assert.Equal(t, expectedWords, words)
	})

}

func newC11y() Contextionary {
	builder := InMemoryBuilder(3)

	builder.AddWord("car", NewVector([]float32{1, 0, 0}))
	builder.AddWord("automobile", NewVector([]float32{0.9, 0, 0}))
	builder.AddWord("airplane", NewVector([]float32{0.3, 0, 0}))
	builder.AddWord("cabernetsauvignon", NewVector([]float32{0, 0, 10}))

	return Contextionary(builder.Build(3))
}
