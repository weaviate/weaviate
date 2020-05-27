package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzer(t *testing.T) {
	a := NewAnalyzer()

	t.Run("with text", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.Text("Hello, my name is John Doe")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("hello"),
					TermFrequency: float32(1) / 6,
				},
				{
					Data:          []byte("my"),
					TermFrequency: float32(1) / 6,
				},
				{
					Data:          []byte("name"),
					TermFrequency: float32(1) / 6,
				},
				{
					Data:          []byte("is"),
					TermFrequency: float32(1) / 6,
				},
				{
					Data:          []byte("john"),
					TermFrequency: float32(1) / 6,
				},
				{
					Data:          []byte("doe"),
					TermFrequency: float32(1) / 6,
				},
			})
		})

		t.Run("with repeated words", func(t *testing.T) {
			res := a.Text("Du. Du hast. Du hast. Du hast mich gefragt.")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("du"),
					TermFrequency: float32(4) / 9,
				},
				{
					Data:          []byte("hast"),
					TermFrequency: float32(3) / 9,
				},
				{
					Data:          []byte("mich"),
					TermFrequency: float32(1) / 9,
				},
				{
					Data:          []byte("gefragt"),
					TermFrequency: float32(1) / 9,
				},
			})
		})

	})

	t.Run("with string", func(t *testing.T) {
		res := a.String("My email is john-thats-jay.ohh.age.n+alloneword@doe.com")
		assert.ElementsMatch(t, res, []Countable{
			{
				Data:          []byte("john-thats-jay.ohh.age.n+alloneword@doe.com"),
				TermFrequency: float32(1) / 4,
			},
			{
				Data:          []byte("My"),
				TermFrequency: float32(1) / 4,
			},
			{
				Data:          []byte("email"),
				TermFrequency: float32(1) / 4,
			},
			{
				Data:          []byte("is"),
				TermFrequency: float32(1) / 4,
			},
		})
	})
}
