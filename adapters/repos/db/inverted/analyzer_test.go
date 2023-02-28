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

package inverted

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
)

func TestAnalyzer(t *testing.T) {
	a := NewAnalyzer(fakeStopwordDetector{})

	t.Run("with text", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.Text(models.PropertyTokenizationWord, "Hello, my name is John Doe")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("hello"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("my"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("name"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("is"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("john"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("doe"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("repeated words", func(t *testing.T) {
			res := a.Text(models.PropertyTokenizationWord, "Du. Du hast. Du hast. Du hast mich gefragt.")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("du"),
					TermFrequency: float32(4),
				},
				{
					Data:          []byte("hast"),
					TermFrequency: float32(3),
				},
				{
					Data:          []byte("mich"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("gefragt"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("not supported tokenization", func(t *testing.T) {
			res := a.Text("not-supported", "Du. Du hast. Du hast. Du hast mich gefragt.")
			assert.Empty(t, res)
		})
	})

	t.Run("with text array", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.TextArray(models.PropertyTokenizationWord, []string{"Hello,", "my name is John Doe"})
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("hello"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("my"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("name"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("is"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("john"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("doe"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("repeated words", func(t *testing.T) {
			res := a.TextArray(models.PropertyTokenizationWord, []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."})
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("du"),
					TermFrequency: float32(4),
				},
				{
					Data:          []byte("hast"),
					TermFrequency: float32(3),
				},
				{
					Data:          []byte("mich"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("gefragt"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("not supported tokenization", func(t *testing.T) {
			res := a.TextArray("not-supported", []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."})
			assert.Empty(t, res)
		})
	})

	t.Run("with string", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.String(models.PropertyTokenizationWord, "My email is john-thats-jay.ohh.age.n+alloneword@doe.com")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("john-thats-jay.ohh.age.n+alloneword@doe.com"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("My"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("email"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("is"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("repeated words", func(t *testing.T) {
			res := a.String(models.PropertyTokenizationWord, "Du. Du hast. Du hast. Du hast mich gefragt.")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("Du."),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("Du"),
					TermFrequency: float32(3),
				},
				{
					Data:          []byte("hast."),
					TermFrequency: float32(2),
				},
				{
					Data:          []byte("hast"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("mich"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("gefragt."),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("field tokenization", func(t *testing.T) {
			res := a.String(models.PropertyTokenizationField, "\n Du. Du hast. Du hast. Du hast mich gefragt.\t ")
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("Du. Du hast. Du hast. Du hast mich gefragt."),
					TermFrequency: float32(1),
				},
			})
		})
	})

	t.Run("with string array", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.StringArray(models.PropertyTokenizationWord, []string{"My email", "is john-thats-jay.ohh.age.n+alloneword@doe.com"})
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("john-thats-jay.ohh.age.n+alloneword@doe.com"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("My"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("email"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("is"),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("repeated words", func(t *testing.T) {
			res := a.StringArray(models.PropertyTokenizationWord, []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."})
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("Du."),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("Du"),
					TermFrequency: float32(3),
				},
				{
					Data:          []byte("hast."),
					TermFrequency: float32(2),
				},
				{
					Data:          []byte("hast"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("mich"),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("gefragt."),
					TermFrequency: float32(1),
				},
			})
		})

		t.Run("field tokenization", func(t *testing.T) {
			res := a.StringArray(models.PropertyTokenizationField, []string{"\n Du. Du hast. Du hast.", "Du hast mich gefragt.\t "})
			assert.ElementsMatch(t, res, []Countable{
				{
					Data:          []byte("Du. Du hast. Du hast."),
					TermFrequency: float32(1),
				},
				{
					Data:          []byte("Du hast mich gefragt."),
					TermFrequency: float32(1),
				},
			})
		})
	})

	t.Run("with int it stays sortable", func(t *testing.T) {
		getData := func(in []Countable, err error) []byte {
			require.Nil(t, err)
			return in[0].Data
		}

		results := [][]byte{
			getData(a.Float(math.MinInt64)),
			getData(a.Int(-1000000)),
			getData(a.Int(-400000)),
			getData(a.Int(-20000)),
			getData(a.Int(-9000)),
			getData(a.Int(-301)),
			getData(a.Int(-300)),
			getData(a.Int(-299)),
			getData(a.Int(-1)),
			getData(a.Int(0)),
			getData(a.Int(1)),
			getData(a.Int(299)),
			getData(a.Int(300)),
			getData(a.Int(301)),
			getData(a.Int(9000)),
			getData(a.Int(20000)),
			getData(a.Int(400000)),
			getData(a.Int(1000000)),
			getData(a.Float(math.MaxInt64)),
		}

		afterSort := make([][]byte, len(results))
		copy(afterSort, results)
		sort.Slice(afterSort, func(a, b int) bool { return bytes.Compare(afterSort[a], afterSort[b]) == -1 })
		assert.Equal(t, results, afterSort)
	})

	t.Run("with float it stays sortable", func(t *testing.T) {
		getData := func(in []Countable, err error) []byte {
			require.Nil(t, err)
			return in[0].Data
		}

		results := [][]byte{
			getData(a.Float(-math.MaxFloat64)),
			getData(a.Float(-1000000)),
			getData(a.Float(-400000)),
			getData(a.Float(-20000)),
			getData(a.Float(-9000.9)),
			getData(a.Float(-9000.8999)),
			getData(a.Float(-9000.8998)),
			getData(a.Float(-9000.79999)),
			getData(a.Float(-301)),
			getData(a.Float(-300)),
			getData(a.Float(-299)),
			getData(a.Float(-1)),
			getData(a.Float(-0.09)),
			getData(a.Float(-0.01)),
			getData(a.Float(-0.009)),
			getData(a.Float(0)),
			getData(a.Float(math.SmallestNonzeroFloat64)),
			getData(a.Float(0.009)),
			getData(a.Float(0.01)),
			getData(a.Float(0.09)),
			getData(a.Float(0.1)),
			getData(a.Float(0.9)),
			getData(a.Float(1)),
			getData(a.Float(299)),
			getData(a.Float(300)),
			getData(a.Float(301)),
			getData(a.Float(9000)),
			getData(a.Float(20000)),
			getData(a.Float(400000)),
			getData(a.Float(1000000)),
			getData(a.Float(math.MaxFloat64)),
		}

		afterSort := make([][]byte, len(results))
		copy(afterSort, results)
		sort.Slice(afterSort, func(a, b int) bool { return bytes.Compare(afterSort[a], afterSort[b]) == -1 })
		assert.Equal(t, results, afterSort)
	})

	t.Run("with refCount it stays sortable", func(t *testing.T) {
		getData := func(in []Countable, err error) []byte {
			require.Nil(t, err)
			return in[0].Data
		}

		results := [][]byte{
			getData(a.RefCount(make(models.MultipleRef, 0))),
			getData(a.RefCount(make(models.MultipleRef, 1))),
			getData(a.RefCount(make(models.MultipleRef, 2))),
			getData(a.RefCount(make(models.MultipleRef, 99))),
			getData(a.RefCount(make(models.MultipleRef, 100))),
			getData(a.RefCount(make(models.MultipleRef, 101))),
			getData(a.RefCount(make(models.MultipleRef, 256))),
			getData(a.RefCount(make(models.MultipleRef, 300))),
			getData(a.RefCount(make(models.MultipleRef, 456))),
		}

		afterSort := make([][]byte, len(results))
		copy(afterSort, results)
		sort.Slice(afterSort, func(a, b int) bool { return bytes.Compare(afterSort[a], afterSort[b]) == -1 })
		assert.Equal(t, results, afterSort)
	})
}

func TestAnalyzer_DefaultEngPreset(t *testing.T) {
	a := newTestAnalyzer(t, models.StopwordConfig{Preset: "en"})
	input := "hello you-beautiful_world"

	textCountableWord := a.Text(models.PropertyTokenizationWord, input)
	assert.ElementsMatch(t, textCountableWord, []Countable{
		{Data: []byte("hello"), TermFrequency: float32(1)},
		{Data: []byte("you"), TermFrequency: float32(1)},
		{Data: []byte("beautiful"), TermFrequency: float32(1)},
		{Data: []byte("world"), TermFrequency: float32(1)},
	})

	textArrayCountableWord := a.TextArray(models.PropertyTokenizationWord, []string{input, input})
	assert.ElementsMatch(t, textArrayCountableWord, []Countable{
		{Data: []byte("hello"), TermFrequency: float32(2)},
		{Data: []byte("you"), TermFrequency: float32(2)},
		{Data: []byte("beautiful"), TermFrequency: float32(2)},
		{Data: []byte("world"), TermFrequency: float32(2)},
	})

	stringCountableWord := a.String(models.PropertyTokenizationWord, input)
	assert.ElementsMatch(t, stringCountableWord, []Countable{
		{Data: []byte("hello"), TermFrequency: float32(1)},
		{Data: []byte("you-beautiful_world"), TermFrequency: float32(1)},
	})

	stringArrayCountableWord := a.StringArray(models.PropertyTokenizationWord, []string{input, input})
	assert.ElementsMatch(t, stringArrayCountableWord, []Countable{
		{Data: []byte("hello"), TermFrequency: float32(2)},
		{Data: []byte("you-beautiful_world"), TermFrequency: float32(2)},
	})

	stringCountableField := a.String(models.PropertyTokenizationField, input)
	assert.ElementsMatch(t, stringCountableField, []Countable{
		{Data: []byte("hello you-beautiful_world"), TermFrequency: float32(1)},
	})

	stringArrayCountableField := a.StringArray(models.PropertyTokenizationField, []string{input, input})
	assert.ElementsMatch(t, stringArrayCountableField, []Countable{
		{Data: []byte("hello you-beautiful_world"), TermFrequency: float32(2)},
	})
}

type fakeStopwordDetector struct{}

func (fsd fakeStopwordDetector) IsStopword(word string) bool {
	return false
}

func newTestAnalyzer(t *testing.T, cfg models.StopwordConfig) *Analyzer {
	sd, err := stopwords.NewDetectorFromConfig(cfg)
	require.Nil(t, err)

	return NewAnalyzer(sd)
}
