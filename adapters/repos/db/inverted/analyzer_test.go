//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzer(t *testing.T) {
	a := NewAnalyzer(fakeStopwordDetector{})

	t.Run("with text", func(t *testing.T) {
		t.Run("only unique words", func(t *testing.T) {
			res := a.Text("Hello, my name is John Doe")
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

		t.Run("with repeated words", func(t *testing.T) {
			res := a.Text("Du. Du hast. Du hast. Du hast mich gefragt.")
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
	})

	t.Run("with string", func(t *testing.T) {
		res := a.String("My email is john-thats-jay.ohh.age.n+alloneword@doe.com")
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

func TestAnalyzer_ConfigurableStopwords(t *testing.T) {
	type testcase struct {
		cfg               schema.StopwordConfig
		input             string
		expectedCountable int
	}

	runTest := func(t *testing.T, tests []testcase) {
		for _, test := range tests {
			a := newTestAnalyzer(t, test.cfg)

			textCountable := a.Text(test.input)
			assert.Equal(t, test.expectedCountable, len(textCountable))

			stringCountable := a.String(test.input)
			assert.Equal(t, test.expectedCountable, len(stringCountable))
		}
	}

	t.Run("with en preset, additions", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: schema.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             "dog dog dog dog",
				expectedCountable: 0,
			},
			{
				cfg: schema.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             "dog dog dog cat",
				expectedCountable: 1,
			},
			{
				cfg: schema.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
				},
				input:             "a dog is the best",
				expectedCountable: 1,
			},
		}

		runTest(t, tests)
	})

	t.Run("with no preset, additions", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: schema.StopwordConfig{
					Preset:    "none",
					Additions: []string{"dog"},
				},
				input:             "a dog is the best",
				expectedCountable: 4,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: schema.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a"},
				},
				input:             "a dog is the best",
				expectedCountable: 3,
			},
			{
				cfg: schema.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a", "is", "the"},
				},
				input:             "a dog is the best",
				expectedCountable: 5,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: schema.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a"},
				},
				input:             "a dog is the best",
				expectedCountable: 3,
			},
			{
				cfg: schema.StopwordConfig{
					Preset:   "en",
					Removals: []string{"a", "is", "the"},
				},
				input:             "a dog is the best",
				expectedCountable: 5,
			},
		}

		runTest(t, tests)
	})

	t.Run("with en preset, additions, removals", func(t *testing.T) {
		tests := []testcase{
			{
				cfg: schema.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog"},
					Removals:  []string{"a"},
				},
				input:             "a dog is the best",
				expectedCountable: 2,
			},
			{
				cfg: schema.StopwordConfig{
					Preset:    "en",
					Additions: []string{"dog", "best"},
					Removals:  []string{"a", "the", "is"},
				},
				input:             "a dog is the best",
				expectedCountable: 3,
			},
		}

		runTest(t, tests)
	})
}

type fakeStopwordDetector struct{}

func (fsd fakeStopwordDetector) IsStopword(word string) bool {
	return false
}

func newTestAnalyzer(t *testing.T, cfg schema.StopwordConfig) *Analyzer {
	sd, err := stopwords.NewDetectorFromConfig(cfg)
	require.Nil(t, err)

	return NewAnalyzer(sd)
}
