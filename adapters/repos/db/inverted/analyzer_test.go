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

package inverted

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestAnalyzer(t *testing.T) {
	a := NewAnalyzer(nil)

	countable := func(data []string, freq []int) []Countable {
		countable := make([]Countable, len(data))
		for i := range data {
			countable[i] = Countable{
				Data:          []byte(data[i]),
				TermFrequency: float32(freq[i]),
			}
		}
		return countable
	}

	t.Run("with text", func(t *testing.T) {
		type testCase struct {
			name              string
			input             string
			tokenization      string
			expectedCountable []Countable
		}

		testCases := []testCase{
			{
				name:         "tokenization word, unique words",
				input:        "Hello, my name is John Doe",
				tokenization: models.PropertyTokenizationWord,
				expectedCountable: countable(
					[]string{"hello", "my", "name", "is", "john", "doe"},
					[]int{1, 1, 1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization word, duplicated words",
				input:        "Du. Du hast. Du hast. Du hast mich gefragt.",
				tokenization: models.PropertyTokenizationWord,
				expectedCountable: countable(
					[]string{"du", "hast", "mich", "gefragt"},
					[]int{4, 3, 1, 1},
				),
			},
			{
				name:         "tokenization lowercase, unique words",
				input:        "My email is john-thats-jay.ohh.age.n+alloneword@doe.com",
				tokenization: models.PropertyTokenizationLowercase,
				expectedCountable: countable(
					[]string{"my", "email", "is", "john-thats-jay.ohh.age.n+alloneword@doe.com"},
					[]int{1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization lowercase, duplicated words",
				input:        "Du. Du hast. Du hast. Du hast mich gefragt.",
				tokenization: models.PropertyTokenizationLowercase,
				expectedCountable: countable(
					[]string{"du.", "du", "hast.", "hast", "mich", "gefragt."},
					[]int{1, 3, 2, 1, 1, 1},
				),
			},
			{
				name:         "tokenization whitespace, unique words",
				input:        "My email is john-thats-jay.ohh.age.n+alloneword@doe.com",
				tokenization: models.PropertyTokenizationWhitespace,
				expectedCountable: countable(
					[]string{"My", "email", "is", "john-thats-jay.ohh.age.n+alloneword@doe.com"},
					[]int{1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization whitespace, duplicated words",
				input:        "Du. Du hast. Du hast. Du hast mich gefragt.",
				tokenization: models.PropertyTokenizationWhitespace,
				expectedCountable: countable(
					[]string{"Du.", "Du", "hast.", "hast", "mich", "gefragt."},
					[]int{1, 3, 2, 1, 1, 1},
				),
			},
			{
				name:         "tokenization field",
				input:        "\n Du. Du hast. Du hast. Du hast mich gefragt.\t ",
				tokenization: models.PropertyTokenizationField,
				expectedCountable: countable(
					[]string{"Du. Du hast. Du hast. Du hast mich gefragt."},
					[]int{1},
				),
			},
			{
				name:              "non existing tokenization",
				input:             "Du. Du hast. Du hast. Du hast mich gefragt.",
				tokenization:      "non_existing",
				expectedCountable: []Countable{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				countable := a.Text(tc.tokenization, tc.input)
				assert.ElementsMatch(t, tc.expectedCountable, countable)
			})
		}
	})

	t.Run("with text array", func(t *testing.T) {
		type testCase struct {
			name              string
			input             []string
			tokenization      string
			expectedCountable []Countable
		}

		testCases := []testCase{
			{
				name:         "tokenization word, unique words",
				input:        []string{"Hello,", "my name is John Doe"},
				tokenization: models.PropertyTokenizationWord,
				expectedCountable: countable(
					[]string{"hello", "my", "name", "is", "john", "doe"},
					[]int{1, 1, 1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization word, duplicated words",
				input:        []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."},
				tokenization: models.PropertyTokenizationWord,
				expectedCountable: countable(
					[]string{"du", "hast", "mich", "gefragt"},
					[]int{4, 3, 1, 1},
				),
			},
			{
				name:         "tokenization lowercase, unique words",
				input:        []string{"My email", "is john-thats-jay.ohh.age.n+alloneword@doe.com"},
				tokenization: models.PropertyTokenizationLowercase,
				expectedCountable: countable(
					[]string{"my", "email", "is", "john-thats-jay.ohh.age.n+alloneword@doe.com"},
					[]int{1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization lowercase, duplicated words",
				input:        []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."},
				tokenization: models.PropertyTokenizationLowercase,
				expectedCountable: countable(
					[]string{"du.", "du", "hast.", "hast", "mich", "gefragt."},
					[]int{1, 3, 2, 1, 1, 1},
				),
			},
			{
				name:         "tokenization whitespace, unique words",
				input:        []string{"My email", "is john-thats-jay.ohh.age.n+alloneword@doe.com"},
				tokenization: models.PropertyTokenizationWhitespace,
				expectedCountable: countable(
					[]string{"My", "email", "is", "john-thats-jay.ohh.age.n+alloneword@doe.com"},
					[]int{1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization whitespace, duplicated words",
				input:        []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."},
				tokenization: models.PropertyTokenizationWhitespace,
				expectedCountable: countable(
					[]string{"Du.", "Du", "hast.", "hast", "mich", "gefragt."},
					[]int{1, 3, 2, 1, 1, 1},
				),
			},
			{
				name:         "tokenization field",
				input:        []string{"\n Du. Du hast. Du hast.", "Du hast mich gefragt.\t "},
				tokenization: models.PropertyTokenizationField,
				expectedCountable: countable(
					[]string{"Du. Du hast. Du hast.", "Du hast mich gefragt."},
					[]int{1, 1},
				),
			},
			{
				name:              "non existing tokenization",
				input:             []string{"Du. Du hast. Du hast.", "Du hast mich gefragt."},
				tokenization:      "non_existing",
				expectedCountable: []Countable{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				countable := a.TextArray(tc.tokenization, tc.input)
				assert.ElementsMatch(t, tc.expectedCountable, countable)
			})
		}
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

	byteTrue := []byte{0x1}
	byteFalse := []byte{0x0}

	t.Run("analyze bool", func(t *testing.T) {
		t.Run("true", func(t *testing.T) {
			countable, err := a.Bool(true)
			require.Nil(t, err)
			require.Len(t, countable, 1)

			c := countable[0]
			assert.Equal(t, byteTrue, c.Data)
			assert.Equal(t, float32(0), c.TermFrequency)
		})

		t.Run("false", func(t *testing.T) {
			countable, err := a.Bool(false)
			require.Nil(t, err)
			require.Len(t, countable, 1)

			c := countable[0]
			assert.Equal(t, byteFalse, c.Data)
			assert.Equal(t, float32(0), c.TermFrequency)
		})
	})

	t.Run("analyze bool array", func(t *testing.T) {
		type testCase struct {
			name     string
			values   []bool
			expected [][]byte
		}

		testCases := []testCase{
			{
				name:     "[true]",
				values:   []bool{true},
				expected: [][]byte{byteTrue},
			},
			{
				name:     "[false]",
				values:   []bool{false},
				expected: [][]byte{byteFalse},
			},
			{
				name:     "[true, true, true]",
				values:   []bool{true, true, true},
				expected: [][]byte{byteTrue, byteTrue, byteTrue},
			},
			{
				name:     "[false, false, false]",
				values:   []bool{false, false, false},
				expected: [][]byte{byteFalse, byteFalse, byteFalse},
			},
			{
				name:     "[false, true, false, true]",
				values:   []bool{false, true, false, true},
				expected: [][]byte{byteFalse, byteTrue, byteFalse, byteTrue},
			},
			{
				name:     "[]",
				values:   []bool{},
				expected: [][]byte{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				countable, err := a.BoolArray(tc.values)
				require.Nil(t, err)
				require.Len(t, countable, len(tc.expected))

				for i := range countable {
					assert.Equal(t, tc.expected[i], countable[i].Data)
					assert.Equal(t, float32(0), countable[i].TermFrequency)
				}
			})
		}
	})
}

func TestAnalyzer_DefaultEngPreset(t *testing.T) {
	countable := func(data []string, freq []int) []Countable {
		countable := make([]Countable, len(data))
		for i := range data {
			countable[i] = Countable{
				Data:          []byte(data[i]),
				TermFrequency: float32(freq[i]),
			}
		}
		return countable
	}

	a := NewAnalyzer(nil)
	input := "Hello you-beautiful_World"

	t.Run("with text", func(t *testing.T) {
		type testCase struct {
			name              string
			tokenization      string
			input             string
			expectedCountable []Countable
		}

		testCases := []testCase{
			{
				name:         "tokenization word",
				tokenization: models.PropertyTokenizationWord,
				input:        input,
				expectedCountable: countable(
					[]string{"hello", "you", "beautiful", "world"},
					[]int{1, 1, 1, 1},
				),
			},
			{
				name:         "tokenization lowercase",
				tokenization: models.PropertyTokenizationLowercase,
				input:        input,
				expectedCountable: countable(
					[]string{"hello", "you-beautiful_world"},
					[]int{1, 1},
				),
			},
			{
				name:         "tokenization whitespace",
				tokenization: models.PropertyTokenizationWhitespace,
				input:        input,
				expectedCountable: countable(
					[]string{"Hello", "you-beautiful_World"},
					[]int{1, 1},
				),
			},
			{
				name:         "tokenization field",
				tokenization: models.PropertyTokenizationField,
				input:        input,
				expectedCountable: countable(
					[]string{"Hello you-beautiful_World"},
					[]int{1},
				),
			},
			{
				name:              "non existing tokenization",
				tokenization:      "non_existing",
				input:             input,
				expectedCountable: []Countable{},
			},
		}

		for _, tc := range testCases {
			countable := a.Text(tc.tokenization, tc.input)
			assert.ElementsMatch(t, tc.expectedCountable, countable)
		}
	})

	t.Run("with text array", func(t *testing.T) {
		type testCase struct {
			name              string
			tokenization      string
			input             []string
			expectedCountable []Countable
		}

		testCases := []testCase{
			{
				name:         "tokenization word",
				tokenization: models.PropertyTokenizationWord,
				input:        []string{input, input},
				expectedCountable: countable(
					[]string{"hello", "you", "beautiful", "world"},
					[]int{2, 2, 2, 2},
				),
			},
			{
				name:         "tokenization lowercase",
				tokenization: models.PropertyTokenizationLowercase,
				input:        []string{input, input},
				expectedCountable: countable(
					[]string{"hello", "you-beautiful_world"},
					[]int{2, 2},
				),
			},
			{
				name:         "tokenization whitespace",
				tokenization: models.PropertyTokenizationWhitespace,
				input:        []string{input, input},
				expectedCountable: countable(
					[]string{"Hello", "you-beautiful_World"},
					[]int{2, 2},
				),
			},
			{
				name:         "tokenization field",
				tokenization: models.PropertyTokenizationField,
				input:        []string{input, input},
				expectedCountable: countable(
					[]string{"Hello you-beautiful_World"},
					[]int{2},
				),
			},
			{
				name:              "non existing tokenization",
				tokenization:      "non_existing",
				input:             []string{input, input},
				expectedCountable: []Countable{},
			},
		}

		for _, tc := range testCases {
			countable := a.TextArray(tc.tokenization, tc.input)
			assert.ElementsMatch(t, tc.expectedCountable, countable)
		}
	})
}

type fakeStopwordDetector struct{}

func (fsd fakeStopwordDetector) IsStopword(word string) bool {
	return false
}
