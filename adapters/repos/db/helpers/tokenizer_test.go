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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestTokenize(t *testing.T) {
	input := " Hello You*-beautiful_world?!"

	type testCase struct {
		tokenization string
		expected     []string
	}

	t.Run("tokenize", func(t *testing.T) {
		testCases := []testCase{
			{
				tokenization: models.PropertyTokenizationField,
				expected:     []string{"Hello You*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationWhitespace,
				expected:     []string{"Hello", "You*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationLowercase,
				expected:     []string{"hello", "you*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationWord,
				expected:     []string{"hello", "you", "beautiful", "world"},
			},
		}

		for _, tc := range testCases {
			terms := Tokenize(tc.tokenization, input)
			assert.ElementsMatch(t, tc.expected, terms)
		}
	})

	t.Run("tokenize with wildcards", func(t *testing.T) {
		testCases := []testCase{
			{
				tokenization: models.PropertyTokenizationField,
				expected:     []string{"Hello You*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationWhitespace,
				expected:     []string{"Hello", "You*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationLowercase,
				expected:     []string{"hello", "you*-beautiful_world?!"},
			},
			{
				tokenization: models.PropertyTokenizationWord,
				expected:     []string{"hello", "you*", "beautiful", "world?"},
			},
		}

		for _, tc := range testCases {
			terms := TokenizeWithWildcards(tc.tokenization, input)
			assert.ElementsMatch(t, tc.expected, terms)
		}
	})
}

func TestTokenizeAndCountDuplicates(t *testing.T) {
	input := "Hello You Beautiful World! hello you beautiful world!"

	type testCase struct {
		tokenization string
		expected     map[string]int
	}

	testCases := []testCase{
		{
			tokenization: models.PropertyTokenizationField,
			expected: map[string]int{
				"Hello You Beautiful World! hello you beautiful world!": 1,
			},
		},
		{
			tokenization: models.PropertyTokenizationWhitespace,
			expected: map[string]int{
				"Hello":     1,
				"You":       1,
				"Beautiful": 1,
				"World!":    1,
				"hello":     1,
				"you":       1,
				"beautiful": 1,
				"world!":    1,
			},
		},
		{
			tokenization: models.PropertyTokenizationLowercase,
			expected: map[string]int{
				"hello":     2,
				"you":       2,
				"beautiful": 2,
				"world!":    2,
			},
		},
		{
			tokenization: models.PropertyTokenizationWord,
			expected: map[string]int{
				"hello":     2,
				"you":       2,
				"beautiful": 2,
				"world":     2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tokenization, func(t *testing.T) {
			terms, dups := TokenizeAndCountDuplicates(tc.tokenization, input)

			assert.Len(t, terms, len(tc.expected))
			assert.Len(t, dups, len(tc.expected))

			for i := range terms {
				assert.Contains(t, tc.expected, terms[i])
				assert.Equal(t, tc.expected[terms[i]], dups[i])
			}
		})
	}
}
