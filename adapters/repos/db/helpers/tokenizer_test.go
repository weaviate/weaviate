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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)


func TestTokenise(t *testing.T) {
	tokens := Tokenize(models.PropertyTokenizationTrigram, "Thequickbrownfoxjumpsoverthelazydog")
	assert.Equal(t, []string{"the", "heq", "equ", "qui", "uic", "ick", "ckb", "kbr", "bro", "row", "own", "wnf", "nfo", "fox", "oxj", "xju", "jum", "ump", "mps", "pso", "sov", "ove", "ver", "ert", "rth", "the", "hel", "ela", "laz", "azy", "zyd", "ydo", "dog"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationTrigram, "The quick brown fox jumps over the lazy dog")
	assert.Equal(t, []string{"the", "heq", "equ", "qui", "uic", "ick", "ckb", "kbr", "bro", "row", "own", "wnf", "nfo", "fox", "oxj", "xju", "jum", "ump", "mps", "pso", "sov", "ove", "ver", "ert", "rth", "the", "hel", "ela", "laz", "azy", "zyd", "ydo", "dog"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationTrigram, "いろはにほへとちりぬるをわかよたれそつねならむうゐのおくやまけふこえてあさきゆめみしゑひもせす")
	assert.Equal(t, []string{"いろは", "ろはに", "はにほ", "にほへ", "ほへと", "へとち", "とちり", "ちりぬ", "りぬる", "ぬるを", "るをわ", "をわか", "わかよ", "かよた", "よたれ", "たれそ", "れそつ", "そつね", "つねな", "ねなら", "ならむ", "らむう", "むうゐ", "うゐの", "ゐのお", "のおく", "おくや", "くやま", "やまけ", "まけふ", "けふこ", "ふこえ", "こえて", "えてあ", "てあさ", "あさき", "さきゆ", "きゆめ", "ゆめみ", "めみし", "みしゑ", "しゑひ", "ゑひも", "ひもせ", "もせす"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGSE, "素早い茶色の狐が怠けた犬を飛び越えた")
	assert.Equal(t,strings.Split("素早い 茶色 の 狐 が 怠 けた 犬 を 飛び越え た", " "), tokens)

	tokens = Tokenize(models.PropertyTokenizationGSE, "すばやいちゃいろのきつねがなまけたいぬをとびこえた")
	assert.Equal(t,strings.Split("すばやい ちゃいろ のき つね がな ま けた いぬ を とびこえ た", " "), tokens)

	tokens = Tokenize(models.PropertyTokenizationGSE, "スバヤイチャイロノキツネガナマケタイヌヲトビコエタ")
	assert.Equal(t,strings.Split("スバ ヤイ チャイロ ノ キツ ネガ ナマ ケタ イヌ ヲ トビ コ エ タ", " "), tokens)

	tokens = Tokenize(models.PropertyTokenizationGSE, "The quick brown fox jumps over the lazy dog")
	assert.Equal(t,strings.Split("the quick brown fox jumps over the lazy dog", " "), tokens)
}




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
