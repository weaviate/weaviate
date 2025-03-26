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

func TestTokeniseParallel(t *testing.T) {
	UseGse = true
	init_gse()
	UseGseCh = true
	init_gse_ch()
	// Kagome tokenizer for Korean
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	_ = initializeKagomeTokenizerKr()

	// Kagome tokenizer for Japanese
	t.Setenv("ENABLE_TOKENIZER_KAGOME_JA", "true")
	_ = initializeKagomeTokenizerJa()
	for i := 0; i < 1000; i++ {
		go SingleTokenise(t)
	}
}

func SingleTokenise(t *testing.T) {
	tokens := Tokenize(models.PropertyTokenizationTrigram, "Thequickbrownfoxjumpsoverthelazydog")
	assert.Equal(t, []string{"the", "heq", "equ", "qui", "uic", "ick", "ckb", "kbr", "bro", "row", "own", "wnf", "nfo", "fox", "oxj", "xju", "jum", "ump", "mps", "pso", "sov", "ove", "ver", "ert", "rth", "the", "hel", "ela", "laz", "azy", "zyd", "ydo", "dog"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationTrigram, "The quick brown fox jumps over the lazy dog")
	assert.Equal(t, []string{"the", "heq", "equ", "qui", "uic", "ick", "ckb", "kbr", "bro", "row", "own", "wnf", "nfo", "fox", "oxj", "xju", "jum", "ump", "mps", "pso", "sov", "ove", "ver", "ert", "rth", "the", "hel", "ela", "laz", "azy", "zyd", "ydo", "dog"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationTrigram, "いろはにほへとちりぬるをわかよたれそつねならむうゐのおくやまけふこえてあさきゆめみしゑひもせす")
	assert.Equal(t, []string{"いろは", "ろはに", "はにほ", "にほへ", "ほへと", "へとち", "とちり", "ちりぬ", "りぬる", "ぬるを", "るをわ", "をわか", "わかよ", "かよた", "よたれ", "たれそ", "れそつ", "そつね", "つねな", "ねなら", "ならむ", "らむう", "むうゐ", "うゐの", "ゐのお", "のおく", "おくや", "くやま", "やまけ", "まけふ", "けふこ", "ふこえ", "こえて", "えてあ", "てあさ", "あさき", "さきゆ", "きゆめ", "ゆめみ", "めみし", "みしゑ", "しゑひ", "ゑひも", "ひもせ", "もせす"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationTrigram, `春の夜の夢はうつつよりもかなしき
	夏の夜の夢はうつつに似たり
	秋の夜の夢はうつつを超え
	冬の夜の夢は心に響く

	山のあなたに小さな村が見える
	川の音が静かに耳に届く
	風が木々を通り抜ける音
	星空の下、すべてが平和である`)
	assert.Equal(t, []string{"春の夜", "の夜の", "夜の夢", "の夢は", "夢はう", "はうつ", "うつつ", "つつよ", "つより", "よりも", "りもか", "もかな", "かなし", "なしき", "しき夏", "き夏の", "夏の夜", "の夜の", "夜の夢", "の夢は", "夢はう", "はうつ", "うつつ", "つつに", "つに似", "に似た", "似たり", "たり秋", "り秋の", "秋の夜", "の夜の", "夜の夢", "の夢は", "夢はう", "はうつ", "うつつ", "つつを", "つを超", "を超え", "超え冬", "え冬の", "冬の夜", "の夜の", "夜の夢", "の夢は", "夢は心", "は心に", "心に響", "に響く", "響く山", "く山の", "山のあ", "のあな", "あなた", "なたに", "たに小", "に小さ", "小さな", "さな村", "な村が", "村が見", "が見え", "見える", "える川", "る川の", "川の音", "の音が", "音が静", "が静か", "静かに", "かに耳", "に耳に", "耳に届", "に届く", "届く風", "く風が", "風が木", "が木々", "木々を", "々を通", "を通り", "通り抜", "り抜け", "抜ける", "ける音", "る音星", "音星空", "星空の", "空の下", "の下す", "下すべ", "すべて", "べてが", "てが平", "が平和", "平和で", "和であ", "である"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGse, `春の夜の夢はうつつよりもかなしき
	夏の夜の夢はうつつに似たり
	秋の夜の夢はうつつを超え
	冬の夜の夢は心に響く

	山のあなたに小さな村が見える
	川の音が静かに耳に届く
	風が木々を通り抜ける音
	星空の下、すべてが平和である`)
	assert.Equal(t, []string{"春の", "夜", "の", "夢", "はう", "うつ", "うつつ", "つつ", "つよ", "より", "も", "かな", "かなし", "かなしき", "なし", "しき", "\n", "\t", "夏", "の", "夜", "の", "夢", "はう", "うつ", "うつつ", "つつ", "に", "似", "たり", "\n", "\t", "秋", "の", "夜", "の", "夢", "はう", "うつ", "うつつ", "つつ", "を", "超え", "\n", "\t", "冬", "の", "夜", "の", "夢", "は", "心", "に", "響く", "\n", "\n", "\t", "山", "の", "あな", "あなた", "に", "小さ", "小さな", "村", "が", "見え", "見える", "える", "\n", "\t", "川", "の", "音", "が", "静か", "かに", "耳", "に", "届く", "\n", "\t", "風", "が", "木々", "を", "通り", "通り抜け", "通り抜ける", "抜け", "抜ける", "ける", "音", "\n", "\t", "星空", "の", "下", "、", "すべ", "すべて", "が", "平和", "で", "ある"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGse, "素早い茶色の狐が怠けた犬を飛び越えた")
	assert.Equal(t, []string{"素早", "素早い", "早い", "茶色", "の", "狐", "が", "怠け", "けた", "犬", "を", "飛び", "飛び越え", "越え", "た"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGse, "すばやいちゃいろのきつねがなまけたいぬをとびこえた")
	assert.Equal(t, []string{"すばや", "すばやい", "やい", "いち", "ちゃ", "ちゃい", "ちゃいろ", "いろ", "のき", "きつ", "きつね", "つね", "ねが", "がな", "なま", "なまけ", "まけ", "けた", "けたい", "たい", "いぬ", "を", "とび", "とびこえ", "こえ", "た"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGse, "スバヤイチャイロノキツネガナマケタイヌヲトビコエタ")
	assert.Equal(t, []string{"スバ", "ヤイ", "イチ", "チャイ", "チャイロ", "ノ", "キツ", "キツネ", "ツネ", "ネガ", "ナマ", "ケタ", "タイ", "イヌ", "ヲ", "トビ", "コ", "エ", "タ"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGse, "The quick brown fox jumps over the lazy dog")
	assert.Equal(t, []string{"t", "h", "e", "q", "u", "i", "c", "k", "b", "r", "o", "w", "n", "f", "o", "x", "j", "u", "m", "p", "s", "o", "v", "e", "r", "t", "h", "e", "l", "a", "z", "y", "d", "o", "g"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationGseCh, "施氏食狮史石室诗士施氏，嗜狮，誓食十狮。氏时时适市视狮。十时，适十狮适市。是时，适施氏适市。氏视是十狮，恃矢势，使是十狮逝世。氏拾是十狮尸，适石室。石室湿，氏使侍拭石室。石室拭，氏始试食是十狮尸。食时，始识是十狮尸，实十石狮尸。试释是事。")
	assert.Equal(t, []string{"施", "氏", "食", "狮", "史", "石室", "诗", "士", "施", "氏", "，", "嗜", "狮", "，", "誓", "食", "十", "狮", "。", "氏", "时时", "适", "市", "视", "狮", "。", "十时", "，", "适", "十", "狮", "适", "市", "。", "是", "时", "，", "适", "施", "氏", "适", "市", "。", "氏", "视", "是", "十", "狮", "，", "恃", "矢", "势", "，", "使", "是", "十", "狮", "逝世", "。", "氏", "拾", "是", "十", "狮", "尸", "，", "适", "石室", "。", "石室", "湿", "，", "氏", "使", "侍", "拭", "石室", "。", "石室", "拭", "，", "氏", "始", "试", "食", "是", "十", "狮", "尸", "。", "食", "时", "，", "始", "识", "是", "十", "狮", "尸", "，", "实", "十", "石狮", "尸", "。", "试", "释", "是", "事", "。"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeKr, "아버지가방에들어가신다")
	assert.Equal(t, []string{"아버지", "가", "방", "에", "들어가", "신다"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeKr, "아버지가 방에 들어가신다")
	assert.Equal(t, []string{"아버지", "가", "방", "에", "들어가", "신다"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeKr, "결정하겠다")
	assert.Equal(t, []string{"결정", "하", "겠", "다"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeKr, "한국어를처리하는예시입니다")
	assert.Equal(t, []string{"한국어", "를", "처리", "하", "는", "예시", "입니다"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeKr, "한국어를 처리하는 예시입니다")
	assert.Equal(t, []string{"한국어", "를", "처리", "하", "는", "예시", "입니다"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeJa, `春の夜の夢はうつつよりもかなしき
	夏の夜の夢はうつつに似たり
	秋の夜の夢はうつつを超え
	冬の夜の夢は心に響く

	山のあなたに小さな村が見える
	川の音が静かに耳に届く
	風が木々を通り抜ける音
	星空の下、すべてが平和である`)
	assert.Equal(t, []string{"春", "の", "夜", "の", "夢", "は", "うつつ", "より", "も", "かなしき", "\n\t", "夏", "の", "夜", "の", "夢", "は", "うつつ", "に", "似", "たり", "\n\t", "秋", "の", "夜", "の", "夢", "は", "うつつ", "を", "超え", "\n\t", "冬", "の", "夜", "の", "夢", "は", "心", "に", "響く", "\n\n\t", "山", "の", "あなた", "に", "小さな", "村", "が", "見える", "\n\t", "川", "の", "音", "が", "静か", "に", "耳", "に", "届く", "\n\t", "風", "が", "木々", "を", "通り抜ける", "音", "\n\t", "星空", "の", "下", "、", "すべて", "が", "平和", "で", "ある"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeJa, "素早い茶色の狐が怠けた犬を飛び越えた")
	assert.Equal(t, []string{"素早い", "茶色", "の", "狐", "が", "怠け", "た", "犬", "を", "飛び越え", "た"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeJa, "すばやいちゃいろのきつねがなまけたいぬをとびこえた")
	assert.Equal(t, []string{"すばやい", "ちゃ", "いろ", "の", "きつね", "が", "なまけ", "た", "いぬ", "を", "とびこえ", "た"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeJa, "スバヤイチャイロノキツネガナマケタイヌヲトビコエタ")
	assert.Equal(t, []string{"スバ", "ヤイ", "チャイ", "ロノキツネガナマケタイヌヲトビコエタ"}, tokens)

	tokens = Tokenize(models.PropertyTokenizationKagomeJa, "The quick brown fox jumps over the lazy dog")
	assert.Equal(t, []string{"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}, tokens)
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
	type testCase struct {
		input        string
		tokenization string
		expected     map[string]int
	}

	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	_ = initializeKagomeTokenizerKr()

	alphaInput := "Hello You Beautiful World! hello you beautiful world!"

	testCases := []testCase{
		{
			input:        alphaInput,
			tokenization: models.PropertyTokenizationField,
			expected: map[string]int{
				"Hello You Beautiful World! hello you beautiful world!": 1,
			},
		},
		{
			input:        alphaInput,
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
			input:        alphaInput,
			tokenization: models.PropertyTokenizationLowercase,
			expected: map[string]int{
				"hello":     2,
				"you":       2,
				"beautiful": 2,
				"world!":    2,
			},
		},
		{
			input:        alphaInput,
			tokenization: models.PropertyTokenizationWord,
			expected: map[string]int{
				"hello":     2,
				"you":       2,
				"beautiful": 2,
				"world":     2,
			},
		},
		{
			input:        "한국어를 처리하는 예시입니다 한국어를 처리하는 예시입니다",
			tokenization: models.PropertyTokenizationKagomeKr,
			expected: map[string]int{
				"한국어": 2,
				"를":   2,
				"처리":  2,
				"하":   2,
				"는":   2,
				"예시":  2,
				"입니다": 2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tokenization, func(t *testing.T) {
			terms, dups := TokenizeAndCountDuplicates(tc.tokenization, tc.input)

			assert.Len(t, terms, len(tc.expected))
			assert.Len(t, dups, len(tc.expected))

			for i := range terms {
				assert.Contains(t, tc.expected, terms[i])
				assert.Equal(t, tc.expected[terms[i]], dups[i])
			}
		})
	}
}
