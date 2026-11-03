//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tokenizer

import (
	"testing"

	"github.com/go-ego/gse"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

var gseSampleTexts = map[string][]string{
	"ja": {
		"私は東京に住んでいます",
		"日本語のテキストを分割します",
		"ベクトルデータベース",
		"東京都庁は新宿区にあります",
		"トリック　オア　トリート",
	},
	"zh": {
		"我爱北京天安门",
		"中华人民共和国今天成立了",
		"繁體中文測試句子",
		"小明硕士毕业于中国科学院计算所，后在日本京都大学深造",
		"台灣的國語和大陸的普通話",
	},
}

// TestGseEmbeddedMatchesFileLoader pins that the segmenter built from the embedded
// dictionaries is identical to the one gse.New builds from the dictionary files: same
// tokens in the same order with the same frequency and part of speech, and the same
// segmentation. Indexes written by binaries that still used the file loader must keep
// matching queries tokenized by the embedded one.
//
// The file loader needs the gse module's files on disk, which only an untrimmed test
// binary can find, so the test skips under `go test -trimpath`.
func TestGseEmbeddedMatchesFileLoader(t *testing.T) {
	for _, lang := range []string{"ja", "zh"} {
		t.Run(lang, func(t *testing.T) {
			want := gse.Segmenter{SkipLog: true}
			if err := want.LoadDict(lang); err != nil {
				t.Skipf("file-based gse loader unavailable (trimmed build?): %v", err)
			}
			got, err := newGseSegmenter(lang)
			require.NoError(t, err)

			require.Equal(t, len(want.Dict.Tokens), len(got.Dict.Tokens), "dictionary size")
			for i := range want.Dict.Tokens {
				w, g := want.Dict.Tokens[i], got.Dict.Tokens[i]
				if w.Text() != g.Text() || w.Freq() != g.Freq() || w.Pos() != g.Pos() {
					t.Fatalf("token %d: file loader %q/%v/%q, embedded %q/%v/%q",
						i, w.Text(), w.Freq(), w.Pos(), g.Text(), g.Freq(), g.Pos())
				}
			}
			require.Equal(t, want.Dict.TotalFreq(), got.Dict.TotalFreq(), "total frequency")
			require.Equal(t, want.Dict.MaxTokenLen(), got.Dict.MaxTokenLen(), "max token length")

			for _, text := range gseSampleTexts[lang] {
				require.Equal(t, want.CutAll(text), got.CutAll(text), "CutAll %q", text)
				require.Equal(t, want.Cut(text, true), got.Cut(text, true), "Cut %q", text)
			}
		})
	}
}

// TestGseTokenizersLoadWithoutDictionaryFiles pins that both gse tokenizers initialise
// from the embedded dictionaries and segment text. It is the regression test for
// release builds: run it with `go test -trimpath`, where the file loader cannot find the
// dictionaries, and it still has to pass.
func TestGseTokenizersLoadWithoutDictionaryFiles(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_GSE", "true")
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	InitOptionalTokenizers()
	require.True(t, UseGse, "gse (ja) tokenizer must load")
	require.True(t, UseGseCh, "gse_ch tokenizer must load")

	cases := []struct {
		tokenization string
		in           string
		wantTokens   []string
	}{
		{models.PropertyTokenizationGse, "私は東京に住んでいます", []string{"東京", "住ん"}},
		{models.PropertyTokenizationGse, "ベクトルデータベース", []string{"ベクトル", "データベース"}},
		{models.PropertyTokenizationGseCh, "我爱北京天安门", []string{"北京", "天安门"}},
		{models.PropertyTokenizationGseCh, "繁體中文測試句子", []string{"繁體中文", "測試", "句子"}},
	}
	for _, tc := range cases {
		t.Run(tc.tokenization+"/"+tc.in, func(t *testing.T) {
			got := Tokenize(tc.tokenization, tc.in)
			require.NotEmpty(t, got)
			for _, w := range tc.wantTokens {
				require.Contains(t, got, w)
			}
		})
	}
}
