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
	"fmt"
	"strings"
	"unicode"

	"github.com/go-ego/gse"
)

// newGseSegmenter builds the "ja" or "zh" segmenter from the dictionaries the gse module
// embeds in the binary, reproducing gse.New's file-based dictionary token for token.
//
// gse.New locates its dictionary files through runtime.Caller. In a -trimpath build, which
// is how every release binary is built, that yields a module-relative path, the files are
// not found, and the tokenizer is silently unavailable.
//
// gse.NewEmbed is the obvious replacement, but its loader differs from the file loader in
// two ways that change tokenization. Both are corrected here so that indexes written by
// binaries that still used the file loader keep matching queries tokenized by this one:
//
//   - "zh" embeds simplified before traditional, while the file loader reads traditional
//     first and keeps the first frequency seen for words present in both. Load in file order.
//   - One Japanese entry is a lone U+3000 ideographic space. The embedded loader trims it to
//     "" and inserts an empty key into the dictionary trie, after which the trie no longer
//     de-duplicates and about 3000 words get a second, later frequency. The file loader's
//     whitespace-splitting scanner never produced that entry, nor the 14 entries with an
//     embedded U+3000. Rebuild the dictionary without them.
//
// TestGseEmbeddedMatchesFileLoader pins the parity.
func newGseSegmenter(lang string) (gse.Segmenter, error) {
	var (
		seg gse.Segmenter
		err error
	)
	switch lang {
	case "ja":
		seg, err = gse.NewEmbed("ja")
	case "zh":
		seg, err = gse.NewEmbed("zh_t")
		if err == nil {
			err = seg.LoadDictEmbed("zh_s")
		}
	default:
		return seg, fmt.Errorf("unsupported gse dictionary %q", lang)
	}
	if err != nil {
		return seg, err
	}

	clean := gse.NewDict()
	for i := range seg.Dict.Tokens {
		tk := seg.Dict.Tokens[i]
		if text := tk.Text(); text == "" || strings.ContainsFunc(text, unicode.IsSpace) {
			continue
		}
		if err := clean.AddToken(tk); err != nil {
			return seg, fmt.Errorf("rebuild %s dictionary: %w", lang, err)
		}
	}
	seg.Dict = clean
	seg.CalcToken()
	return seg, nil
}
