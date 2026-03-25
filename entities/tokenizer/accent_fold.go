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
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// foldTable maps characters that NFD decomposition cannot handle to their
// ASCII equivalents. This covers stroked/barred letters, special letters,
// ligatures, and hooked/tailed letters.
var foldTable = map[rune]string{
	// Stroked / barred letters
	'ł': "l", 'Ł': "L",
	'ø': "o", 'Ø': "O",
	'đ': "d", 'Đ': "D",
	'ħ': "h", 'Ħ': "H",
	'ŧ': "t", 'Ŧ': "T",
	'ɨ': "i", 'Ɨ': "I",
	'ƀ': "b", 'Ƀ': "B",
	'ɇ': "e", 'Ɇ': "E",
	'ɉ': "j", 'Ɉ': "J",
	'ɍ': "r", 'Ɍ': "R",
	'ɏ': "y", 'Ɏ': "Y",
	'ⱥ': "a", 'Ⱥ': "A",

	// Special letters
	'ð': "d", 'Ð': "D",
	'ı': "i", 'İ': "I",
	'ĸ': "k",
	'ŉ': "n",
	'ſ': "s",

	// Ligatures (single-char → multi-char expansion)
	'æ': "ae", 'Æ': "AE",
	'œ': "oe", 'Œ': "OE",
	'ĳ': "ij", 'Ĳ': "IJ",
	'ß': "ss", 'ẞ': "SS",
	'þ': "th", 'Þ': "TH",
	'ﬀ': "ff",
	'ﬁ': "fi",
	'ﬂ': "fl",
	'ﬃ': "ffi",
	'ﬄ': "ffl",
	'ﬅ': "st",
	'ﬆ': "st",

	// Hooked / tailed letters
	'ɓ': "b",
	'ƈ': "c",
	'ɗ': "d",
	'ƒ': "f",
	'ɠ': "g",
	'ɦ': "h",
	'ƙ': "k",
	'ɱ': "m",
	'ɲ': "n", 'ƞ': "n",
	'ƥ': "p",
	'ʠ': "q",
	'ɽ': "r",
	'ʂ': "s",
	'ƭ': "t",
	'ʋ': "v",
	'ⱳ': "w",
	'ƴ': "y",
	'ʐ': "z",
}

// FoldAccents normalizes accented text for accent-insensitive search.
//
// Phase 1: Explicit replacements for characters that don't decompose
// under NFD (stroked letters, ligatures, special letters, hooked letters).
// ł→l, ø→o, æ→ae, ß→ss, ð→d, þ→th, etc.
//
// Phase 2: NFD decompose + strip combining marks (category Mn only).
// Handles all Latin characters that decompose into base + accent:
// é→e, ñ→n, ç→c, ž→z, etc. Only Mn (Mark, Nonspacing) is stripped —
// Mc (Mark, Spacing Combining) is preserved to avoid destroying vowel
// signs in Indic and Southeast Asian scripts.
//
// Phase 3: NFC recompose to clean up any remaining sequences.
func FoldAccents(s string) string {
	// Phase 1: replace characters that NFD doesn't decompose
	var buf strings.Builder
	buf.Grow(len(s))
	for _, r := range s {
		if repl, ok := foldTable[r]; ok {
			buf.WriteString(repl)
		} else {
			buf.WriteRune(r)
		}
	}

	// Phase 2: NFD decompose and strip nonspacing marks (Mn)
	decomposed := norm.NFD.String(buf.String())

	result := make([]byte, 0, len(decomposed))
	for i := 0; i < len(decomposed); {
		r, size := utf8.DecodeRuneInString(decomposed[i:])
		if !unicode.Is(unicode.Mn, r) {
			result = append(result, decomposed[i:i+size]...)
		}
		i += size
	}

	// Phase 3: recompose to NFC for clean storage
	return norm.NFC.String(string(result))
}

// FoldAccentsSlice applies accent folding to each element of a string slice in-place.
func FoldAccentsSlice(terms []string) []string {
	for i := range terms {
		terms[i] = FoldAccents(terms[i])
	}
	return terms
}
