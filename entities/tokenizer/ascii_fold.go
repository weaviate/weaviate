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

// Package tokenizer provides text tokenization and accent folding for
// Weaviate's inverted index.
//
// # Accent folding
//
// FoldASCII removes diacritical marks from Latin characters while
// preserving the base letters.  It uses a three-phase approach:
//
//  1. Table-driven replacement for characters that Unicode NFD
//     normalization does not decompose (ø→o, æ→ae, ß→ss, ð→d, þ→th,
//     ł→l, đ→d, ħ→h, ŧ→t, etc.).
//  2. NFD decomposition + stripping of combining marks (category Mn).
//     Only Mn marks are stripped so that vowel signs in other scripts are
//     not affected.
//  3. NFC recomposition for clean storage.
//
// Characters that have table-driven replacements but no NFD decomposition:
//
//	Character              | Replacement | Language / script
//	-----------------------|-------------|------------------------------------------
//	ł (U+0142) — L-stroke | l           | Polish
//	Ł (U+0141)            | L           | Polish
//	ø (U+00F8) — O-stroke | o           | Danish, Norwegian
//	Ø (U+00D8)            | O           | Danish, Norwegian
//	æ (U+00E6)            | ae          | Danish, Norwegian, Icelandic, Old English
//	Æ (U+00C6)            | AE          |
//	œ (U+0153)            | oe          | French
//	Œ (U+0152)            | OE          |
//	ß (U+00DF) — Eszett   | ss          | German
//	ẞ (U+1E9E)            | SS          | German (capital)
//	ð (U+00F0) — Eth      | d           | Icelandic, Old English
//	Ð (U+00D0)            | D           |
//	þ (U+00FE) — Thorn    | th          | Icelandic, Old English
//	Þ (U+00DE)            | Th          |
//	đ (U+0111) — D-stroke | d           | Croatian, Vietnamese
//	Đ (U+0110)            | D           |
//	ħ (U+0127) — H-stroke | h           | Maltese
//	Ħ (U+0126)            | H           |
//	ŧ (U+0167) — T-stroke | t           | Northern Sami
//	Ŧ (U+0166)            | T           |
//	ı (U+0131) — dotless i| i           | Turkish
//
// Additional entries cover hooked, tailed, and other modified Latin
// letters that NFD does not decompose (ɓ→b, ƈ→c, ɗ→d, etc.).
//
// The fold table is intentionally limited to Latin-script characters.
// CJK, Cyrillic, Arabic, Devanagari, and other scripts are passed through
// unchanged.  Within the Latin block, characters that already have a clean
// NFD decomposition (e.g. é → e + combining acute) are handled entirely by
// the remaining stroked letters, ligatures, special letters, and
// hooked/tailed letters.
package tokenizer

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// foldTable maps characters that NFD normalization does NOT decompose
// to their ASCII equivalents.  Characters that decompose cleanly under
// NFD (like é → e + ◌́) are handled in Phase 2 of FoldASCII.
var foldTable = map[rune]string{
	// Stroked letters
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

	// Ligatures → digraphs
	'æ': "ae", 'Æ': "AE",
	'œ': "oe", 'Œ': "OE",
	'ĳ': "ij", 'Ĳ': "IJ",

	// Special letters
	'ß': "ss", 'ẞ': "SS",
	'ð': "d", 'Ð': "D",
	'þ': "th", 'Þ': "TH",

	// Dotless i / dotted I / kra / long s / apostrophe-n
	'ı': "i", 'İ': "I",
	'ĸ': "k",
	'ŉ': "n",
	'ſ': "s",

	// Typographic ligatures
	'ﬀ': "ff",
	'ﬁ': "fi",
	'ﬂ': "fl",
	'ﬃ': "ffi",
	'ﬄ': "ffl",
	'ﬅ': "st",
	'ﬆ': "st",

	// Hooked / tailed letters (not decomposed by NFD)
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

	// Hooked / tailed uppercase
	'Ɓ': "B",
	'Ƈ': "C",
	'Ɗ': "D",
	'Ƒ': "F",
	'Ɠ': "G",
	'Ƙ': "K",
	'Ɲ': "N",
	'Ƥ': "P",
	'Ƭ': "T",
	'Ʋ': "V",
	'Ƴ': "Y",
	'Ȥ': "Z",
}

// IgnoreSet holds the precomputed data for the asciiFoldIgnore feature.
// Create one via BuildIgnoreSet.
type IgnoreSet struct {
	// runes is the set of NFC characters to preserve (both cases).
	runes map[rune]struct{}
	// suffixes maps an NFD base rune to the combining-mark suffixes that
	// recompose to an ignored character.  For ignore={"é"}, this contains
	// 'e' → ["\u0301"] and 'E' → ["\u0301"].
	// Phase 2 checks if the marks following a base character match any
	// suffix — a simple string prefix check, no NFC call needed.
	suffixes map[rune][]string
}

// FoldASCII removes diacritical marks from Latin characters.
//
// Phase 1: table-driven replacement for characters NFD doesn't decompose
// (ł→l, ø→o, æ→ae, ß→ss, ð→d, þ→th, etc.).
//
// Phase 2: NFD decompose + strip combining marks (Mn category).
// When ignore is non-nil, base characters whose NFD suffix matches a
// precomputed pattern are preserved along with their combining marks.
//
// Phase 3: NFC recompose to clean up any remaining sequences.
//
// If ignore is non-nil, characters present in the set are preserved
// without folding.
func FoldASCII(s string, ignore *IgnoreSet) string {
	// Phase 1: replace characters that NFD doesn't decompose
	var buf strings.Builder
	buf.Grow(len(s))
	hasNonASCII := false
	for _, r := range s {
		if ignore != nil {
			if _, skip := ignore.runes[r]; skip {
				buf.WriteRune(r)
				if r > 127 {
					hasNonASCII = true
				}
				continue
			}
		}
		if repl, ok := foldTable[r]; ok {
			buf.WriteString(repl)
		} else {
			buf.WriteRune(r)
			if r > 127 {
				hasNonASCII = true
			}
		}
	}

	// Fast path: if Phase 1 resolved everything to ASCII, NFD/NFC are
	// identity transforms and Phase 2 has no marks to strip.
	if !hasNonASCII {
		return buf.String()
	}

	// Phase 2: NFD decompose and strip nonspacing marks (Mn)
	decomposed := norm.NFD.String(buf.String())

	result := make([]byte, 0, len(decomposed))
	skipMarks := false
	for i := 0; i < len(decomposed); {
		r, size := utf8.DecodeRuneInString(decomposed[i:])
		if unicode.Is(unicode.Mn, r) {
			if !skipMarks {
				// Strip this combining mark (normal folding behavior)
			} else {
				// Preserve this mark — it belongs to an ignored character
				result = append(result, decomposed[i:i+size]...)
			}
		} else {
			// Base character — check if it should be ignored
			skipMarks = false
			if ignore != nil {
				if patterns, ok := ignore.suffixes[r]; ok {
					// Collect the combining marks that follow this base.
					markStart := i + size
					j := markStart
					for j < len(decomposed) {
						nr, ns := utf8.DecodeRuneInString(decomposed[j:])
						if !unicode.Is(unicode.Mn, nr) {
							break
						}
						j += ns
					}
					marks := decomposed[markStart:j]
					// Check if the marks match any precomputed suffix.
					for _, suffix := range patterns {
						if marks == suffix {
							skipMarks = true
							break
						}
					}
				}
			}
			// After NFD decomposition, base characters that are in the
			// foldTable (e.g. ø from ǿ→ø+acute) must still be replaced,
			// unless they are in the ignore set.
			if !skipMarks {
				ignored := false
				if ignore != nil {
					_, ignored = ignore.runes[r]
				}
				if !ignored {
					if repl, ok := foldTable[r]; ok {
						result = append(result, repl...)
					} else {
						result = append(result, decomposed[i:i+size]...)
					}
				} else {
					result = append(result, decomposed[i:i+size]...)
				}
			} else {
				result = append(result, decomposed[i:i+size]...)
			}
		}
		i += size
	}

	// Phase 3: recompose to NFC for clean storage
	return norm.NFC.String(string(result))
}

// BuildIgnoreSet converts a slice of strings (each typically a single character)
// into an IgnoreSet for use with FoldASCII.  It pre-decomposes the ignored
// characters into base + combining-mark suffixes so that Phase 2 can match
// by simple string comparison instead of calling norm.NFC.String().
func BuildIgnoreSet(chars []string) *IgnoreSet {
	if len(chars) == 0 {
		return nil
	}
	runes := make(map[rune]struct{}, len(chars)*2)
	suffixes := make(map[rune][]string, len(chars)*2)

	for _, s := range chars {
		// Normalize to NFC so that NFD input like "e" + combining acute
		// becomes the single codepoint U+00E9.
		s = norm.NFC.String(s)
		for _, r := range s {
			runes[r] = struct{}{}
			// Include both cases so that ignore list entries like 'ø'
			// also prevent folding of 'Ø' (and vice versa).
			lo := unicode.ToLower(r)
			up := unicode.ToUpper(r)
			runes[lo] = struct{}{}
			runes[up] = struct{}{}

			// Pre-decompose to extract base + suffix for each case variant.
			for _, caseR := range []rune{lo, up} {
				decomposed := norm.NFD.String(string(caseR))
				// Extract base rune and the combining marks that follow.
				baseR, baseSize := utf8.DecodeRuneInString(decomposed)
				markSuffix := decomposed[baseSize:]
				// Only add if there are combining marks (otherwise the character
				// is already handled by the Phase 1 runes check or foldTable).
				if len(markSuffix) > 0 {
					// Deduplicate: don't add the same suffix twice for the same base.
					existing := suffixes[baseR]
					found := false
					for _, s := range existing {
						if s == markSuffix {
							found = true
							break
						}
					}
					if !found {
						suffixes[baseR] = append(existing, markSuffix)
					}
				}
			}
		}
	}
	return &IgnoreSet{runes: runes, suffixes: suffixes}
}

// FoldASCIISlice applies accent folding to each element of a string slice in-place.
func FoldASCIISlice(terms []string, ignore *IgnoreSet) []string {
	for i := range terms {
		terms[i] = FoldASCII(terms[i], ignore)
	}
	return terms
}
