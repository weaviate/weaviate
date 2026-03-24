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
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// FoldAccents removes diacritic marks from Unicode text by decomposing
// into NFD form and stripping combining marks (Mn category).
// For example: "école" → "ecole", "naïve" → "naive", "São Paulo" → "Sao Paulo".
func FoldAccents(s string) string {
	// NFD decomposes characters: e.g. 'é' → 'e' + combining acute accent
	decomposed := norm.NFD.String(s)

	result := make([]byte, 0, len(decomposed))
	for i := 0; i < len(decomposed); {
		r, size := utf8.DecodeRuneInString(decomposed[i:])
		if !unicode.Is(unicode.Mn, r) {
			// Keep everything except combining marks (Mn = Mark, Nonspacing)
			result = append(result, decomposed[i:i+size]...)
		}
		i += size
	}

	// Recompose to NFC for clean storage
	return norm.NFC.String(string(result))
}

// FoldAccentsSlice applies accent folding to each element of a string slice in-place.
func FoldAccentsSlice(terms []string) []string {
	for i := range terms {
		terms[i] = FoldAccents(terms[i])
	}
	return terms
}
