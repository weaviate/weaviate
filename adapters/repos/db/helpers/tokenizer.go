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

package helpers

import (
	"strings"
	"unicode"
)

// TokenizeString only splits on white spaces, it does not alter casing
func TokenizeString(in string) []string {
	return strings.FieldsFunc(in, unicode.IsSpace)
}

// Tokenize Text splits on any non-alphanumerical and lowercases the words
func TokenizeText(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})
	for i, part := range parts {
		parts[i] = strings.ToLower(part)
	}

	return parts
}

// Tokenize Text splits on any non-alphanumerical except wildcard-symbols and
// lowercases the words
func TokenizeTextKeepWildcards(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c) && c != '?' && c != '*'
	})

	for i, part := range parts {
		parts[i] = strings.ToLower(part)
	}

	return parts
}

// TrimString trims on white spaces
func TrimString(in string) string {
	return strings.TrimFunc(in, unicode.IsSpace)
}
