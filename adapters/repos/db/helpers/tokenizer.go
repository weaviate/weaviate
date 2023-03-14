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
	"unicode"
)

// TokenizeField trims white spaces
// (former DataTypeString/Field)
func TokenizeField(in string) []string {
	return []string{strings.TrimFunc(in, unicode.IsSpace)}
}

// TokenizeWhitespace splits on white spaces, does not alter casing
// (former DataTypeString/Word)
func TokenizeWhitespace(in string) []string {
	return strings.FieldsFunc(in, unicode.IsSpace)
}

// TokenizeLowercase splits on white spaces and lowercases the words
func TokenizeLowercase(in string) []string {
	parts := TokenizeWhitespace(in)
	for i, part := range parts {
		parts[i] = strings.ToLower(part)
	}
	return parts
}

// TokenizeWord splits on any non-alphanumerical and lowercases the words
// (former DataTypeText/Word)
func TokenizeWord(in string) []string {
	parts := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	for i, part := range parts {
		parts[i] = strings.ToLower(part)
	}
	return parts
}

// TokenizeWordWithWildcards splits on any non-alphanumerical except wildcard-symbols and
// lowercases the words
func TokenizeWordWithWildcards(in string) []string {
	parts := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '?' && r != '*'
	})
	for i, part := range parts {
		parts[i] = strings.ToLower(part)
	}
	return parts
}

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

func TokenizeTextAndCountDuplicates(in string) ([]string, []int) {
	parts := TokenizeText(in)
	return CountDuplicates(parts)
}

func TokenizeStringAndCountDuplicates(in string) ([]string, []int) {
	parts := TokenizeString(in)
	return CountDuplicates(parts)
}

func CountDuplicates(parts []string) ([]string, []int) {
	count := map[string]int{}
	for _, term := range parts {
		count[term]++
	}

	terms := make([]string, 0, len(count))
	boosts := make([]int, 0, len(count))

	for term, boost := range count {
		terms = append(terms, term)
		boosts = append(boosts, boost)
	}

	return terms, boosts
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
