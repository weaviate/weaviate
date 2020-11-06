package helpers

import (
	"strings"
	"unicode"
)

func TokenizeString(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return unicode.IsSpace(c)
	})
	return parts
}

func TokenizeText(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})
	return parts
}
