package classification

// TODO: This code is duplicated across weaviate and contextionary which makes
// changes risky. Can we find a single source of truth for this logic

import (
	"strings"
	"unicode"
)

func newSplitter() *splitter {
	return &splitter{}
}

type splitter struct{}

func (s *splitter) Split(corpus string) []string {
	return strings.FieldsFunc(corpus, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})
}
