package inverted

import (
	"strings"
	"unicode"
)

type Countable struct {
	Data          []byte
	TermFrequency float32
}

type Analyzer struct{}

// Text removes non alpha-numeric and splits into words, then aggregates
// duplicates
func (a *Analyzer) Text(in string) []Countable {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})

	terms := map[string]uint32{}
	total := 0
	for _, word := range parts {
		word = strings.ToLower(word)
		count, ok := terms[word]
		if !ok {
			terms[word] = 0
		}
		terms[word] = count + 1
		total++
	}

	out := make([]Countable, len(terms))
	i := 0
	for term, count := range terms {
		out[i] = Countable{
			Data:          []byte(term),
			TermFrequency: float32(count) / float32(total),
		}
		i++
	}

	return out
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{}
}
