package inverted

import (
	"bytes"
	"encoding/binary"
	"strings"
	"unicode"
)

type Countable struct {
	Data          []byte
	TermFrequency float32
}

type Property struct {
	Name         string
	Items        []Countable
	HasFrequency bool
}

type Analyzer struct {
}

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

// String splits only on spaces and does not lowercase, then aggregates
// duplicates
func (a *Analyzer) String(in string) []Countable {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return unicode.IsSpace(c)
	})

	terms := map[string]uint32{}
	total := 0
	for _, word := range parts {
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

// Int requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) Int(in int) ([]Countable, error) {
	data, err := LexicographicallySortableInt64(int64(in))
	if err != nil {
		return nil, err
	}

	return []Countable{
		Countable{
			Data: data,
		},
	}, nil
}

// Float requires no analysis, so it's actually just a simple conversion to a
// lexicographically sortable byte slice.
func (a *Analyzer) Float(in float64) ([]Countable, error) {
	data, err := LexicographicallySortableFloat64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		Countable{
			Data: data,
		},
	}, nil
}

// Bool requires no analysis, so it's actually just a simple conversion to a
// little-endian ordered byte slice
func (a *Analyzer) Bool(in bool) ([]Countable, error) {
	b := bytes.NewBuffer(nil)
	err := binary.Write(b, binary.LittleEndian, &in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		Countable{
			Data: b.Bytes(),
		},
	}, nil
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{}
}
