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

package inverted

import (
	"bytes"
	"encoding/binary"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/semi-technologies/weaviate/entities/models"
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
	stopwords stopwords.StopwordDetector
}

// Text removes non alpha-numeric and splits into lowercased words, then aggregates
// duplicates
func (a *Analyzer) Text(tokenization, in string) []Countable {
	parts := textArrayTokenize(tokenization, []string{in})
	return a.countParts(parts)
}

// TextArray removes non alpha-numeric and splits into lowercased words, then aggregates
// duplicates
func (a *Analyzer) TextArray(tokenization string, in []string) []Countable {
	parts := textArrayTokenize(tokenization, in)
	return a.countParts(parts)
}

func textArrayTokenize(tokenization string, in []string) []string {
	var parts []string

	switch tokenization {
	case models.PropertyTokenizationWord:
		for _, value := range in {
			parts = append(parts, helpers.TokenizeText(value)...)
		}
	}

	return parts
}

// String splits only on spaces and does not lowercase, then aggregates
// duplicates
func (a *Analyzer) String(tokenization, in string) []Countable {
	parts := stringArrayTokenize(tokenization, []string{in})
	return a.countParts(parts)
}

// StringArray splits only on spaces and does not lowercase, then aggregates
// duplicates
func (a *Analyzer) StringArray(tokenization string, in []string) []Countable {
	parts := stringArrayTokenize(tokenization, in)
	return a.countParts(parts)
}

func stringArrayTokenize(tokenization string, in []string) []string {
	var parts []string

	switch tokenization {
	case models.PropertyTokenizationField:
		for _, value := range in {
			if trimmed := helpers.TrimString(value); trimmed != "" {
				parts = append(parts, trimmed)
			}
		}
	case models.PropertyTokenizationWord:
		for _, value := range in {
			parts = append(parts, helpers.TokenizeString(value)...)
		}
	}

	return parts
}

func (a *Analyzer) countParts(parts []string) []Countable {
	terms := map[string]uint64{}
	for _, word := range parts {
		if a.stopwords.IsStopword(word) {
			continue
		}

		count, ok := terms[word]
		if !ok {
			terms[word] = 0
		}
		terms[word] = count + 1
	}

	out := make([]Countable, len(terms))
	i := 0
	for term, count := range terms {
		out[i] = Countable{
			Data:          []byte(term),
			TermFrequency: float32(count),
		}
		i++
	}

	return out
}

// Int requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) Int(in int64) ([]Countable, error) {
	data, err := LexicographicallySortableInt64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// Int array requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) IntArray(in []int64) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		data, err := LexicographicallySortableInt64(in[i])
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: data}
	}

	return out, nil
}

// Float requires no analysis, so it's actually just a simple conversion to a
// lexicographically sortable byte slice.
func (a *Analyzer) Float(in float64) ([]Countable, error) {
	data, err := LexicographicallySortableFloat64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// Float array requires no analysis, so it's actually just a simple conversion to a
// lexicographically sortable byte slice.
func (a *Analyzer) FloatArray(in []float64) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		data, err := LexicographicallySortableFloat64(in[i])
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: data}
	}

	return out, nil
}

// BoolArray requires no analysis, so it's actually just a simple conversion to a
// little-endian ordered byte slice
func (a *Analyzer) BoolArray(in []bool) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		b := bytes.NewBuffer(nil)
		err := binary.Write(b, binary.LittleEndian, &in)
		if err != nil {
			return nil, err
		}
		out[i] = Countable{Data: b.Bytes()}
	}

	return out, nil
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
		{
			Data: b.Bytes(),
		},
	}, nil
}

// RefCount does not index the content of the refs, but only the count with 0
// being an explicitly allowed value as well.
func (a *Analyzer) RefCount(in models.MultipleRef) ([]Countable, error) {
	length := uint64(len(in))
	data, err := LexicographicallySortableUint64(length)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// Ref indexes references as beacon-strings
func (a *Analyzer) Ref(in models.MultipleRef) ([]Countable, error) {
	out := make([]Countable, len(in))

	for i, ref := range in {
		out[i] = Countable{
			Data: []byte(ref.Beacon),
		}
	}

	return out, nil
}

func NewAnalyzer(stopwords stopwords.StopwordDetector) *Analyzer {
	return &Analyzer{stopwords: stopwords}
}
