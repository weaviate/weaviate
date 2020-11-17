//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
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
}

// Text removes non alpha-numeric and splits into words, then aggregates
// duplicates
func (a *Analyzer) Text(in string) []Countable {
	parts := helpers.TokenizeText(in)
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
	parts := helpers.TokenizeString(in)
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
func (a *Analyzer) Int(in int64) ([]Countable, error) {
	data, err := LexicographicallySortableInt64(in)
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

// RefCount does not index the content of the refs, but only the count with 0
// being an explicitly allowed value as well.
func (a *Analyzer) RefCount(in models.MultipleRef) ([]Countable, error) {
	length := uint32(len(in))
	data, err := LexicographicallySortableUint32(length)
	if err != nil {
		return nil, err
	}

	return []Countable{
		Countable{
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

func NewAnalyzer() *Analyzer {
	return &Analyzer{}
}
