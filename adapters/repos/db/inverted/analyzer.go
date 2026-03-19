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

package inverted

import (
	"bytes"
	"encoding/binary"

	"github.com/google/uuid"
	ent "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

type IsFallbackToSearchable func() bool

type Countable struct {
	Data          []byte
	TermFrequency float32
}

type Property struct {
	Name               string
	Items              []Countable
	Length             int
	HasFilterableIndex bool // roaring set index
	HasSearchableIndex bool // map index (with frequencies)
	HasRangeableIndex  bool // roaring set index for ranged queries
}

type NilProperty struct {
	Name                string
	AddToPropertyLength bool
}

// NestedValue is a single analyzed value from a nested property, ready to
// be written to the value bucket. The key in the bucket will be
// hash8(Path) + Data; the bitmap values will be Positions with the real
// docID ORed in.
type NestedValue struct {
	Path      string   // dot-notation path, e.g. "addresses.city"
	Data      []byte   // analyzed value bytes
	Positions []uint64 // positions with docID=0
}

// NestedProperty holds all analyzed values and metadata from position
// assignment of a single nested property.
type NestedProperty struct {
	Name     string         // top-level property name (for bucket naming)
	Values   []NestedValue  // analyzed values for the value bucket
	Idx      []NestedMeta   // _idx metadata entries
	Exists   []NestedMeta   // _exists metadata entries
}

// NestedMeta is an _idx or _exists metadata entry from position assignment.
type NestedMeta struct {
	Path      string   // e.g. "addresses" for _idx, "owner.firstname" for _exists
	Index     int      // 0-based element index (only used for _idx; -1 for _exists)
	Positions []uint64 // positions with docID=0
}

func DedupItems(props []Property) []Property {
	for i := range props {
		seen := map[string]struct{}{}
		items := props[i].Items

		var key string
		// reverse order to keep latest elements
		for j := len(items) - 1; j >= 0; j-- {
			key = string(items[j].Data)
			if _, ok := seen[key]; ok {
				// remove element already seen
				items = append(items[:j], items[j+1:]...)
			}
			seen[key] = struct{}{}
		}
		props[i].Items = items
	}
	return props
}

type Analyzer struct {
	isFallbackToSearchable IsFallbackToSearchable
	className              string
}

// Text tokenizes given input according to selected tokenization,
// then aggregates duplicates
func (a *Analyzer) Text(tokenization, in string) []Countable {
	return a.TextArray(tokenization, []string{in})
}

// TextArray tokenizes given input according to selected tokenization,
// then aggregates duplicates
func (a *Analyzer) TextArray(tokenization string, inArr []string) []Countable {
	var terms []string
	for _, in := range inArr {
		terms = append(terms, tokenizer.TokenizeForClass(tokenization, in, a.className)...)
	}

	counts := map[string]uint64{}
	for _, term := range terms {
		counts[term]++
	}

	countable := make([]Countable, len(counts))
	i := 0
	for term, count := range counts {
		countable[i] = Countable{
			Data:          []byte(term),
			TermFrequency: float32(count),
		}
		i++
	}
	return countable
}

// Int requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) Int(in int64) ([]Countable, error) {
	data, err := ent.LexicographicallySortableInt64(in)
	if err != nil {
		return nil, err
	}

	return []Countable{
		{
			Data: data,
		},
	}, nil
}

// UUID requires no analysis, so it's just dumping the raw binary representation
func (a *Analyzer) UUID(in uuid.UUID) ([]Countable, error) {
	return []Countable{
		{
			Data: in[:],
		},
	}, nil
}

// UUID array requires no analysis, so it's just dumping the raw binary
// representation of each contained element
func (a *Analyzer) UUIDArray(in []uuid.UUID) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		out[i] = Countable{
			Data: in[i][:],
		}
	}

	return out, nil
}

// Int array requires no analysis, so it's actually just a simple conversion to a
// string-formatted byte slice of the int
func (a *Analyzer) IntArray(in []int64) ([]Countable, error) {
	out := make([]Countable, len(in))
	for i := range in {
		data, err := ent.LexicographicallySortableInt64(in[i])
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
	data, err := ent.LexicographicallySortableFloat64(in)
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
		data, err := ent.LexicographicallySortableFloat64(in[i])
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
		err := binary.Write(b, binary.LittleEndian, &in[i])
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
	data, err := ent.LexicographicallySortableUint64(length)
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

func NewAnalyzer(isFallbackToSearchable IsFallbackToSearchable, className string) *Analyzer {
	if isFallbackToSearchable == nil {
		isFallbackToSearchable = func() bool { return false }
	}
	return &Analyzer{isFallbackToSearchable: isFallbackToSearchable, className: className}
}
