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

package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSelectPropertiesLookup(t *testing.T) {
	tests := []struct {
		name     string
		props    SelectProperties
		lookup   string
		expected *int // index into props, nil for no match
	}{
		{
			name:     "nil props",
			props:    nil,
			lookup:   "anything",
			expected: nil,
		},
		{
			name:     "empty props",
			props:    SelectProperties{},
			lookup:   "anything",
			expected: nil,
		},
		{
			name: "missing property",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
				{Name: "author", IsPrimitive: true},
			},
			lookup:   "publisher",
			expected: nil,
		},
		{
			name: "single match",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
			},
			lookup:   "title",
			expected: ptrTo(0),
		},
		{
			name: "match among many",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
				{Name: "author", IsPrimitive: true},
				{Name: "publisher", IsPrimitive: true},
			},
			lookup:   "publisher",
			expected: ptrTo(2),
		},
		{
			name: "duplicate names return first occurrence",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
				{Name: "dup", IsPrimitive: true},
				{Name: "dup", IsPrimitive: false},
			},
			lookup:   "dup",
			expected: ptrTo(1),
		},
		{
			name: "empty name lookup",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
			},
			lookup:   "",
			expected: nil,
		},
		{
			name: "ref property",
			props: SelectProperties{
				{Name: "title", IsPrimitive: true},
				{
					Name: "ofPublisher",
					Refs: []SelectClass{{
						ClassName: "Publisher",
						RefProperties: SelectProperties{
							{Name: "name", IsPrimitive: true},
						},
					}},
				},
			},
			lookup:   "ofPublisher",
			expected: ptrTo(1),
		},
		{
			name: "nested object property",
			props: SelectProperties{
				{
					Name:     "address",
					IsObject: true,
					Props: SelectProperties{
						{Name: "street", IsPrimitive: true},
					},
				},
			},
			lookup:   "address",
			expected: ptrTo(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("FindProperty", func(t *testing.T) {
				res := tt.props.FindProperty(tt.lookup)
				if tt.expected == nil {
					assert.Nil(t, res)
				} else {
					require.NotNil(t, res)
					// the result must alias the slice element, not a copy
					assert.Same(t, &tt.props[*tt.expected], res)
				}
			})

			t.Run("Indexed", func(t *testing.T) {
				res := tt.props.Indexed().Find(tt.lookup)
				if tt.expected == nil {
					assert.Nil(t, res)
				} else {
					require.NotNil(t, res)
					assert.Same(t, &tt.props[*tt.expected], res)
				}
			})
		})
	}
}

func TestIndexedEmptyYieldsNil(t *testing.T) {
	assert.Nil(t, SelectProperties(nil).Indexed())
	assert.Nil(t, SelectProperties{}.Indexed())
}

func TestFindSelectClass(t *testing.T) {
	prop := SelectProperty{
		Name: "ofPublisher",
		Refs: []SelectClass{
			{ClassName: "Publisher"},
			{ClassName: "Journal"},
		},
	}

	t.Run("match aliases the slice element", func(t *testing.T) {
		res := prop.FindSelectClass(schema.ClassName("Journal"))
		require.NotNil(t, res)
		assert.Same(t, &prop.Refs[1], res)
	})

	t.Run("miss", func(t *testing.T) {
		assert.Nil(t, prop.FindSelectClass(schema.ClassName("Magazine")))
	})

	t.Run("no refs", func(t *testing.T) {
		assert.Nil(t, SelectProperty{}.FindSelectClass(schema.ClassName("Publisher")))
	})
}

func TestFindSelectProperty(t *testing.T) {
	prop := SelectProperty{
		Name:     "address",
		IsObject: true,
		Props: SelectProperties{
			{Name: "street", IsPrimitive: true},
			{Name: "city", IsPrimitive: true},
		},
	}

	t.Run("match aliases the slice element", func(t *testing.T) {
		res := prop.FindSelectProperty("city")
		require.NotNil(t, res)
		assert.Same(t, &prop.Props[1], res)
	})

	t.Run("miss", func(t *testing.T) {
		assert.Nil(t, prop.FindSelectProperty("zip"))
	})

	t.Run("no nested props", func(t *testing.T) {
		assert.Nil(t, SelectProperty{}.FindSelectProperty("street"))
	})
}

// Lookups only see the first entry for a name, so whatever a repeated
// selection added on top (references, nested properties, additional flags)
// was never resolved.
func TestSelectPropertiesMerged(t *testing.T) {
	ref := func(class string, additionalProps additional.Properties, props ...SelectProperty) SelectClass {
		return SelectClass{ClassName: class, RefProperties: props, AdditionalProperties: additionalProps}
	}
	title := SelectProperty{Name: "title", IsPrimitive: true}
	year := SelectProperty{Name: "year", IsPrimitive: true}

	for _, tc := range []struct {
		name     string
		props    SelectProperties
		expected SelectProperties
	}{
		{name: "nil", props: nil, expected: nil},
		{
			name:     "nothing repeated",
			props:    SelectProperties{title, {Name: "author", Refs: []SelectClass{ref("Person", additional.Properties{}, title)}}},
			expected: SelectProperties{title, {Name: "author", Refs: []SelectClass{ref("Person", additional.Properties{}, title)}}},
		},
		{
			name:     "primitive repeated",
			props:    SelectProperties{title, year, title},
			expected: SelectProperties{title, year},
		},
		{
			name: "reference repeated with different target classes",
			props: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{}, title)}},
				{Name: "ofPublisher", Refs: []SelectClass{ref("Journal", additional.Properties{}, year)}},
			},
			expected: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{
					ref("Publisher", additional.Properties{}, title),
					ref("Journal", additional.Properties{}, year),
				}},
			},
		},
		{
			name: "reference repeated with the same target class",
			props: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{ID: true}, title)}},
				{Name: "ofPublisher", IncludeTypeName: true, Refs: []SelectClass{ref("Publisher", additional.Properties{Distance: true}, year, title)}},
			},
			expected: SelectProperties{
				{Name: "ofPublisher", IncludeTypeName: true, Refs: []SelectClass{
					ref("Publisher", additional.Properties{ID: true, Distance: true}, title, year),
				}},
			},
		},
		{
			name: "same target class twice within one reference",
			props: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{
					ref("Publisher", additional.Properties{}, title),
					ref("Publisher", additional.Properties{}, year),
				}},
			},
			expected: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{}, title, year)}},
			},
		},
		{
			name: "nested reference repeated",
			props: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{},
					SelectProperty{Name: "owner", Refs: []SelectClass{ref("Person", additional.Properties{}, title)}},
				)}},
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{},
					SelectProperty{Name: "owner", Refs: []SelectClass{ref("Person", additional.Properties{}, year)}},
				)}},
			},
			expected: SelectProperties{
				{Name: "ofPublisher", Refs: []SelectClass{ref("Publisher", additional.Properties{},
					SelectProperty{Name: "owner", Refs: []SelectClass{ref("Person", additional.Properties{}, title, year)}},
				)}},
			},
		},
		{
			name: "nested object repeated",
			props: SelectProperties{
				{Name: "address", IsObject: true, Props: SelectProperties{{Name: "city", IsPrimitive: true}}},
				{Name: "address", IsObject: true, Props: SelectProperties{{Name: "zip", IsPrimitive: true}, {Name: "city", IsPrimitive: true}}},
			},
			expected: SelectProperties{
				{Name: "address", IsObject: true, Props: SelectProperties{{Name: "city", IsPrimitive: true}, {Name: "zip", IsPrimitive: true}}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.props.Merged())
		})
	}

	t.Run("input is not mutated", func(t *testing.T) {
		refs := make([]SelectClass, 1, 4)
		refs[0] = ref("Publisher", additional.Properties{}, title)
		props := SelectProperties{
			{Name: "ofPublisher", Refs: refs},
			{Name: "ofPublisher", Refs: []SelectClass{ref("Journal", additional.Properties{}, year)}},
		}

		props.Merged()

		assert.Len(t, props[0].Refs, 1)
		assert.Equal(t, ref("Publisher", additional.Properties{}, title), refs[:2][0])
		assert.Equal(t, SelectClass{}, refs[:2][1], "spare capacity of the input must not be written")
	})
}

func ptrTo[T any](v T) *T {
	return &v
}
