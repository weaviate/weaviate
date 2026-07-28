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

func ptrTo[T any](v T) *T {
	return &v
}
