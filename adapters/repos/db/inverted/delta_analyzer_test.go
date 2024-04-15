//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeltaAnalyzer(t *testing.T) {
	t.Run("without previous indexing", func(t *testing.T) {
		previous := []Property(nil)
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}

		res := Delta(previous, next)
		assert.Equal(t, next, res.ToAdd)
		assert.Len(t, res.ToDelete, 0)
	})

	t.Run("with previous indexing and no changes", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}

		res := Delta(previous, next)
		assert.Len(t, res.ToDelete, 0)
		assert.Len(t, res.ToAdd, 0)
	})

	t.Run("with previous indexing - only additions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}

		expectedAdd := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
				},
			},
		}

		res := Delta(previous, next)
		assert.Equal(t, expectedAdd, res.ToAdd)
		assert.Len(t, res.ToDelete, 0)
	})

	t.Run("with previous indexing - both additions and deletions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}

		expectedAdd := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
				},
			},
		}

		expectedDelete := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
		}

		res := Delta(previous, next)
		assert.Equal(t, expectedAdd, res.ToAdd)
		assert.Equal(t, expectedDelete, res.ToDelete)
	})
}

func TestDeltaAnalyzer_Arrays(t *testing.T) {
	lexInt64 := func(val int64) []byte {
		bytes, _ := LexicographicallySortableInt64(val)
		return bytes
	}
	lexBool := func(val bool) []byte {
		if val {
			return []uint8{1}
		}
		return []uint8{0}
	}

	t.Run("with previous indexing - both additions and deletions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(102)},
					{Data: lexInt64(103)},
					{Data: lexInt64(104)},
				},
				Length:             9,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "booleans",
				Items: []Countable{
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(false)},
				},
				Length:             4,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "numbers",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "texts",
				Items: []Countable{
					{Data: []byte("aaa")},
					{Data: []byte("bbb")},
					{Data: []byte("ccc")},
				},
				Length:             3,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "dates",
				Items: []Countable{
					{Data: []byte("2021-06-01T22:18:59.640162Z")},
					{Data: []byte("2022-06-01T22:18:59.640162Z")},
				},
				Length:             2,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_creationTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}
		next := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(103)},
					{Data: lexInt64(104)},
					{Data: lexInt64(105)},
				},
				Length:             7,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "booleans",
				Items: []Countable{
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(false)},
				},
				Length:             4,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "texts",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_creationTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778500000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}

		expectedAdd := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(105)},
				},
				Length:             7,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "texts",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778500000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}
		expectedDelete := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(102)},
				},
				Length:             9,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "texts",
				Items: []Countable{
					{Data: []byte("aaa")},
					{Data: []byte("bbb")},
					{Data: []byte("ccc")},
				},
				Length:             3,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "numbers",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "dates",
				Items: []Countable{
					{Data: []byte("2021-06-01T22:18:59.640162Z")},
					{Data: []byte("2022-06-01T22:18:59.640162Z")},
				},
				Length:             2,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}

		delta := Delta(previous, next)
		assert.Equal(t, expectedAdd, delta.ToAdd)
		assert.Equal(t, expectedDelete, delta.ToDelete)
	})
}

func TestDeltaNilAnalyzer(t *testing.T) {
	previous := []NilProperty{
		{
			Name:                "ints",
			AddToPropertyLength: false,
		},
		{
			Name:                "booleans",
			AddToPropertyLength: true,
		},
		{
			Name:                "numbers",
			AddToPropertyLength: true,
		},
	}
	next := []NilProperty{
		{
			Name:                "booleans",
			AddToPropertyLength: true,
		},
		{
			Name:                "texts",
			AddToPropertyLength: true,
		},
		{
			Name:                "dates",
			AddToPropertyLength: false,
		},
	}

	expectedAdd := []NilProperty{
		{
			Name:                "texts",
			AddToPropertyLength: true,
		},
		{
			Name:                "dates",
			AddToPropertyLength: false,
		},
	}
	expectedDelete := []NilProperty{
		{
			Name:                "ints",
			AddToPropertyLength: false,
		},
		{
			Name:                "numbers",
			AddToPropertyLength: true,
		},
	}

	deltaNil := DeltaNil(previous, next)
	assert.Equal(t, expectedAdd, deltaNil.ToAdd)
	assert.Equal(t, expectedDelete, deltaNil.ToDelete)
}
