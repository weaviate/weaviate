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
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// nestedSearcherClass is the test schema used across all extractNestedProp tests:
//
//	Article {
//	  nested: object {
//	    city:    text  (field tokenization, filterable)
//	    title:   text  (word tokenization, filterable)
//	    count:   int   (filterable)
//	    numbers: int[] (filterable)
//	    skipped: text  (filterable=false)
//	    owner: object {
//	      firstname: text (field tokenization, filterable)
//	    }
//	  }
//	}
func nestedSearcherClass() *models.Class {
	f := func(v bool) *bool { return &v }
	return &models.Class{
		Class: "Article",
		Properties: []*models.Property{
			{
				Name:     "nested",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: f(true)},
					{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: f(true)},
					{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: f(true)},
					{Name: "numbers", DataType: schema.DataTypeIntArray.PropString(), IndexFilterable: f(true)},
					{Name: "skipped", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: f(false)},
					{
						Name:     "owner",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: f(true)},
						},
					},
				},
			},
		},
	}
}

func newTestSearcher() *Searcher {
	class := nestedSearcherClass()
	logger, _ := test.NewNullLogger()
	return &Searcher{
		getClass:               func(name string) *models.Class { return class },
		stopwords:              fakeStopwordDetector{},
		isFallbackToSearchable: func() bool { return false },
		logger:                 logger,
	}
}

func makeNestedFilterClause(propName string, operator filters.Operator, valueType schema.DataType, value any) *filters.Clause {
	return &filters.Clause{
		Operator: operator,
		Value:    &filters.Value{Type: valueType, Value: value},
		On:       &filters.Path{Class: "Article", Property: schema.PropertyName(propName)},
	}
}

func TestExtractNestedProp(t *testing.T) {
	s := newTestSearcher()
	class := nestedSearcherClass()
	prop := class.Properties[0] // "nested"

	intVal42, err := s.extractIntValue(42)
	require.NoError(t, err)
	intVal7, err := s.extractIntValue(7)
	require.NoError(t, err)

	tests := []struct {
		name      string
		path      string
		operator  filters.Operator
		valueType schema.DataType
		value     any
		// expected success fields
		wantProp  string
		wantValue []byte
		// optional extra check for complex cases (e.g. multi-token AND)
		verify  func(t *testing.T, pv *propValuePair)
		wantErr string
	}{
		{
			name: "text equal single token field tokenization",
			path: "nested.city", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Berlin",
			wantProp: "nested", wantValue: []byte("Berlin"),
		},
		{
			name: "text equal two tokens word tokenization produces AND children",
			path: "nested.title", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "hello world",
			verify: func(t *testing.T, pv *propValuePair) {
				// output:
				// └── correlated(nested) ← nested.isCorrelated=true, nested.childrenFromTokenization=true
				//     ├── title:"hello"
				//     └── title:"world"
				require.Equal(t, filters.OperatorAnd, pv.operator)
				assert.True(t, pv.nested.isCorrelated, "compound AND should be marked nested.isCorrelated")
				assert.True(t, pv.nested.childrenFromTokenization, "compound AND should be marked nested.childrenFromTokenization")
				assert.Equal(t, "nested", pv.prop)
				require.Len(t, pv.children, 2)
				for _, child := range pv.children {
					assert.Equal(t, "nested", child.prop)
					assert.Equal(t, "title", child.nested.relPath)
					assert.True(t, child.nested.isNested)
					assert.True(t, child.hasFilterableIndex)
				}
				assert.Equal(t, []byte("hello"), pv.children[0].value)
				assert.Equal(t, []byte("world"), pv.children[1].value)
			},
		},
		{
			name: "text like pattern passed as-is",
			path: "nested.city", operator: filters.OperatorLike,
			valueType: schema.DataTypeText, value: "Ber*",
			wantProp: "nested", wantValue: []byte("Ber*"),
		},
		{
			name: "int equal",
			path: "nested.count", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 42,
			wantProp: "nested", wantValue: intVal42,
		},
		{
			name: "int array mapped to scalar int encoding",
			path: "nested.numbers", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 7,
			wantProp: "nested", wantValue: intVal7,
		},
		{
			name: "two-level path",
			path: "nested.owner.firstname", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Alice",
			wantProp: "nested", wantValue: []byte("Alice"),
		},
		{
			name: "IsNull returns error",
			path: "nested.city", operator: filters.OperatorIsNull,
			valueType: schema.DataTypeBoolean, value: true,
			wantErr: "IsNull",
		},
		{
			name: "non-filterable leaf returns error",
			path: "nested.skipped", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "x",
			wantErr: "not filterable",
		},
		{
			name: "non-existent sub-property returns error",
			path: "nested.missing", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "x",
			wantErr: `"missing" not found`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := makeNestedFilterClause(tt.path, tt.operator, tt.valueType, tt.value)
			pv, err := s.extractNestedProp(clause, tt.path, prop, class)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			if tt.verify != nil {
				tt.verify(t, pv)
				return
			}
			relativePath := tt.path[strings.Index(tt.path, ".")+1:]
			assert.Equal(t, tt.wantProp, pv.prop)
			assert.Equal(t, relativePath, pv.nested.relPath)
			assert.True(t, pv.nested.isNested)
			assert.True(t, pv.hasFilterableIndex)
			if tt.wantValue != nil {
				assert.Equal(t, tt.wantValue, pv.value)
			}
		})
	}
}

// TestExtractPropValuePairNestedGrouping verifies that extractPropValuePair
// correctly groups nested AND children via groupNestedByProp, with special
// attention to multi-token text conditions (nested.isCorrelated + nested.childrenFromTokenization).
func TestExtractPropValuePairNestedGrouping(t *testing.T) {
	s := newTestSearcher()

	andClause := func(operands ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}
	}
	leaf := func(path string, value string) filters.Clause {
		return *makeNestedFilterClause(path, filters.OperatorEqual, schema.DataTypeText, value)
	}

	t.Run("standalone multi-token nested text", func(t *testing.T) {
		// input:  nested.title = "hello world"
		// output:
		// └── correlated(nested)  ← nested.isCorrelated=true, nested.childrenFromTokenization=true
		//     ├── title:"hello"
		//     └── title:"world"
		clause := makeNestedFilterClause("nested.title", filters.OperatorEqual, schema.DataTypeText, "hello world")
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isCorrelated)
		assert.True(t, pv.nested.childrenFromTokenization)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assert.Equal(t, []byte("hello"), pv.children[0].value)
		assert.Equal(t, []byte("world"), pv.children[1].value)
	})

	t.Run("multi-token nested text alongside scalar nested condition", func(t *testing.T) {
		// input:  AND(nested.title = "hello world", nested.city = "berlin")
		// output:
		// └── correlated(nested)        ← nested.isCorrelated=true, nested.childrenFromTokenization=false
		//     ├── correlated(nested)    ← nested.isCorrelated=true, nested.childrenFromTokenization=true (title tokens)
		//     │   ├── title:"hello"
		//     │   └── title:"world"
		//     └── city:"berlin"
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(leaf("nested.title", "hello world"), leaf("nested.city", "berlin")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		require.Len(t, pv.children, 1, "both conditions grouped under one correlated(nested) node")

		group := pv.children[0]
		assert.True(t, group.nested.isCorrelated)
		assert.False(t, group.nested.childrenFromTokenization)
		assert.Equal(t, "nested", group.prop)
		require.Len(t, group.children, 2)

		// first child: the tokenization compound AND for "hello world"
		tokenAnd := group.children[0]
		assert.True(t, tokenAnd.nested.isCorrelated)
		assert.True(t, tokenAnd.nested.childrenFromTokenization)
		assert.Equal(t, filters.OperatorAnd, tokenAnd.operator)
		require.Len(t, tokenAnd.children, 2)
		assert.Equal(t, []byte("hello"), tokenAnd.children[0].value)
		assert.Equal(t, []byte("world"), tokenAnd.children[1].value)

		// second child: the scalar city leaf
		cityLeaf := group.children[1]
		assert.True(t, cityLeaf.nested.isNested)
		assert.False(t, cityLeaf.nested.isCorrelated)
		assert.Equal(t, []byte("berlin"), cityLeaf.value)
		assert.Equal(t, "city", cityLeaf.nested.relPath)
	})
}

// TestExtractPropValuePairNestedRouting verifies that extractPropValuePair
// correctly detects nested paths and delegates to extractNestedProp,
// producing the same propValuePair as a direct call would.
func TestExtractPropValuePairNestedRouting(t *testing.T) {
	s := newTestSearcher()

	tests := []struct {
		name       string
		path       string
		operator   filters.Operator
		valueType  schema.DataType
		value      any
		wantProp   string // expected pv.prop (top-level name)
		wantNested bool
	}{
		{
			name: "text equal routes to nested",
			path: "nested.city", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Berlin",
			wantProp: "nested", wantNested: true,
		},
		{
			name: "int equal routes to nested",
			path: "nested.count", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 42,
			wantProp: "nested", wantNested: true,
		},
		{
			name: "two-level path routes to nested",
			path: "nested.owner.firstname", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Alice",
			wantProp: "nested", wantNested: true,
		},
		{
			name: "like routes to nested",
			path: "nested.city", operator: filters.OperatorLike,
			valueType: schema.DataTypeText, value: "Ber*",
			wantProp: "nested", wantNested: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := makeNestedFilterClause(tt.path, tt.operator, tt.valueType, tt.value)
			pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
			require.NoError(t, err)

			relativePath := tt.path[strings.Index(tt.path, ".")+1:]
			assert.Equal(t, tt.wantProp, pv.prop)
			assert.Equal(t, tt.wantNested, pv.nested.isNested)
			assert.Equal(t, relativePath, pv.nested.relPath)
			assert.Equal(t, tt.operator, pv.operator)
		})
	}
}
