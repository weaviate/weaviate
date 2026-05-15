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
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
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
		stopwordProvider:       stopwords.NewProvider(fakeStopwordDetector{}, nil),
		isFallbackToSearchable: func() bool { return false },
		logger:                 logger,
		nestedBitmapOps:        invnested.NewBitmapOps(roaringset.NewBitmapBufPoolNoop()),
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
				// └── correlated(nested) ← nested.isWithinRootSubtree=true, nested.childrenFromTokenization=true
				//     ├── title:"hello"
				//     └── title:"world"
				require.Equal(t, filters.OperatorAnd, pv.operator)
				assert.True(t, pv.nested.isWithinRootSubtree, "compound AND should be marked nested.isWithinRootSubtree")
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
			name: "root IsNull false — relPath is empty",
			path: "nested", operator: filters.OperatorIsNull,
			valueType: schema.DataTypeBoolean, value: false,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, filters.OperatorIsNull, pv.operator)
				assert.Equal(t, "nested", pv.prop)
				assert.True(t, pv.nested.isNested)
				assert.Equal(t, "", pv.nested.relPath) // root-level existence
				assert.Equal(t, []byte{0x00}, pv.value)
			},
		},
		{
			name: "root IsNull true — relPath is empty, denylist value",
			path: "nested", operator: filters.OperatorIsNull,
			valueType: schema.DataTypeBoolean, value: true,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, filters.OperatorIsNull, pv.operator)
				assert.Equal(t, "nested", pv.prop)
				assert.True(t, pv.nested.isNested)
				assert.Equal(t, "", pv.nested.relPath) // root-level existence
				assert.Equal(t, []byte{0x01}, pv.value)
			},
		},
		{
			name: "IsNull false — produces nested isNull pair",
			path: "nested.city", operator: filters.OperatorIsNull,
			valueType: schema.DataTypeBoolean, value: false,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, filters.OperatorIsNull, pv.operator)
				assert.Equal(t, "nested", pv.prop)
				assert.True(t, pv.nested.isNested)
				assert.Equal(t, "city", pv.nested.relPath)
				assert.Equal(t, []byte{0x00}, pv.value) // false = property exists
			},
		},
		{
			name: "IsNull true — produces nested isNull pair with denylist value",
			path: "nested.city", operator: filters.OperatorIsNull,
			valueType: schema.DataTypeBoolean, value: true,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, filters.OperatorIsNull, pv.operator)
				assert.Equal(t, "nested", pv.prop)
				assert.True(t, pv.nested.isNested)
				assert.Equal(t, "city", pv.nested.relPath)
				assert.Equal(t, []byte{0x01}, pv.value) // true = property absent
			},
		},
		// --- indexed paths: arrayIndices populated, relPath clean ---
		{
			name: "root index — nested[0].city",
			path: "nested[0].city", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Berlin",
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, "nested", pv.prop)
				assert.Equal(t, "city", pv.nested.relPath)
				assert.True(t, pv.nested.isNested)
				require.Len(t, pv.nested.arrayIndices, 1)
				assert.Equal(t, filnested.ArrayIndex{RelPath: "", Index: 0}, pv.nested.arrayIndices[0])
			},
		},
		{
			name: "sub-property index — nested.numbers[1]",
			path: "nested.numbers[1]", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 7,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, "nested", pv.prop)
				assert.Equal(t, "numbers", pv.nested.relPath)
				assert.True(t, pv.nested.isNested)
				require.Len(t, pv.nested.arrayIndices, 1)
				assert.Equal(t, filnested.ArrayIndex{RelPath: "numbers", Index: 1}, pv.nested.arrayIndices[0])
			},
		},
		{
			name: "multi-level index — nested[0].owner.firstname (two-level path, root indexed)",
			path: "nested[0].owner.firstname", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Alice",
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, "nested", pv.prop)
				assert.Equal(t, "owner.firstname", pv.nested.relPath)
				assert.True(t, pv.nested.isNested)
				require.Len(t, pv.nested.arrayIndices, 1)
				assert.Equal(t, filnested.ArrayIndex{RelPath: "", Index: 0}, pv.nested.arrayIndices[0])
			},
		},
		{
			name: "multi-level indexes — nested[0].numbers[2]",
			path: "nested[0].numbers[2]", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 7,
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, "nested", pv.prop)
				assert.Equal(t, "numbers", pv.nested.relPath)
				assert.True(t, pv.nested.isNested)
				require.Len(t, pv.nested.arrayIndices, 2)
				assert.Equal(t, filnested.ArrayIndex{RelPath: "", Index: 0}, pv.nested.arrayIndices[0])
				assert.Equal(t, filnested.ArrayIndex{RelPath: "numbers", Index: 2}, pv.nested.arrayIndices[1])
			},
		},
		{
			name: "no index — arrayIndices empty",
			path: "nested.city", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Berlin",
			verify: func(t *testing.T, pv *propValuePair) {
				assert.Equal(t, "city", pv.nested.relPath)
				assert.Empty(t, pv.nested.arrayIndices)
			},
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
// attention to multi-token text conditions (nested.isWithinRootSubtree + nested.childrenFromTokenization).
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
		// └── correlated(nested)  ← nested.isWithinRootSubtree=true, nested.childrenFromTokenization=true
		//     ├── title:"hello"
		//     └── title:"world"
		clause := makeNestedFilterClause("nested.title", filters.OperatorEqual, schema.DataTypeText, "hello world")
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.True(t, pv.nested.childrenFromTokenization)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assert.Equal(t, []byte("hello"), pv.children[0].value)
		assert.Equal(t, []byte("world"), pv.children[1].value)
	})

	t.Run("multi-token nested text alongside scalar nested condition", func(t *testing.T) {
		// input:  AND(nested.title = "hello world", nested.city = "berlin")
		// output:
		// └── AND {isWithinRootSubtree=true, prop=nested}  ← outer AND collapsed
		//     ├── AND {isWithinRootSubtree=true, fromTok}   (title tokens)
		//     │   ├── title:"hello"
		//     │   └── title:"world"
		//     └── city:"berlin"
		//
		// The outer AND has both same-root children landing in one group, so
		// groupNestedSubtrees promotes the outer AND in place rather than producing
		// a redundant AND-with-single-wrapper-child layer.
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(leaf("nested.title", "hello world"), leaf("nested.city", "berlin")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "outer AND collapsed into the same-root wrapper")
		assert.False(t, pv.nested.childrenFromTokenization)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		// first child: the tokenization compound AND for "hello world"
		tokenAnd := pv.children[0]
		assert.True(t, tokenAnd.nested.isWithinRootSubtree)
		assert.True(t, tokenAnd.nested.childrenFromTokenization)
		assert.Equal(t, filters.OperatorAnd, tokenAnd.operator)
		require.Len(t, tokenAnd.children, 2)
		assert.Equal(t, []byte("hello"), tokenAnd.children[0].value)
		assert.Equal(t, []byte("world"), tokenAnd.children[1].value)

		// second child: the scalar city leaf
		cityLeaf := pv.children[1]
		assert.True(t, cityLeaf.nested.isNested)
		assert.False(t, cityLeaf.nested.isWithinRootSubtree)
		assert.Equal(t, []byte("berlin"), cityLeaf.value)
		assert.Equal(t, "city", cityLeaf.nested.relPath)
	})

	orClause := func(operands ...filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorOr, Operands: operands}
	}
	notClause := func(operand filters.Clause) *filters.Clause {
		return &filters.Clause{Operator: filters.OperatorNot, Operands: []filters.Clause{operand}}
	}
	intLeaf := func(path string, value int) filters.Clause {
		return *makeNestedFilterClause(path, filters.OperatorEqual, schema.DataTypeInt, value)
	}

	// assertNestedLeaf asserts the pvp is a nested leaf at relPath with value.
	assertNestedLeaf := func(t *testing.T, pv *propValuePair, relPath string, value []byte) {
		t.Helper()
		assert.True(t, pv.nested.isNested, "expected nested leaf")
		assert.False(t, pv.nested.isWithinRootSubtree, "leaf should not be marked as wrapper")
		assert.Equal(t, relPath, pv.nested.relPath)
		assert.Equal(t, value, pv.value)
	}

	t.Run("AND of two same-root scalars collapses outer AND", func(t *testing.T) {
		// input:  AND(nested.city=berlin, nested.title=hello)
		// output (collapsed — no redundant outer AND level):
		// └── AND {isWRS:nested}
		//     ├── city:berlin
		//     └── title:hello  (single token — no tokenization wrapper)
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(leaf("nested.city", "berlin"), leaf("nested.title", "hello")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))
		assertNestedLeaf(t, pv.children[1], "title", []byte("hello"))
	})

	t.Run("AND with OR(same-root) inside wraps both levels", func(t *testing.T) {
		// input:  AND(nested.city=berlin, OR(nested.title=alpha, nested.title=beta))
		// output (outer AND collapses; inner OR also collapses to a same-
		// root wrapper so the planner evaluates the OR per-element at the
		// nested LCA):
		// └── AND {isWRS:nested}
		//     ├── city:berlin
		//     └── OR {isWRS:nested}     ← inner OR wraps under OR-of-same-root wrapping
		//         ├── title:alpha
		//         └── title:beta
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(
				leaf("nested.city", "berlin"),
				*orClause(leaf("nested.title", "alpha"), leaf("nested.title", "beta")),
			),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))

		orChild := pv.children[1]
		assert.Equal(t, filters.OperatorOr, orChild.operator)
		assert.True(t, orChild.nested.isWithinRootSubtree, "inner OR same-root wraps")
		assert.Equal(t, "nested", orChild.prop)
		require.Len(t, orChild.children, 2)
		assertNestedLeaf(t, orChild.children[0], "title", []byte("alpha"))
		assertNestedLeaf(t, orChild.children[1], "title", []byte("beta"))
	})

	t.Run("parenthesized AND nested in same-root AND wraps both levels", func(t *testing.T) {
		// input:  AND(nested.city=berlin, AND(nested.title=alpha, nested.count=1))
		// output (outer AND collapses; inner AND also collapses):
		// └── AND {isWRS:nested}
		//     ├── city:berlin
		//     └── AND {isWRS:nested}     ← inner AND collapsed; flattenAndOperators
		//         ├── title:alpha       removes this layer at plan time
		//         └── count:1
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(
				leaf("nested.city", "berlin"),
				*andClause(leaf("nested.title", "alpha"), intLeaf("nested.count", 1)),
			),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))

		innerAnd := pv.children[1]
		assert.Equal(t, filters.OperatorAnd, innerAnd.operator)
		assert.True(t, innerAnd.nested.isWithinRootSubtree, "inner AND also collapsed")
		assert.Equal(t, "nested", innerAnd.prop)
		require.Len(t, innerAnd.children, 2)
		assertNestedLeaf(t, innerAnd.children[0], "title", []byte("alpha"))
		assert.True(t, innerAnd.children[1].nested.isNested)
		assert.Equal(t, "count", innerAnd.children[1].nested.relPath)
	})

	t.Run("OR(AND(A,B), leaf) — outer OR and inner AND both wrap", func(t *testing.T) {
		// input:  OR(AND(nested.city=berlin, nested.title=alpha), nested.title=beta)
		// output (both levels collapse; outer OR also wraps under OR-of-same-root wrapping):
		// └── OR {isWRS:nested}
		//     ├── AND {isWRS:nested}  ← inner AND collapsed
		//     │   ├── city:berlin
		//     │   └── title:alpha
		//     └── title:beta
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(
				*andClause(leaf("nested.city", "berlin"), leaf("nested.title", "alpha")),
				leaf("nested.title", "beta"),
			),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "outer OR same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		innerAnd := pv.children[0]
		assert.Equal(t, filters.OperatorAnd, innerAnd.operator)
		assert.True(t, innerAnd.nested.isWithinRootSubtree, "inner AND collapsed into a wrapper")
		assert.Equal(t, "nested", innerAnd.prop)
		require.Len(t, innerAnd.children, 2)
		assertNestedLeaf(t, innerAnd.children[0], "city", []byte("berlin"))
		assertNestedLeaf(t, innerAnd.children[1], "title", []byte("alpha"))

		assertNestedLeaf(t, pv.children[1], "title", []byte("beta"))
	})

	t.Run("top-level ContainsAll on int[] collapses to same-root wrapper", func(t *testing.T) {
		// input:  ContainsAll(nested.numbers, [1, 2])
		// output (Route 1: operator identity preserved; same-root wrapping
		// still collapses since ContainsAll is treated as an AND alias by
		// groupNestedSubtrees / planner):
		// └── ContainsAll {isWRS:nested}
		//     ├── numbers=1
		//     └── numbers=2
		clause := &filters.Clause{
			Operator: filters.ContainsAll,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1, 2}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.ContainsAll, pv.operator,
			"nested ContainsAll preserves operator identity (Route 1)")
		assert.True(t, pv.nested.isWithinRootSubtree, "ContainsAll same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assert.True(t, pv.children[0].nested.isNested)
		assert.Equal(t, "numbers", pv.children[0].nested.relPath)
	})

	t.Run("top-level ContainsAny collapses to same-root OR wrapper", func(t *testing.T) {
		// input:  ContainsAny(nested.numbers, [1, 2])
		// output (Route 1: operator identity preserved; same-root wrapping
		// still collapses since ContainsAny is treated as an OR alias by
		// groupNestedSubtrees / planner):
		// └── ContainsAny {isWRS:nested}
		//     ├── numbers=1
		//     └── numbers=2
		clause := &filters.Clause{
			Operator: filters.ContainsAny,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1, 2}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.ContainsAny, pv.operator,
			"nested ContainsAny preserves operator identity (Route 1)")
		assert.True(t, pv.nested.isWithinRootSubtree, "ContainsAny same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assert.True(t, pv.children[0].nested.isNested)
	})

	t.Run("single-value nested ContainsAll collapses to bare leaf", func(t *testing.T) {
		// input:  ContainsAll(nested.numbers, [1])
		// output (single-value Contains ≡ Equal — return the leaf directly
		// so resolveDocIDs sees a nested leaf, not an unbacked compound):
		// └── numbers=1
		clause := &filters.Clause{
			Operator: filters.ContainsAll,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorEqual, pv.operator,
			"single-value Contains collapses to the bare Equal leaf")
		assert.True(t, pv.nested.isNested)
		assert.Equal(t, "nested", pv.prop)
		assert.Equal(t, "numbers", pv.nested.relPath)
	})

	t.Run("single-value nested ContainsAny collapses to bare leaf", func(t *testing.T) {
		// input:  ContainsAny(nested.numbers, [1])
		// output: same shape as ContainsAll single-value.
		clause := &filters.Clause{
			Operator: filters.ContainsAny,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorEqual, pv.operator,
			"single-value Contains collapses to the bare Equal leaf")
		assert.True(t, pv.nested.isNested)
		assert.Equal(t, "nested", pv.prop)
		assert.Equal(t, "numbers", pv.nested.relPath)
	})

	t.Run("top-level nested ContainsNone is a first-class operator", func(t *testing.T) {
		// input:  ContainsNone(nested.numbers, [1, 2])
		// output (Route 1: no desugar to NOT(OR), single ContainsNone pvp
		// carrying the operand relPath and the per-value children):
		// └── ContainsNone {prop:nested, relPath:numbers}
		//     ├── numbers=1
		//     └── numbers=2
		clause := &filters.Clause{
			Operator: filters.ContainsNone,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1, 2}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
		require.NoError(t, err)

		assert.Equal(t, filters.ContainsNone, pv.operator,
			"nested ContainsNone preserves operator identity (Route 1)")
		assert.Equal(t, "nested", pv.prop)
		assert.Equal(t, "numbers", pv.nested.relPath,
			"operand path carried on the wrapper so the resolver / planner can read _exists.numbers as universe")
		assert.False(t, pv.nested.isNested, "compound operator node, not a leaf")
		assert.False(t, pv.nested.isWithinRootSubtree, "not a same-root AND group")
		require.Len(t, pv.children, 2, "one child per forbidden value")
		for _, child := range pv.children {
			assert.True(t, child.nested.isNested)
			assert.Equal(t, "numbers", child.nested.relPath)
		}
	})

	t.Run("ContainsAny inside AND wraps both levels", func(t *testing.T) {
		// input:  AND(nested.city=berlin, ContainsAny(nested.numbers, [1, 2]))
		// output (outer AND collapses; inner ContainsAny also collapses
		// under sub-rule 1 — both same-root wrappers, ContainsAny keeps
		// its operator identity under Route 1):
		// └── AND {isWRS:nested}
		//     ├── city:berlin
		//     └── ContainsAny {isWRS:nested}
		//         ├── numbers=1
		//         └── numbers=2
		containsAny := filters.Clause{
			Operator: filters.ContainsAny,
			Value:    &filters.Value{Type: schema.DataTypeIntArray, Value: []int{1, 2}},
			On:       &filters.Path{Class: "Article", Property: "nested.numbers"},
		}
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(leaf("nested.city", "berlin"), containsAny),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))

		anyChild := pv.children[1]
		assert.Equal(t, filters.ContainsAny, anyChild.operator,
			"inner ContainsAny preserves operator identity (Route 1)")
		assert.True(t, anyChild.nested.isWithinRootSubtree, "inner ContainsAny same-root wraps")
		assert.Equal(t, "nested", anyChild.prop)
		require.Len(t, anyChild.children, 2)
	})

	t.Run("standalone OR of same-root leaves wraps and collapses", func(t *testing.T) {
		// input:  OR(nested.city=berlin, nested.title=alpha)
		// output (OR-of-same-root wrapping: OR of same-root children wraps with
		// isWithinRootSubtree=true, then collapses in place):
		// └── OR {isWRS:nested}
		//     ├── city:berlin
		//     └── title:alpha
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(leaf("nested.city", "berlin"), leaf("nested.title", "alpha")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "OR same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))
		assertNestedLeaf(t, pv.children[1], "title", []byte("alpha"))
	})

	t.Run("standalone NOT of nested leaf wraps under top-level NOT wrapping", func(t *testing.T) {
		// input:  NOT(nested.city=berlin)
		// output (top-level NOT wrapping: standalone NOT with a nested-rooted
		// operand wraps so the planner inverts at the operand's LCA
		// per-element):
		// └── NOT {isWRS:nested}
		//     └── city:berlin
		pv, err := s.extractPropValuePair(t.Context(),
			notClause(leaf("nested.city", "berlin")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorNot, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "NOT same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 1)
		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))
	})

	t.Run("AND with NOT(leaf) inside — outer AND and inner NOT both wrap", func(t *testing.T) {
		// input:  AND(nested.city=berlin, NOT(nested.title=alpha))
		// output (outer AND collapses under same-root grouping; inner
		// NOT also wraps under top-level NOT wrapping):
		// └── AND {isWRS:nested}
		//     ├── city:berlin
		//     └── NOT {isWRS:nested}    ← inner NOT also wraps
		//         └── title:alpha
		pv, err := s.extractPropValuePair(t.Context(),
			andClause(leaf("nested.city", "berlin"), *notClause(leaf("nested.title", "alpha"))),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorAnd, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree)
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)

		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))

		notChild := pv.children[1]
		assert.Equal(t, filters.OperatorNot, notChild.operator)
		assert.True(t, notChild.nested.isWithinRootSubtree, "inner NOT wraps under top-level NOT wrapping")
		assert.Equal(t, "nested", notChild.prop)
		require.Len(t, notChild.children, 1)
		assertNestedLeaf(t, notChild.children[0], "title", []byte("alpha"))
	})

	// --- OR-of-same-root wrapping (OR grouping) positive cases ---

	t.Run("OR with leaf + NOT(same-root) — both OR and inner NOT wrap", func(t *testing.T) {
		// input:  OR(nested.city=berlin, NOT(nested.title=alpha))
		// output (OR-of-same-root wrapping wraps the OR; top-level NOT wrapping wraps the inner
		// NOT):
		// └── OR {isWRS:nested}
		//     ├── city:berlin
		//     └── NOT {isWRS:nested}
		//         └── title:alpha
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(leaf("nested.city", "berlin"), *notClause(leaf("nested.title", "alpha"))),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "OR same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))

		notChild := pv.children[1]
		assert.Equal(t, filters.OperatorNot, notChild.operator)
		assert.True(t, notChild.nested.isWithinRootSubtree, "inner NOT wraps under top-level NOT wrapping")
		assert.Equal(t, "nested", notChild.prop)
		require.Len(t, notChild.children, 1)
		assertNestedLeaf(t, notChild.children[0], "title", []byte("alpha"))
	})

	t.Run("OR of two NOTs at same root — OR and both NOTs wrap", func(t *testing.T) {
		// input:  OR(NOT(nested.city=berlin), NOT(nested.title=alpha))
		// output (OR-of-same-root wrapping wraps the OR; top-level NOT wrapping wraps each NOT):
		// └── OR {isWRS:nested}
		//     ├── NOT {isWRS:nested}
		//     │   └── city:berlin
		//     └── NOT {isWRS:nested}
		//         └── title:alpha
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(
				*notClause(leaf("nested.city", "berlin")),
				*notClause(leaf("nested.title", "alpha")),
			),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "OR of same-root NOTs wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 2)
		for i, child := range pv.children {
			assert.Equal(t, filters.OperatorNot, child.operator, "child[%d] should be NOT", i)
			assert.True(t, child.nested.isWithinRootSubtree, "child[%d] NOT wraps under top-level NOT wrapping", i)
			assert.Equal(t, "nested", child.prop, "child[%d] prop", i)
		}
	})

	t.Run("OR of three same-root leaves wraps", func(t *testing.T) {
		// input:  OR(nested.city=berlin, nested.title=alpha, nested.count=1)
		// Three siblings, all root=nested → single OR wrapper.
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(
				leaf("nested.city", "berlin"),
				leaf("nested.title", "alpha"),
				intLeaf("nested.count", 1),
			),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "three-way OR same-root wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 3)
	})

	// --- top-level NOT wrapping (top-level NOT marking) positive cases ---

	t.Run("NOT of nested AND wraps both levels under top-level NOT wrapping", func(t *testing.T) {
		// input:  NOT(AND(nested.city=berlin, nested.title=alpha))
		// output (inner AND collapses under same-root grouping; outer
		// NOT wraps under top-level NOT wrapping):
		// └── NOT {isWRS:nested}
		//     └── AND {isWRS:nested}
		//         ├── city:berlin
		//         └── title:alpha
		pv, err := s.extractPropValuePair(t.Context(),
			notClause(*andClause(leaf("nested.city", "berlin"), leaf("nested.title", "alpha"))),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorNot, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "outer NOT wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 1)

		innerAnd := pv.children[0]
		assert.Equal(t, filters.OperatorAnd, innerAnd.operator)
		assert.True(t, innerAnd.nested.isWithinRootSubtree, "inner AND wraps under OR-of-same-root wrapping")
		require.Len(t, innerAnd.children, 2)
	})

	t.Run("NOT of nested OR wraps both levels under top-level NOT wrapping", func(t *testing.T) {
		// input:  NOT(OR(nested.city=berlin, nested.title=alpha))
		// output: both wrap.
		pv, err := s.extractPropValuePair(t.Context(),
			notClause(*orClause(leaf("nested.city", "berlin"), leaf("nested.title", "alpha"))),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorNot, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "outer NOT wraps")
		assert.Equal(t, "nested", pv.prop)
		require.Len(t, pv.children, 1)

		innerOr := pv.children[0]
		assert.Equal(t, filters.OperatorOr, innerOr.operator)
		assert.True(t, innerOr.nested.isWithinRootSubtree, "inner OR wraps under OR-of-same-root wrapping")
		require.Len(t, innerOr.children, 2)
	})

	t.Run("NOT(NOT) — both NOTs wrap (double negation)", func(t *testing.T) {
		// input:  NOT(NOT(nested.city=berlin))
		// output: both wrap; the planner double-inverts giving the
		// original leaf semantic at the LCA.
		pv, err := s.extractPropValuePair(t.Context(),
			notClause(*notClause(leaf("nested.city", "berlin"))),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorNot, pv.operator)
		assert.True(t, pv.nested.isWithinRootSubtree, "outer NOT wraps")
		require.Len(t, pv.children, 1)

		innerNot := pv.children[0]
		assert.Equal(t, filters.OperatorNot, innerNot.operator)
		assert.True(t, innerNot.nested.isWithinRootSubtree, "inner NOT wraps")
		require.Len(t, innerNot.children, 1)
		assertNestedLeaf(t, innerNot.children[0], "city", []byte("berlin"))
	})

	t.Run("NOT of IsNull leaf — DeMorgan rewrite to flipped IsNull (singleton-NOT/OR wrapping)", func(t *testing.T) {
		// input:  NOT(nested.city IS NULL=true)
		// singleton-NOT/OR wrapping rewrites NOT(IsNull=v) → IsNull=!v at
		// extraction time. The NOT is eliminated and the IsNull leaf
		// returned in its place with the value byte flipped.
		isNullClause := &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: true},
			On:       &filters.Path{Class: "Article", Property: "nested.city"},
		}
		pv, err := s.extractPropValuePair(t.Context(),
			notClause(*isNullClause),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorIsNull, pv.operator, "NOT eliminated; IsNull returned in its place")
		assert.True(t, pv.nested.isNested, "leaf preserved as nested")
		require.NotEmpty(t, pv.value)
		assert.Equal(t, byte(0x00), pv.value[0], "IsNull=true flipped to IsNull=false")
	})

	t.Run("NOT of flat-property leaf is not wrapped", func(t *testing.T) {
		// input:  NOT(flatProperty = ...)  — operand has no nested root.
		// nestedRootProp returns "" → NOT stays unwrapped → today's
		// docID-level dispatch.
		//
		// Test schema doesn't define a top-level flat property, so we
		// simulate by directly constructing a pvp where the operand
		// looks flat (no isNested). The grouping pass should leave the
		// outer NOT alone.
		flat := &propValuePair{prop: "title", operator: filters.OperatorEqual}
		pv := &propValuePair{
			operator: filters.OperatorNot,
			children: []*propValuePair{flat},
		}
		class := nestedSearcherClass()
		out := groupNestedSubtrees(pv, class)
		assert.Equal(t, filters.OperatorNot, out.operator)
		assert.False(t, out.nested.isWithinRootSubtree, "flat-operand NOT stays unwrapped")
	})

	t.Run("OR of single nested leaf is not wrapped (singleton group)", func(t *testing.T) {
		// input:  OR(nested.city=berlin) — single child
		// Even though the operand is nested, a single child means no
		// grouping is needed; the OR passes through unchanged.
		pv, err := s.extractPropValuePair(t.Context(),
			orClause(leaf("nested.city", "berlin")),
			"Article")
		require.NoError(t, err)

		assert.Equal(t, filters.OperatorOr, pv.operator)
		assert.False(t, pv.nested.isWithinRootSubtree, "singleton OR not wrapped")
		require.Len(t, pv.children, 1)
		assertNestedLeaf(t, pv.children[0], "city", []byte("berlin"))
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
		// indexed paths: [N] stripped for schema lookup, arrayIndices populated
		{
			name: "root index routes to nested — nested[0].city",
			path: "nested[0].city", operator: filters.OperatorEqual,
			valueType: schema.DataTypeText, value: "Berlin",
			wantProp: "nested", wantNested: true,
		},
		{
			name: "sub-property index routes to nested — nested.numbers[2]",
			path: "nested.numbers[2]", operator: filters.OperatorEqual,
			valueType: schema.DataTypeInt, value: 7,
			wantProp: "nested", wantNested: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := makeNestedFilterClause(tt.path, tt.operator, tt.valueType, tt.value)
			pv, err := s.extractPropValuePair(t.Context(), clause, "Article")
			require.NoError(t, err)

			assert.Equal(t, tt.wantProp, pv.prop)
			assert.Equal(t, tt.wantNested, pv.nested.isNested)
			assert.Equal(t, tt.operator, pv.operator)
			// relPath must not contain any [N] markers
			assert.NotContains(t, pv.nested.relPath, "[")
		})
	}
}
