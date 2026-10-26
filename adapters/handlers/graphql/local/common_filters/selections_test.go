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

package common_filters

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-platform/graphql/language/ast"
	"github.com/tailor-platform/graphql/language/parser"
)

func TestSelectedFields(t *testing.T) {
	// each fragment spreads its predecessor twice: naive expansion yields
	// 2^19 copies of "a" from a query of a few hundred bytes
	var doubling strings.Builder
	doubling.WriteString("{ ...F19 } fragment F0 on T { a } ")
	for i := 1; i <= 19; i++ {
		fmt.Fprintf(&doubling, "fragment F%d on T { ...F%d ...F%d } ", i, i-1, i-1)
	}

	for _, tc := range []struct {
		name      string
		query     string
		variables map[string]interface{}
		expected  []string
	}{
		{name: "fields", query: "{ a b }", expected: []string{"a", "b"}},
		{name: "inline fragment", query: "{ a ... on T { b } }", expected: []string{"a", "b"}},
		{name: "named fragment", query: "{ a ...F } fragment F on T { b }", expected: []string{"a", "b"}},
		{name: "nested fragments", query: "{ ...F } fragment F on T { a ... on T { ...G } } fragment G on T { b }", expected: []string{"a", "b"}},
		{name: "repeated spread expands once", query: "{ ...F a ...F } fragment F on T { b }", expected: []string{"b", "a"}},
		{name: "repeated acyclic spreads expand once", query: doubling.String(), expected: []string{"a"}},
		{name: "skip true on field", query: "{ a b @skip(if: true) }", expected: []string{"a"}},
		{name: "skip false on field", query: "{ a b @skip(if: false) }", expected: []string{"a", "b"}},
		{name: "include false on field", query: "{ a b @include(if: false) }", expected: []string{"a"}},
		{name: "include true on field", query: "{ a b @include(if: true) }", expected: []string{"a", "b"}},
		{name: "skip wins over include", query: "{ a b @skip(if: true) @include(if: true) }", expected: []string{"a"}},
		{name: "skip on inline fragment", query: "{ a ... on T @skip(if: true) { b } }", expected: []string{"a"}},
		{name: "include false on named fragment", query: "{ a ...F @include(if: false) } fragment F on T { b }", expected: []string{"a"}},
		{name: "skipped spread does not hide a later one", query: "{ ...F @skip(if: true) a ...F } fragment F on T { b }", expected: []string{"a", "b"}},
		{name: "skip inside named fragment", query: "{ a ...F } fragment F on T { b @skip(if: true) }", expected: []string{"a"}},
		{
			name: "skip from variable", query: "query ($s: Boolean!) { a b @skip(if: $s) }",
			variables: map[string]interface{}{"s": true}, expected: []string{"a"},
		},
		{
			name: "include from variable", query: "query ($i: Boolean!) { a ...F @include(if: $i) } fragment F on T { b }",
			variables: map[string]interface{}{"i": false}, expected: []string{"a"},
		},
		{
			name: "variable keeps field", query: "query ($s: Boolean!) { a b @skip(if: $s) }",
			variables: map[string]interface{}{"s": false}, expected: []string{"a", "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, fragments := parseSelection(t, tc.query)

			var names []string
			for _, field := range SelectedFields(set, fragments, tc.variables) {
				names = append(names, field.Name.Value)
			}

			assert.Equal(t, tc.expected, names)
		})
	}
}

func parseSelection(t *testing.T, query string) (*ast.SelectionSet, map[string]ast.Definition) {
	t.Helper()
	doc, err := parser.Parse(parser.ParseParams{Source: query})
	require.NoError(t, err)

	var set *ast.SelectionSet
	fragments := map[string]ast.Definition{}
	for _, def := range doc.Definitions {
		switch d := def.(type) {
		case *ast.OperationDefinition:
			set = d.SelectionSet
		case *ast.FragmentDefinition:
			fragments[d.Name.Value] = d
		}
	}
	return set, fragments
}
