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

import "github.com/tailor-platform/graphql/language/ast"

// SelectedFields returns the fields of a selection set with inline fragments
// and fragment spreads expanded, so callers never have to type-assert a
// selection to *ast.Field (which panics on fragments). Like graphql's own
// CollectFields it drops selections excluded by @skip/@include and expands
// each named fragment at most once, so repeated spreads cannot multiply the
// result exponentially.
func SelectedFields(set *ast.SelectionSet, fragments map[string]ast.Definition, variables map[string]interface{}) []*ast.Field {
	return collectFields(nil, set, fragments, variables, map[string]bool{})
}

func collectFields(fields []*ast.Field, set *ast.SelectionSet, fragments map[string]ast.Definition,
	variables map[string]interface{}, visited map[string]bool,
) []*ast.Field {
	if set == nil {
		return fields
	}
	for _, selection := range set.Selections {
		switch s := selection.(type) {
		case *ast.Field:
			if IsIncluded(s.Directives, variables) {
				fields = append(fields, s)
			}
		case *ast.InlineFragment:
			if IsIncluded(s.Directives, variables) {
				fields = collectFields(fields, s.SelectionSet, fragments, variables, visited)
			}
		case *ast.FragmentSpread:
			if visited[s.Name.Value] || !IsIncluded(s.Directives, variables) {
				continue
			}
			visited[s.Name.Value] = true
			// graphql validation rejects unknown and cyclic fragments before resolving
			if def, ok := fragments[s.Name.Value].(*ast.FragmentDefinition); ok {
				fields = collectFields(fields, def.SelectionSet, fragments, variables, visited)
			}
		}
	}
	return fields
}

// IsIncluded evaluates @skip and @include like graphql's executor does.
func IsIncluded(directives []*ast.Directive, variables map[string]interface{}) bool {
	for _, directive := range directives {
		switch directive.Name.Value {
		case "skip":
			if skip, ok := directiveIf(directive, variables); ok && skip {
				return false
			}
		case "include":
			if include, ok := directiveIf(directive, variables); ok && !include {
				return false
			}
		}
	}
	return true
}

func directiveIf(directive *ast.Directive, variables map[string]interface{}) (value, ok bool) {
	for _, arg := range directive.Arguments {
		if arg.Name.Value != "if" {
			continue
		}
		switch v := arg.Value.(type) {
		case *ast.BooleanValue:
			return v.Value, true
		case *ast.Variable:
			value, ok = variables[v.Name.Value].(bool)
			return value, ok
		}
	}
	return false, false
}
