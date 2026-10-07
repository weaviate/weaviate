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
// selection to *ast.Field (which panics on fragments).
func SelectedFields(set *ast.SelectionSet, fragments map[string]ast.Definition) []*ast.Field {
	if set == nil {
		return nil
	}
	var fields []*ast.Field
	for _, selection := range set.Selections {
		switch s := selection.(type) {
		case *ast.Field:
			fields = append(fields, s)
		case *ast.InlineFragment:
			fields = append(fields, SelectedFields(s.SelectionSet, fragments)...)
		case *ast.FragmentSpread:
			// graphql validation rejects unknown and cyclic fragments before resolving
			if def, ok := fragments[s.Name.Value].(*ast.FragmentDefinition); ok {
				fields = append(fields, SelectedFields(def.SelectionSet, fragments)...)
			}
		}
	}
	return fields
}
