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

package filters

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// validateNestedProp is the single entry point for all validation of nested
// (object/object[]) properties. It handles four cases:
//   - len(nested) or len(nested.path): length filtering is not supported
//   - IsNull operator: checks operand type is boolean and the path navigates
//     correctly; the leaf may be any type (existence check)
//   - nested == "x": no dot notation, direct filter on object type is not supported
//   - nested.city == "x": valid dot-notation path — delegates path-shape checks
//     to nested.ResolveLeafFromRoot and then validates the leaf's filterability
//     and value-type compatibility
func validateNestedProp(prop *models.Property, propName schema.PropertyName, isPropLengthFilter bool, cw *clauseWrapper) error {
	if isPropLengthFilter {
		return fmt.Errorf("property length filtering is not supported for nested properties")
	}

	pathSegs := nested.ParseSegments(string(propName))

	if cw.getOperator() == OperatorIsNull {
		if !cw.isType(schema.DataTypeBoolean) {
			return fmt.Errorf("operator IsNull requires a booleanValue, got %v instead",
				cw.getValueNameFromType())
		}
		// Root-level IsNull (no sub-path) — still validate any [N] index on
		// the root; otherwise the filter is fine and any leaf type is valid.
		if pathSegs[0].HasIndex {
			rootDT := schema.DataType(prop.DataType[0])
			if _, ok := schema.IsArrayType(rootDT); !ok {
				return fmt.Errorf("property %q is of type %q — [N] indexing requires an array type",
					pathSegs[0].Name, rootDT)
			}
		}
		if len(pathSegs) == 1 {
			return nil
		}
		// Sub-property IsNull: walk the path to verify it navigates. The
		// helper accepts any leaf type, which matches IsNull semantics
		// (existence check on object/object[] leaves is valid).
		_, err := nested.ResolveLeafFromRoot(prop, string(propName))
		return err
	}

	if len(pathSegs) == 1 {
		if pathSegs[0].HasIndex {
			rootDT := schema.DataType(prop.DataType[0])
			if _, ok := schema.IsArrayType(rootDT); !ok {
				return fmt.Errorf("property %q is of type %q — [N] indexing requires an array type",
					pathSegs[0].Name, rootDT)
			}
		}
		return fmt.Errorf("property %q is of type %q; use dot notation to filter on a sub-property",
			propName, schema.DataType(prop.DataType[0]))
	}

	leaf, err := nested.ResolveLeafFromRoot(prop, string(propName))
	if err != nil {
		return err
	}
	leafDT := schema.DataType(leaf.DataType[0])

	// Leaf sub-property must be a filterable primitive, not object/object[].
	if schema.IsNested(leafDT) {
		return fmt.Errorf("nested path %q: sub-property %q is of type %q; "+
			"filtering on object types is not supported, specify a primitive sub-property",
			propName, leaf.Name, leafDT)
	}
	// TODO aliszka:nested_filtering when rangeable / searchable nested
	// filtering support is added, add corresponding
	// schema.IsNestedRangeable and schema.IsNestedSearchable checks
	// alongside the filterable check below.
	if !schema.IsNestedFilterable(leaf) {
		return fmt.Errorf("nested path %q: sub-property %q is not filterable", propName, leaf.Name)
	}
	return validateNestedLeafType(propName, leaf, cw)
}

// validateNestedLeafType checks that the filter value type is compatible with
// the primitive leaf NestedProperty. Mirrors the type-check logic in
// validateClause for flat properties.
func validateNestedLeafType(propName schema.PropertyName, np *models.NestedProperty, cw *clauseWrapper) error {
	// IsNull checks existence, not value — its boolean value is valid for any
	// leaf type regardless of the property's data type.
	if cw.getOperator() == OperatorIsNull {
		return nil
	}

	if isUUIDType(np.DataType[0]) {
		return validateUUIDType(propName, cw)
	}

	dt := schema.DataType(np.DataType[0])

	if baseType, ok := schema.IsArrayType(dt); ok {
		if !cw.isType(baseType) {
			return fmt.Errorf("nested path %q: data type filter cannot use %q on type %q, use %q instead",
				propName, cw.getValueNameFromType(), dt, valueNameFromDataType(baseType))
		}
		return nil
	}

	if !cw.isType(dt) {
		return fmt.Errorf("nested path %q: data type filter cannot use %q on type %q, use %q instead",
			propName, cw.getValueNameFromType(), dt, valueNameFromDataType(dt))
	}
	return nil
}
