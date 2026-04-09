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
// (object/object[]) properties. It handles three cases:
//   - len(nested) or len(nested.path): length filtering is not supported
//   - nested == "x": no dot notation, direct filter on object type is not supported
//   - nested.city == "x": valid dot-notation path, delegated to validateNestedPathClause
func validateNestedProp(prop *models.Property, propName schema.PropertyName, isPropLengthFilter bool, cw *clauseWrapper) error {
	if isPropLengthFilter {
		return fmt.Errorf("property length filtering is not supported for nested properties")
	}

	pathSegs := nested.SplitPath(string(propName))

	// Check whether a [N] index was applied to the root property itself.
	if pathSegs[0].HasIndex {
		rootDT := schema.DataType(prop.DataType[0])
		if _, ok := schema.IsArrayType(rootDT); !ok {
			return fmt.Errorf("property %q is of type %q — [N] indexing requires an array type",
				pathSegs[0].Name, rootDT)
		}
	}

	if cw.getOperator() == OperatorIsNull {
		if !cw.isType(schema.DataTypeBoolean) {
			return fmt.Errorf("operator IsNull requires a booleanValue, got %v instead",
				cw.getValueNameFromType())
		}
		// Root-level IsNull (no sub-path) — valid.
		if len(pathSegs) == 1 {
			return nil
		}
		// Sub-property IsNull: walk the path to verify it exists.
		return validateNestedPathClause(pathSegs[1:], prop.NestedProperties, propName, cw)
	}

	if len(pathSegs) == 1 {
		return fmt.Errorf("property %q is of type %q; use dot notation to filter on a sub-property",
			propName, schema.DataType(prop.DataType[0]))
	}

	return validateNestedPathClause(pathSegs[1:], prop.NestedProperties, propName, cw)
}

// validateNestedPathClause validates sub-property segments of a nested filter
// path. subSegs are the segments after the root (already resolved by the caller);
// nestedProps are the root property's NestedProperties. propName is used only
// for error messages.
func validateNestedPathClause(relSegs []nested.PathSegment, nestedProps []*models.NestedProperty, propName schema.PropertyName, cw *clauseWrapper) error {
	for i, seg := range relSegs {
		np := nested.FindNestedProp(nestedProps, seg.Name)
		if np == nil {
			return fmt.Errorf("nested path %q: sub-property %q not found", propName, seg.Name)
		}

		isLast := i == len(relSegs)-1
		dt := schema.DataType(np.DataType[0])

		// Indexing is only valid on array types.
		if seg.HasIndex {
			if _, ok := schema.IsArrayType(dt); !ok {
				return fmt.Errorf("nested path %q: sub-property %q is of type %q — [N] indexing requires an array type",
					propName, seg.Name, dt)
			}
		}

		if !isLast {
			// Intermediate sub-property must be object or object[].
			if !schema.IsNested(dt) {
				return fmt.Errorf("nested path %q: sub-property %q must be object or object[], got %q",
					propName, seg.Name, dt)
			}
			nestedProps = np.NestedProperties
		} else {
			// IsNull on any leaf type — including object/object[] — is valid:
			// it checks existence, not value.
			if cw.getOperator() == OperatorIsNull {
				return nil
			}
			// Leaf sub-property must be a filterable primitive, not object/object[].
			if schema.IsNested(dt) {
				return fmt.Errorf("nested path %q: sub-property %q is of type %q; "+
					"filtering on object types is not supported, specify a primitive sub-property",
					propName, seg.Name, dt)
			}
			// TODO aliszka:nested_filtering when rangeable / searchable nested
			// filtering support is added, add corresponding
			// schema.IsNestedRangeable and schema.IsNestedSearchable checks
			// alongside the filterable check below.
			if !schema.IsNestedFilterable(np) {
				return fmt.Errorf("nested path %q: sub-property %q is not filterable", propName, seg.Name)
			}
			return validateNestedLeafType(propName, np, cw)
		}
	}

	// Unreachable: caller guarantees relSegs is non-empty.
	return nil
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
