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
	"strings"

	"github.com/pkg/errors"
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
		return errors.Errorf("property length filtering is not supported for nested properties")
	}

	if !strings.Contains(string(propName), ".") {
		return errors.Errorf("property %q is of type %q; use dot notation to filter on a sub-property",
			propName, schema.DataType(prop.DataType[0]))
	}

	return validateNestedPathClause(prop, propName, cw)
}

// validateNestedPathClause validates a dot-notation filter path such as
// "addresses.city" or "cars.tires.width". prop is the already-fetched
// top-level object/object[] property; segments[1:] are walked through
// its NestedProperties to locate the primitive leaf.
func validateNestedPathClause(prop *models.Property, propName schema.PropertyName, cw *clauseWrapper) error {
	segments := strings.Split(string(propName), ".")

	// Walk segments[1:] through nested properties. segments[0] is the
	// top-level prop, already fetched and verified as object/object[].
	nestedProps := prop.NestedProperties
	for i, seg := range segments[1:] {
		np := findNestedProp(nestedProps, seg)
		if np == nil {
			return errors.Errorf("nested path %q: sub-property %q not found", propName, seg)
		}

		isLast := i == len(segments[1:])-1
		dt := schema.DataType(np.DataType[0])

		if !isLast {
			// Intermediate sub-property must be object or object[].
			if !schema.IsNested(dt) {
				return errors.Errorf("nested path %q: sub-property %q must be object or object[], got %q",
					propName, seg, dt)
			}
			nestedProps = np.NestedProperties
		} else {
			// Leaf sub-property must be a filterable primitive, not object/object[].
			if schema.IsNested(dt) {
				return errors.Errorf("nested path %q: sub-property %q is of type %q; "+
					"filtering on object types is not supported, specify a primitive sub-property",
					propName, seg, dt)
			}
			return validateNestedLeafType(propName, np, cw)
		}
	}

	// Unreachable: caller guarantees propName contains '.' so segments[1:] is non-empty.
	return nil
}

func findNestedProp(props []*models.NestedProperty, name string) *models.NestedProperty {
	for _, np := range props {
		if np.Name == name {
			return np
		}
	}
	return nil
}

// validateNestedLeafType checks that the filter value type is compatible with
// the primitive leaf NestedProperty. Mirrors the type-check logic in
// validateClause for flat properties.
func validateNestedLeafType(propName schema.PropertyName, np *models.NestedProperty, cw *clauseWrapper) error {
	if isUUIDType(np.DataType[0]) {
		return validateUUIDType(propName, cw)
	}

	dt := schema.DataType(np.DataType[0])

	if baseType, ok := schema.IsArrayType(dt); ok {
		if !cw.isType(baseType) {
			return errors.Errorf("nested path %q: data type filter cannot use %q on type %q, use %q instead",
				propName, cw.getValueNameFromType(), dt, valueNameFromDataType(baseType))
		}
		return nil
	}

	if !cw.isType(dt) {
		return errors.Errorf("nested path %q: data type filter cannot use %q on type %q, use %q instead",
			propName, cw.getValueNameFromType(), dt, valueNameFromDataType(dt))
	}
	return nil
}
