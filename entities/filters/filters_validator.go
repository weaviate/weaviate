//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

func ValidateFilters(sch schema.Schema, filters *LocalFilter) error {
	if filters == nil {
		return errors.New("empty where")
	}
	return validateClause(sch, filters.Root)
}

func validateClause(sch schema.Schema, clause *Clause) error {
	// check if nested
	if clause.Operands != nil {
		var errs []error

		for i, child := range clause.Operands {
			if err := validateClause(sch, &child); err != nil {
				errs = append(errs, errors.Wrapf(err, "child operand at position %d", i))
			}
		}

		if len(errs) > 0 {
			return mergeErrs(errs)
		} else {
			return nil
		}
	}

	// validate current

	className := clause.On.GetInnerMost().Class
	propName := clause.On.GetInnerMost().Property

	if IsInternalProperty(propName) {
		return validateInternalPropertyClause(propName, clause)
	}

	class := sch.FindClassByName(className)
	if class == nil {
		return errors.Errorf("class %q does not exist in schema",
			className)
	}

	propNameTyped := string(propName)
	lengthPropName, isPropLengthFilter := schema.IsPropertyLength(propNameTyped, 0)
	if isPropLengthFilter {
		propName = schema.PropertyName(lengthPropName)
	}

	prop, err := sch.GetProperty(className, propName)
	if err != nil {
		return err
	}

	if clause.Operator == OperatorIsNull {
		if clause.Value.Type == schema.DataTypeBoolean {
			return nil
		} else {
			errors.Errorf("operator IsNull requires a booleanValue, got %q instead",
				valueNameFromDataType(clause.Value.Type))
		}
	}

	if isPropLengthFilter {
		op := clause.Operator
		if clause.Value.Type != schema.DataTypeInt {
			return errors.Errorf("Filtering for property length requires IntValue, got %q instead",
				valueNameFromDataType(clause.Value.Type))
		} else if op != OperatorEqual && op != OperatorNotEqual &&
			op != OperatorGreaterThan && op != OperatorGreaterThanEqual &&
			op != OperatorLessThan && op != OperatorLessThanEqual {
			return errors.Errorf("Filtering for property length supports operators (not) equal and greater/less than (equal), got %q instead",
				op)
		} else if clause.Value.Value.(int) < 0 {
			return errors.Errorf("Can only filter for positive property length got %v instead", clause.Value.Value)
		} else {
			return nil
		}
	}

	if isUUIDType(prop.DataType[0]) {
		return validateUUIDType(propName, clause)
	}

	if schema.IsRefDataType(prop.DataType) {
		// bit of an edge case, directly on refs (i.e. not on a primitive prop of a
		// ref) we only allow valueInt which is what's used to count references
		if clause.Value.Type == schema.DataTypeInt {
			return nil
		}

		return errors.Errorf("Property %q is a ref prop to the class %q. Only "+
			"\"valueInt\" can be used on a ref prop directly to count the number of refs. "+
			"Or did you mean to filter on a primitive prop of the referenced class? "+
			"In this case make sure your path contains 3 elements in the form of "+
			"[<propName>, <ClassNameOfReferencedClass>, <primitvePropOnClass>]",
			propName, prop.DataType[0])
	} else if baseType, ok := schema.IsArrayType(schema.DataType(prop.DataType[0])); ok {
		if baseType != clause.Value.Type {
			return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
				valueNameFromDataType(clause.Value.Type),
				schema.DataType(prop.DataType[0]),
				valueNameFromDataType(baseType))
		}
	} else if prop.DataType[0] != string(clause.Value.Type) {
		return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
			valueNameFromDataType(clause.Value.Type),
			schema.DataType(prop.DataType[0]),
			valueNameFromDataType(schema.DataType(prop.DataType[0])))
	}

	return nil
}

func valueNameFromDataType(dt schema.DataType) string {
	return "value" + strings.ToUpper(string(dt[0])) + string(dt[1:])
}

func mergeErrs(errs []error) error {
	msgs := make([]string, len(errs))
	for i, err := range errs {
		msgs[i] = err.Error()
	}

	return errors.Errorf("%s", strings.Join(msgs, ", "))
}

func IsInternalProperty(propName schema.PropertyName) bool {
	switch propName {
	case InternalPropBackwardsCompatID,
		InternalPropID,
		InternalPropCreationTimeUnix,
		InternalPropLastUpdateTimeUnix:
		return true
	default:
		return false
	}
}

func validateInternalPropertyClause(propName schema.PropertyName, clause *Clause) error {
	switch propName {
	case InternalPropBackwardsCompatID, InternalPropID:
		if clause.Value.Type == schema.DataTypeString {
			return nil
		}
		return errors.Errorf(
			`using ["_id"] to filter by uuid: must use "valueString" to specify the id`)
	case InternalPropCreationTimeUnix, InternalPropLastUpdateTimeUnix:
		if clause.Value.Type == schema.DataTypeDate ||
			clause.Value.Type == schema.DataTypeString {
			return nil
		}
		return errors.Errorf(
			`using ["%s"] to filter by timestamp: must use "valueString" or "valueDate"`, propName)
	default:
		return errors.Errorf("unsupported internal property: %s", propName)
	}
}

func isUUIDType(dtString string) bool {
	dt := schema.DataType(dtString)
	return dt == schema.DataTypeUUID || dt == schema.DataTypeUUIDArray
}

func validateUUIDType(propName schema.PropertyName, clause *Clause) error {
	if clause.Value.Type == schema.DataTypeString {
		return validateUUIDOperators(propName, clause)
	}

	return fmt.Errorf("property %q is of type \"uuid\" or \"uuid[]\": "+
		"specify uuid as string using \"valueString\"", propName)
}

func validateUUIDOperators(propName schema.PropertyName, clause *Clause) error {
	op := clause.Operator

	switch op {
	case OperatorEqual, OperatorNotEqual, OperatorLessThan, OperatorLessThanEqual,
		OperatorGreaterThan, OperatorGreaterThanEqual:
		return nil
	default:
		return fmt.Errorf("operator %q cannot be used on uuid/uuid[] props", op.Name())
	}
}
