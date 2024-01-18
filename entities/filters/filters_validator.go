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

package filters

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

// string and stringArray are deprecated as of v1.19
// however they are allowed in filters and considered aliases
// for text and textArray
var deprecatedDataTypeAliases map[schema.DataType]schema.DataType = map[schema.DataType]schema.DataType{
	schema.DataTypeString:      schema.DataTypeText,
	schema.DataTypeStringArray: schema.DataTypeTextArray,
}

func ValidateFilters(sch schema.Schema, filters *LocalFilter) error {
	if filters == nil {
		return errors.New("empty where")
	}
	cw := newClauseWrapper(filters.Root)
	if err := validateClause(sch, cw); err != nil {
		return err
	}
	cw.updateClause()
	return nil
}

func validateClause(sch schema.Schema, cw *clauseWrapper) error {
	// check if nested
	if cw.getOperands() != nil {
		var errs []error

		for i, child := range cw.getOperands() {
			if err := validateClause(sch, child); err != nil {
				errs = append(errs, errors.Wrapf(err, "child operand at position %d", i))
			}
		}

		if len(errs) > 0 {
			return mergeErrs(errs)
		}
		return nil
	}

	// validate current

	className := cw.getClassName()
	propName := cw.getPropertyName()

	if IsInternalProperty(propName) {
		return validateInternalPropertyClause(propName, cw)
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

	if cw.getOperator() == OperatorIsNull {
		if !cw.isType(schema.DataTypeBoolean) {
			return errors.Errorf("operator IsNull requires a booleanValue, got %q instead",
				cw.getValueNameFromType())
		}
		return nil
	}

	if isPropLengthFilter {
		if !cw.isType(schema.DataTypeInt) {
			return errors.Errorf("Filtering for property length requires IntValue, got %q instead",
				cw.getValueNameFromType())
		}
		switch op := cw.getOperator(); op {
		case OperatorEqual, OperatorNotEqual, OperatorGreaterThan, OperatorGreaterThanEqual,
			OperatorLessThan, OperatorLessThanEqual:
			// ok
		default:
			return errors.Errorf("Filtering for property length supports operators (not) equal and greater/less than (equal), got %q instead",
				op)
		}
		if val := cw.getValue(); val.(int) < 0 {
			return errors.Errorf("Can only filter for positive property length got %v instead", val)
		}
		return nil
	}

	if isUUIDType(prop.DataType[0]) {
		return validateUUIDType(propName, cw)
	}

	if schema.IsRefDataType(prop.DataType) {
		// bit of an edge case, directly on refs (i.e. not on a primitive prop of a
		// ref) we only allow valueInt which is what's used to count references
		if cw.isType(schema.DataTypeInt) {
			return nil
		}
		return errors.Errorf("Property %q is a ref prop to the class %q. Only "+
			"\"valueInt\" can be used on a ref prop directly to count the number of refs. "+
			"Or did you mean to filter on a primitive prop of the referenced class? "+
			"In this case make sure your path contains 3 elements in the form of "+
			"[<propName>, <ClassNameOfReferencedClass>, <primitvePropOnClass>]",
			propName, prop.DataType[0])
	} else if baseType, ok := schema.IsArrayType(schema.DataType(prop.DataType[0])); ok {
		if !cw.isType(baseType) {
			return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
				cw.getValueNameFromType(),
				schema.DataType(prop.DataType[0]),
				valueNameFromDataType(baseType))
		}
	} else if !cw.isType(schema.DataType(prop.DataType[0])) {
		return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
			cw.getValueNameFromType(),
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

func validateInternalPropertyClause(propName schema.PropertyName, cw *clauseWrapper) error {
	switch propName {
	case InternalPropBackwardsCompatID, InternalPropID:
		if cw.isType(schema.DataTypeText) {
			return nil
		}
		return errors.Errorf(
			`using ["_id"] to filter by uuid: must use "valueText" to specify the id`)
	case InternalPropCreationTimeUnix, InternalPropLastUpdateTimeUnix:
		if cw.isType(schema.DataTypeDate) || cw.isType(schema.DataTypeText) {
			return nil
		}
		return errors.Errorf(
			`using ["%s"] to filter by timestamp: must use "valueText" or "valueDate"`, propName)
	default:
		return errors.Errorf("unsupported internal property: %s", propName)
	}
}

func isUUIDType(dtString string) bool {
	dt := schema.DataType(dtString)
	return dt == schema.DataTypeUUID || dt == schema.DataTypeUUIDArray
}

func validateUUIDType(propName schema.PropertyName, cw *clauseWrapper) error {
	if cw.isType(schema.DataTypeText) {
		return validateUUIDOperators(propName, cw)
	}

	return fmt.Errorf("property %q is of type \"uuid\" or \"uuid[]\": "+
		"specify uuid as string using \"valueText\"", propName)
}

func validateUUIDOperators(propName schema.PropertyName, cw *clauseWrapper) error {
	op := cw.getOperator()

	switch op {
	case OperatorEqual, OperatorNotEqual, OperatorLessThan, OperatorLessThanEqual,
		OperatorGreaterThan, OperatorGreaterThanEqual, ContainsAll, ContainsAny:
		return nil
	default:
		return fmt.Errorf("operator %q cannot be used on uuid/uuid[] props", op.Name())
	}
}

type clauseWrapper struct {
	clause    *Clause
	origType  schema.DataType
	aliasType schema.DataType
	operands  []*clauseWrapper
}

func newClauseWrapper(clause *Clause) *clauseWrapper {
	w := &clauseWrapper{clause: clause}
	if clause.Operands != nil {
		w.operands = make([]*clauseWrapper, len(clause.Operands))
		for i := range clause.Operands {
			w.operands[i] = newClauseWrapper(&clause.Operands[i])
		}
	} else {
		w.origType = clause.Value.Type
		w.aliasType = deprecatedDataTypeAliases[clause.Value.Type]
	}
	return w
}

func (w *clauseWrapper) isType(dt schema.DataType) bool {
	if w.operands != nil {
		return false
	}
	return dt == w.origType || (dt == w.aliasType && w.aliasType != "")
}

func (w *clauseWrapper) getValueNameFromType() string {
	return valueNameFromDataType(w.origType)
}

func (w *clauseWrapper) getOperands() []*clauseWrapper {
	return w.operands
}

func (w *clauseWrapper) getOperator() Operator {
	return w.clause.Operator
}

func (w *clauseWrapper) getValue() interface{} {
	return w.clause.Value.Value
}

func (w *clauseWrapper) getClassName() schema.ClassName {
	if w.operands != nil {
		return ""
	}
	return w.clause.On.GetInnerMost().Class
}

func (w *clauseWrapper) getPropertyName() schema.PropertyName {
	if w.operands != nil {
		return ""
	}
	return w.clause.On.GetInnerMost().Property
}

func (w *clauseWrapper) updateClause() {
	if w.operands != nil {
		for i := range w.operands {
			w.operands[i].updateClause()
		}
	} else {
		if w.aliasType != "" {
			w.clause.Value.Type = w.aliasType
		}
	}
}
