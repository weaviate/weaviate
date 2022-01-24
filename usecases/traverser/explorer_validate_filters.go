//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (e *Explorer) validateFilters(filters *filters.LocalFilter) error {
	if filters == nil {
		return nil
	}

	sch := e.schemaGetter.GetSchemaSkipAuth()
	return e.validateClause(sch, filters.Root)
}

func (e *Explorer) validateClause(sch schema.Schema, clause *filters.Clause) error {
	// check if nested
	if clause.Operands != nil {
		var errs []error

		for i, child := range clause.Operands {
			if err := e.validateClause(sch, &child); err != nil {
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

	if propName == "id" {
		// special case for the uuid search
		if clause.Value.Type == schema.DataTypeString {
			return nil
		}

		return errors.Errorf("using special path [\"id\"] to filter by uuid: " +
			"must use \"valueString\" to specify the id")
	}

	class := sch.FindClassByName(className)
	if class == nil {
		return errors.Errorf("class %q does not exist in schema",
			className)
	}

	prop, err := sch.GetProperty(className, propName)
	if err != nil {
		return err
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
