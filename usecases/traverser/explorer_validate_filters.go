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

	class := sch.FindClassByName(className)
	if class == nil {
		return errors.Errorf("class %q does not exist in schema",
			className)
	}

	prop, err := sch.GetProperty(className, propName)
	if err != nil {
		return err
	}

	if baseType, ok := schema.IsArrayType(schema.DataType(prop.DataType[0])); ok {
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
