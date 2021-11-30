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
	className := filters.Root.On.GetInnerMost().Class
	propName := filters.Root.On.GetInnerMost().Property

	class := sch.FindClassByName(className)
	if class == nil {
		return errors.Errorf("where filter: class %q does not exist in schema",
			className)
	}

	prop, err := sch.GetProperty(className, propName)
	if err != nil {
		return errors.Wrap(err, "where filter")
	}

	if baseType, ok := schema.IsArrayType(schema.DataType(prop.DataType[0])); ok {
		if baseType != filters.Root.Value.Type {
			return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
				valueNameFromDataType(filters.Root.Value.Type),
				schema.DataType(prop.DataType[0]),
				valueNameFromDataType(baseType))
		}
	} else if prop.DataType[0] != string(filters.Root.Value.Type) {
		return errors.Errorf("data type filter cannot use %q on type %q, use %q instead",
			valueNameFromDataType(filters.Root.Value.Type),
			schema.DataType(prop.DataType[0]),
			valueNameFromDataType(schema.DataType(prop.DataType[0])))
	}

	return nil
}

func valueNameFromDataType(dt schema.DataType) string {
	return "value" + strings.ToUpper(string(dt[0])) + string(dt[1:])
}
