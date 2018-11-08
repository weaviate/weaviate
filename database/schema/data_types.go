package schema

import (
	"errors"
	"fmt"
	"unicode"
)

type DataType string

const (
	// DataTypeCRef The data type is a cross-reference, it is starting with a capital letter
	DataTypeCRef DataType = "cref"
	// DataTypeString The data type is a value of type string
	DataTypeString DataType = "string"
	// DataTypeInt The data type is a value of type int
	DataTypeInt DataType = "int"
	// DataTypeNumber The data type is a value of type number/float
	DataTypeNumber DataType = "number"
	// DataTypeBoolean The data type is a value of type boolean
	DataTypeBoolean DataType = "boolean"
	// DataTypeDate The data type is a value of type date
	DataTypeDate DataType = "date"
)

var PrimitiveDataTypes []DataType = []DataType{DataTypeString, DataTypeInt, DataTypeNumber, DataTypeBoolean, DataTypeDate}

type PropertyKind int

const PropertyKindPrimitive PropertyKind = 1
const PropertyKindRef PropertyKind = 2

type PropertyDataType interface {
	Kind() PropertyKind

	IsPrimitive() bool
	AsPrimitive() DataType

	IsReference() bool
	Classes() []ClassName

	ContainsClass(name ClassName) bool
}

type propertyDataType struct {
	kind          PropertyKind
	primitiveType DataType
	classes       []ClassName
}

func (p *propertyDataType) Kind() PropertyKind {
	return p.kind
}

func (p *propertyDataType) IsPrimitive() bool {
	return p.kind == PropertyKindPrimitive
}

func (p *propertyDataType) AsPrimitive() DataType {
	if p.kind != PropertyKindPrimitive {
		panic("not primitive type")
	}

	return p.primitiveType
}

func (p *propertyDataType) IsReference() bool {
	return p.kind == PropertyKindRef
}

func (p *propertyDataType) Classes() []ClassName {
	if p.kind != PropertyKindRef {
		panic("not MultipleRef type")
	}

	return p.classes
}

func (p *propertyDataType) ContainsClass(needle ClassName) bool {
	if p.kind != PropertyKindRef {
		panic("not MultipleRef type")
	}

	for _, class := range p.classes {
		if class == needle {
			return true
		}
	}

	return false
}

// Based on the schema, return a valid description of the defined datatype
func (s *Schema) FindPropertyDataType(dataType []string) (PropertyDataType, error) {
	if len(dataType) < 1 {
		return nil, errors.New("Empty datatype is invalid")
	} else if len(dataType) == 1 {
		someDataType := dataType[0]
		firstLetter := rune(someDataType[0])
		if unicode.IsLower(firstLetter) {
			switch someDataType {
			case string(DataTypeString), string(DataTypeInt), string(DataTypeNumber), string(DataTypeBoolean), string(DataTypeDate):
				return &propertyDataType{
					kind:          PropertyKindPrimitive,
					primitiveType: DataType(someDataType),
				}, nil
			default:
				return nil, fmt.Errorf("Unknown primitive data type '%s'", someDataType)
			}
		}
	}
	/* implies len(dataType) > 1, or first element is a class already */
	var classes []ClassName

	for _, someDataType := range dataType {
		className := AssertValidClassName(someDataType)
		if s.FindClassByName(className) == nil {
			return nil, fmt.Errorf("SingleRef class name '%s' does not exist", className)
		}

		classes = append(classes, className)
	}

	return &propertyDataType{
		kind:    PropertyKindRef,
		classes: classes,
	}, nil
}
