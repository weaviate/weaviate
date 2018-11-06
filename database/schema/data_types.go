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
const PropertyKindSingleRef PropertyKind = 2
const PropertyKindMultipleRef PropertyKind = 3

type PropertyDataType interface {
	Kind() PropertyKind

	IsPrimitive() bool
	AsPrimitive() DataType

	IsSingleRef() bool
	AsSingleRef() ClassName

	IsMultipleRef() bool
	AsMultipleRef() []ClassName
}

type propertyDataType struct {
	kind            PropertyKind
	primitiveType   DataType
	singleRefClass  ClassName
	multiRefClasses []ClassName
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

func (p *propertyDataType) IsSingleRef() bool {
	return p.kind == PropertyKindSingleRef
}

func (p *propertyDataType) AsSingleRef() ClassName {
	if p.kind != PropertyKindSingleRef {
		panic("not SingleRef type")
	}

	return p.singleRefClass
}

func (p *propertyDataType) IsMultipleRef() bool {
	return p.kind == PropertyKindMultipleRef
}

func (p *propertyDataType) AsMultipleRef() []ClassName {
	if p.kind != PropertyKindMultipleRef {
		panic("not MultipleRef type")
	}

	return p.multiRefClasses
}

// Based on the schema, return a valid description of the defined datatype
func (s *Schema) FindPropertyDataType(dataType []string) (error, PropertyDataType) {
	if len(dataType) < 1 {
		return errors.New("Empty datatype is invalid"), nil
	} else if len(dataType) == 1 {
		someDataType := dataType[0]
		firstLetter := rune(someDataType[0])
		if unicode.IsLower(firstLetter) {
			switch someDataType {
			case string(DataTypeString), string(DataTypeInt), string(DataTypeNumber), string(DataTypeBoolean), string(DataTypeDate):
				return nil, &propertyDataType{
					kind:          PropertyKindPrimitive,
					primitiveType: DataType(someDataType),
				}
			default:
				return fmt.Errorf("Unknown primitive data type '%s'", someDataType), nil
			}
		} else {
			// LOOKUP CLASSES.
			err, className := ValidateClassName(someDataType)
			if err != nil {
				return fmt.Errorf("Class name %s in the SingleRef is invalid", someDataType), nil
			}
			if s.FindClassByName(className) == nil {
				return fmt.Errorf("SingleRef class name '%s' does not exist", className), nil
			}
			return nil, &propertyDataType{
				kind:           PropertyKindSingleRef,
				singleRefClass: className,
			}
		}
	} else /* implies len(dataType) > 1 */ {
		var classes []ClassName

		for _, someDataType := range dataType {
			className := AssertValidClassName(someDataType)
			if s.FindClassByName(className) == nil {
				return fmt.Errorf("SingleRef class name '%s' does not exist", className), nil
			}

			classes = append(classes, className)
		}

		return nil, &propertyDataType{
			kind:            PropertyKindMultipleRef,
			multiRefClasses: classes,
		}
	}
}
