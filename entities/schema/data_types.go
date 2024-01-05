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

package schema

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

type DataType string

const (
	// DataTypeCRef The data type is a cross-reference, it is starting with a capital letter
	DataTypeCRef DataType = "cref"
	// DataTypeText The data type is a value of type string
	DataTypeText DataType = "text"
	// DataTypeInt The data type is a value of type int
	DataTypeInt DataType = "int"
	// DataTypeNumber The data type is a value of type number/float
	DataTypeNumber DataType = "number"
	// DataTypeBoolean The data type is a value of type boolean
	DataTypeBoolean DataType = "boolean"
	// DataTypeDate The data type is a value of type date
	DataTypeDate DataType = "date"
	// DataTypeGeoCoordinates is used to represent geo coordinates, i.e. latitude
	// and longitude pairs of locations on earth
	DataTypeGeoCoordinates DataType = "geoCoordinates"
	// DataTypePhoneNumber represents a parsed/to-be-parsed phone number
	DataTypePhoneNumber DataType = "phoneNumber"
	// DataTypeBlob represents a base64 encoded data
	DataTypeBlob DataType = "blob"
	// DataTypeTextArray The data type is a value of type string array
	DataTypeTextArray DataType = "text[]"
	// DataTypeIntArray The data type is a value of type int array
	DataTypeIntArray DataType = "int[]"
	// DataTypeNumberArray The data type is a value of type number/float array
	DataTypeNumberArray DataType = "number[]"
	// DataTypeBooleanArray The data type is a value of type boolean array
	DataTypeBooleanArray DataType = "boolean[]"
	// DataTypeDateArray The data type is a value of type date array
	DataTypeDateArray DataType = "date[]"
	// DataTypeUUID is a native UUID data type. It is stored in it's raw byte
	// representation and therefore takes up less space than storing a UUID as a
	// string
	DataTypeUUID DataType = "uuid"
	// DataTypeUUIDArray is the array version of DataTypeUUID
	DataTypeUUIDArray DataType = "uuid[]"

	DataTypeObject      DataType = "object"
	DataTypeObjectArray DataType = "object[]"

	// deprecated as of v1.19, replaced by DataTypeText + relevant tokenization setting
	// DataTypeString The data type is a value of type string
	DataTypeString DataType = "string"
	// deprecated as of v1.19, replaced by DataTypeTextArray + relevant tokenization setting
	// DataTypeArrayString The data type is a value of type string array
	DataTypeStringArray DataType = "string[]"
)

func (dt DataType) String() string {
	return string(dt)
}

func (dt DataType) PropString() []string {
	return []string{dt.String()}
}

func (dt DataType) AsName() string {
	return strings.ReplaceAll(dt.String(), "[]", "Array")
}

var PrimitiveDataTypes []DataType = []DataType{
	DataTypeText, DataTypeInt, DataTypeNumber, DataTypeBoolean, DataTypeDate,
	DataTypeGeoCoordinates, DataTypePhoneNumber, DataTypeBlob, DataTypeTextArray,
	DataTypeIntArray, DataTypeNumberArray, DataTypeBooleanArray, DataTypeDateArray,
	DataTypeUUID, DataTypeUUIDArray,
}

var NestedDataTypes []DataType = []DataType{
	DataTypeObject, DataTypeObjectArray,
}

var DeprecatedPrimitiveDataTypes []DataType = []DataType{
	// deprecated as of v1.19
	DataTypeString, DataTypeStringArray,
}

type PropertyKind int

const (
	PropertyKindPrimitive PropertyKind = 1
	PropertyKindRef       PropertyKind = 2
	PropertyKindNested    PropertyKind = 3
)

type PropertyDataType interface {
	Kind() PropertyKind
	IsPrimitive() bool
	AsPrimitive() DataType
	IsReference() bool
	Classes() []ClassName
	ContainsClass(name ClassName) bool
	IsNested() bool
	AsNested() DataType
}

type propertyDataType struct {
	kind          PropertyKind
	primitiveType DataType
	classes       []ClassName
	nestedType    DataType
}

// IsPropertyLength returns if a string is a filters for property length. They have the form len(*PROPNAME*)
func IsPropertyLength(propName string, offset int) (string, bool) {
	isPropLengthFilter := len(propName) > 4+offset && propName[offset:offset+4] == "len(" && propName[len(propName)-1:] == ")"

	if isPropLengthFilter {
		return propName[offset+4 : len(propName)-1], isPropLengthFilter
	}
	return "", false
}

func IsArrayType(dt DataType) (DataType, bool) {
	switch dt {
	case DataTypeStringArray:
		return DataTypeString, true
	case DataTypeTextArray:
		return DataTypeText, true
	case DataTypeNumberArray:
		return DataTypeNumber, true
	case DataTypeIntArray:
		return DataTypeInt, true
	case DataTypeBooleanArray:
		return DataTypeBoolean, true
	case DataTypeDateArray:
		return DataTypeDate, true
	case DataTypeUUIDArray:
		return DataTypeUUID, true
	case DataTypeObjectArray:
		return DataTypeObject, true
	default:
		return "", false
	}
}

func (p *propertyDataType) Kind() PropertyKind {
	return p.kind
}

func (p *propertyDataType) IsPrimitive() bool {
	return p.kind == PropertyKindPrimitive
}

func (p *propertyDataType) AsPrimitive() DataType {
	if !p.IsPrimitive() {
		panic("not primitive type")
	}

	return p.primitiveType
}

func (p *propertyDataType) IsReference() bool {
	return p.kind == PropertyKindRef
}

func (p *propertyDataType) Classes() []ClassName {
	if !p.IsReference() {
		panic("not MultipleRef type")
	}

	return p.classes
}

func (p *propertyDataType) ContainsClass(needle ClassName) bool {
	if !p.IsReference() {
		panic("not MultipleRef type")
	}

	for _, class := range p.classes {
		if class == needle {
			return true
		}
	}

	return false
}

func (p *propertyDataType) IsNested() bool {
	return p.kind == PropertyKindNested
}

func (p *propertyDataType) AsNested() DataType {
	if !p.IsNested() {
		panic("not nested type")
	}
	return p.nestedType
}

// Based on the schema, return a valid description of the defined datatype
//
// Note that this function will error if referenced classes do not exist. If
// you don't want such validation, use [Schema.FindPropertyDataTypeRelaxedRefs]
// instead and set relax to true
func (s *Schema) FindPropertyDataType(dataType []string) (PropertyDataType, error) {
	return s.FindPropertyDataTypeWithRefs(dataType, false, "")
}

// Based on the schema, return a valid description of the defined datatype
// If relaxCrossRefValidation is set, there is no check if the referenced class
// exists in the schema. This can be helpful in scenarios, such as restoring
// from a backup where we have no guarantee over the order of class creation.
// If belongingToClass is set and equal to referenced class, check whether class
// exists in the schema is skipped. This is done to allow creating class schema with
// properties referencing to itself. Previously such properties had to be created separately
// only after creation of class schema
func (s *Schema) FindPropertyDataTypeWithRefs(
	dataType []string, relaxCrossRefValidation bool, beloningToClass ClassName,
) (PropertyDataType, error) {
	if len(dataType) < 1 {
		return nil, errors.New("dataType must have at least one element")
	}
	if len(dataType) == 1 {
		for _, dt := range append(PrimitiveDataTypes, DeprecatedPrimitiveDataTypes...) {
			if dataType[0] == dt.String() {
				return &propertyDataType{
					kind:          PropertyKindPrimitive,
					primitiveType: dt,
				}, nil
			}
		}
		for _, dt := range NestedDataTypes {
			if dataType[0] == dt.String() {
				return &propertyDataType{
					kind:       PropertyKindNested,
					nestedType: dt,
				}, nil
			}
		}
		if len(dataType[0]) == 0 {
			return nil, fmt.Errorf("dataType cannot be an empty string")
		}
		firstLetter := rune(dataType[0][0])
		if unicode.IsLower(firstLetter) {
			return nil, fmt.Errorf("Unknown primitive data type '%s'", dataType[0])
		}
	}
	/* implies len(dataType) > 1, or first element is a class already */
	var classes []ClassName

	for _, someDataType := range dataType {
		className, err := ValidateClassName(someDataType)
		if err != nil {
			return nil, err
		}

		if beloningToClass != className && !relaxCrossRefValidation {
			if s.FindClassByName(className) == nil {
				return nil, ErrRefToNonexistentClass
			}
		}

		classes = append(classes, className)
	}

	return &propertyDataType{
		kind:    PropertyKindRef,
		classes: classes,
	}, nil
}

func AsPrimitive(dataType []string) (DataType, bool) {
	if len(dataType) == 1 {
		for _, dt := range append(PrimitiveDataTypes, DeprecatedPrimitiveDataTypes...) {
			if dataType[0] == dt.String() {
				return dt, true
			}
		}
		if len(dataType[0]) == 0 {
			return "", true
		}
	}
	return "", false
}

func AsNested(dataType []string) (DataType, bool) {
	if len(dataType) == 1 {
		for _, dt := range NestedDataTypes {
			if dataType[0] == dt.String() {
				return dt, true
			}
		}
	}
	return "", false
}

func IsNested(dataType DataType) bool {
	for _, dt := range NestedDataTypes {
		if dt == dataType {
			return true
		}
	}
	return false
}
