/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
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
	// DataTypeGeoCoordinates is used to represent geo coordintaes, i.e. latitude
	// and longitude pairs of locations on earth
	DataTypeGeoCoordinates DataType = "geoCoordinates"
)

var PrimitiveDataTypes []DataType = []DataType{DataTypeString, DataTypeText, DataTypeInt, DataTypeNumber, DataTypeBoolean, DataTypeDate, DataTypeGeoCoordinates}

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
			case string(DataTypeString), string(DataTypeText),
				string(DataTypeInt), string(DataTypeNumber),
				string(DataTypeBoolean), string(DataTypeDate), string(DataTypeGeoCoordinates):
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
		if ValidNetworkClassName(someDataType) {
			// this is a network instance
			classes = append(classes, ClassName(someDataType))
		} else {
			// this is a local reference
			className, err := ValidateClassName(someDataType)
			if err != nil {
				return nil, err
			}

			if s.FindClassByName(className) == nil {
				return nil, fmt.Errorf("SingleRef class name '%s' does not exist", className)
			}

			classes = append(classes, className)
		}
	}

	return &propertyDataType{
		kind:    PropertyKindRef,
		classes: classes,
	}, nil
}
