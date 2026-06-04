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

package schema

import (
	errors_ "errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

type PropertyInterface interface {
	GetName() string
	GetNestedProperties() []*models.NestedProperty
}

// GetClassByName returns the class by its name.
func GetClassByName(s *models.Schema, className string) (*models.Class, error) {
	if s == nil {
		return nil, fmt.Errorf(ErrorNoSuchClass, className)
	}

	for _, class := range s.Classes {
		// Check if the name of the class is the given name, that's the class we need
		if class.Class == className {
			return class, nil
		}
	}
	return nil, fmt.Errorf(ErrorNoSuchClass, className)
}

// GetPropertyByName returns a frst-order property by its name.
// If propName is a name of the nested property, then this property is returned.
// It is not possible to retrieve deeply nested properties using the dot-notation.
func GetPropertyByName(c *models.Class, propName string) (*models.Property, error) {
	for _, prop := range c.Properties {
		// Check if the name of the property is the given name, that's the property we need
		if prop.Name == strings.Split(propName, ".")[0] {
			return prop, nil
		}
	}
	return nil, fmt.Errorf(ErrorNoSuchProperty, propName, c.Class)
}

// GetPropertyDataType checks whether the given string is a valid data type
func GetPropertyDataType(class *models.Class, propertyName string) (*DataType, error) {
	// Get the class-property
	prop, err := GetPropertyByName(class, propertyName)
	if err != nil {
		return nil, err
	}

	// Init the return value
	var returnDataType DataType

	// For each data type
	for _, dataType := range prop.DataType {
		if len(dataType) == 0 {
			return nil, fmt.Errorf("invalid-dataType")
		}
		if IsRefDataType([]string{dataType}) {
			returnDataType = DataTypeCRef
		} else {
			valueDataType, err := GetValueDataTypeFromString(dataType)
			if err != nil {
				return nil, err
			}
			returnDataType = *valueDataType
		}
	}
	return &returnDataType, nil
}

func GetNestedPropertyByName[P PropertyInterface](p P, propName string) (*models.NestedProperty, error) {
	// For each nested-property
	for _, prop := range p.GetNestedProperties() {
		// Check if the name of the property is the given name, that's the property we need
		if prop.Name == strings.Split(propName, ".")[0] {
			return prop, nil
		}
	}

	return nil, fmt.Errorf(ErrorNoSuchProperty, propName, p.GetName())
}

func GetNestedPropertyDataType[P PropertyInterface](p P, propertyName string) (*DataType, error) {
	// Get the class-property
	prop, err := GetNestedPropertyByName(p, propertyName)
	if err != nil {
		return nil, err
	}

	// Init the return value
	var returnDataType DataType

	// For each data type
	for _, dataType := range prop.DataType {
		if len(dataType) == 0 {
			return nil, fmt.Errorf("invalid-dataType")
		}
		if IsRefDataType([]string{dataType}) {
			returnDataType = DataTypeCRef
		} else {
			valueDataType, err := GetValueDataTypeFromString(dataType)
			if err != nil {
				return nil, err
			}
			returnDataType = *valueDataType
		}
	}
	return &returnDataType, nil
}

// GetValueDataTypeFromString checks whether the given string is a valid data type
func GetValueDataTypeFromString(dt string) (*DataType, error) {
	var returnDataType DataType

	if IsValidValueDataType(dt) {
		returnDataType = DataType(dt)
	} else {
		return nil, errors_.New(ErrorNoSuchDatatype)
	}

	return &returnDataType, nil
}

// IsValidValueDataType checks whether the given string is a valid data type
func IsValidValueDataType(dt string) bool {
	switch dt {
	case
		string(DataTypeString),
		string(DataTypeText),
		string(DataTypeInt),
		string(DataTypeNumber),
		string(DataTypeBoolean),
		string(DataTypeDate),
		string(DataTypeGeoCoordinates),
		string(DataTypePhoneNumber),
		string(DataTypeBlob),
		string(DataTypeBlobHash),
		string(DataTypeUUID),
		string(DataTypeUUIDArray),
		string(DataTypeStringArray),
		string(DataTypeTextArray),
		string(DataTypeIntArray),
		string(DataTypeNumberArray),
		string(DataTypeBooleanArray),
		string(DataTypeDateArray),
		string(DataTypeObject),
		string(DataTypeObjectArray):
		return true
	}
	return false
}

// IsRefDataType reports whether dt is a cross-reference DataType. Expects
// stored DataType — qualified "<namespace>:Class" or bare "Class".
// User-supplied DataType must go through FindPropertyDataTypeWithRefsAndAuth.
func IsRefDataType(dt []string) bool {
	if len(dt) == 0 || len(dt[0]) == 0 {
		return false
	}
	name := dt[0]
	if _, cls, ok := strings.Cut(name, NamespaceSeparator); ok {
		if cls == "" {
			return false
		}
		name = cls
	}
	firstLetter := string(name[0])
	return strings.ToUpper(firstLetter) == firstLetter
}

func IsBlobDataType(dt []string) bool {
	for i := range dt {
		if dt[i] == string(DataTypeBlob) {
			return true
		}
	}
	return false
}

func IsBlobHashDataType(dt []string) bool {
	for i := range dt {
		if dt[i] == string(DataTypeBlobHash) {
			return true
		}
	}
	return false
}

// IsBlobLikeDataType returns true for both blob and blobHash data types.
func IsBlobLikeDataType(dt []string) bool {
	return IsBlobDataType(dt) || IsBlobHashDataType(dt)
}

func IsArrayDataType(dt []string) bool {
	for i := range dt {
		switch DataType(dt[i]) {
		case DataTypeStringArray, DataTypeTextArray, DataTypeIntArray,
			DataTypeNumberArray, DataTypeBooleanArray, DataTypeDateArray,
			DataTypeUUIDArray:
			return true
		default:
			// move to the next loop
		}
	}
	return false
}

func GetPropertyNamesFromClass(class *models.Class, includeRef bool) []string {
	var propertyNames []string
	for _, prop := range class.Properties {
		if !includeRef && IsRefDataType(prop.DataType) {
			continue
		}
		propertyNames = append(propertyNames, prop.Name)
	}
	return propertyNames
}
