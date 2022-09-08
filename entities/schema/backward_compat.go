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

package schema

import (
	errors_ "errors"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

type schemaProperties struct {
	Schema *models.Schema
}

// WeaviateSchema represents the used schema's
type WeaviateSchema struct {
	ActionSchema schemaProperties
	ThingSchema  schemaProperties
}

// GetClassByName returns the class by its name
func GetClassByName(s *models.Schema, className string) (*models.Class, error) {
	if s == nil {
		return nil, fmt.Errorf(ErrorNoSuchClass, className)
	}
	// For each class
	for _, class := range s.Classes {
		// Check if the name of the class is the given name, that's the class we need
		if class.Class == className {
			return class, nil
		}
	}

	return nil, fmt.Errorf(ErrorNoSuchClass, className)
}

// GetPropertyByName returns the class by its name
func GetPropertyByName(c *models.Class, propName string) (*models.Property, error) {
	// For each class-property
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
		// Get the first letter to see if it is a capital
		firstLetter := string(dataType[0])
		if strings.ToUpper(firstLetter) == firstLetter {
			returnDataType = DataTypeCRef
		} else {
			// Get the value-data type (non-cref), return error if there is one, otherwise assign it to return data type
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
		if dt == string(DataTypeBoolean) {
			returnDataType = DataTypeBoolean
		} else if dt == string(DataTypeInt) {
			returnDataType = DataTypeInt
		} else if dt == string(DataTypeDate) {
			returnDataType = DataTypeDate
		} else if dt == string(DataTypeNumber) {
			returnDataType = DataTypeNumber
		} else if dt == string(DataTypeString) {
			returnDataType = DataTypeString
		} else if dt == string(DataTypeText) {
			returnDataType = DataTypeText
		} else if dt == string(DataTypeGeoCoordinates) {
			returnDataType = DataTypeGeoCoordinates
		} else if dt == string(DataTypePhoneNumber) {
			returnDataType = DataTypePhoneNumber
		} else if dt == string(DataTypeBlob) {
			returnDataType = DataTypeBlob
		} else if dt == string(DataTypeStringArray) {
			returnDataType = DataTypeStringArray
		} else if dt == string(DataTypeTextArray) {
			returnDataType = DataTypeTextArray
		} else if dt == string(DataTypeIntArray) {
			returnDataType = DataTypeIntArray
		} else if dt == string(DataTypeNumberArray) {
			returnDataType = DataTypeNumberArray
		} else if dt == string(DataTypeBooleanArray) {
			returnDataType = DataTypeBooleanArray
		} else if dt == string(DataTypeDateArray) {
			returnDataType = DataTypeDateArray
		}
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
		string(DataTypeStringArray),
		string(DataTypeTextArray),
		string(DataTypeIntArray),
		string(DataTypeNumberArray),
		string(DataTypeBooleanArray),
		string(DataTypeDateArray):
		return true
	}
	return false
}

func IsRefDataType(dt []string) bool {
	firstLetter := string(dt[0][0])
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

func IsArrayDataType(dt []string) bool {
	for i := range dt {
		switch DataType(dt[i]) {
		case DataTypeStringArray, DataTypeTextArray, DataTypeIntArray, DataTypeNumberArray, DataTypeBooleanArray, DataTypeDateArray:
			return true
		}
	}
	return false
}
