/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

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

func HackFromDatabaseSchema(dbSchema Schema) WeaviateSchema {
	return WeaviateSchema{
		ActionSchema: schemaProperties{
			Schema: dbSchema.Actions,
		},
		ThingSchema: schemaProperties{
			Schema: dbSchema.Things,
		},
	}
}

const (
	// validationErrorMessage is a constant for returning the same message
	validationErrorMessage string = "All predicates with the same name across different classes should contain the same kind of data"

	// ErrorNoSuchClass message
	ErrorNoSuchClass string = "no such class with name '%s' found in the schema. Check your schema files for which classes are available"
	// ErrorNoSuchProperty message
	ErrorNoSuchProperty string = "no such prop with name '%s' found in class '%s' in the schema. Check your schema files for which properties in this class are available"
	// ErrorNoSuchDatatype message
	ErrorNoSuchDatatype string = "given value-DataType does not exist."
	// ErrorInvalidRefType message
	ErrorInvalidRefType string = "given ref type is not valid"
)

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
		string(DataTypeGeoCoordinates):
		return true
	}
	return false
}
