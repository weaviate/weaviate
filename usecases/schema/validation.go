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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type validatorNestedProperty func(property *models.NestedProperty,
	primitiveDataType, nestedDataType schema.DataType,
	isPrimitive, isNested bool, propNamePrefix string) error

var validatorsNestedProperty = []validatorNestedProperty{
	validateNestedPropertyName,
	validateNestedPropertyDataType,
	validateNestedPropertyTokenization,
	validateNestedPropertyIndexFilterable,
	validateNestedPropertyIndexSearchable,
	validateNestedPropertyIndexRangeFilters,
}

func validateNestedProperties(properties []*models.NestedProperty, propNamePrefix string) error {
	if len(properties) == 0 {
		return fmt.Errorf("property '%s': At least one nested property is required for data type object/object[]",
			propNamePrefix)
	}

	for _, property := range properties {
		primitiveDataType, isPrimitive := schema.AsPrimitive(property.DataType)
		nestedDataType, isNested := schema.AsNested(property.DataType)

		for _, validator := range validatorsNestedProperty {
			if err := validator(property, primitiveDataType, nestedDataType, isPrimitive, isNested, propNamePrefix); err != nil {
				return err
			}
		}
		if isNested {
			if err := validateNestedProperties(property.NestedProperties, propNamePrefix+"."+property.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateNestedPropertyName(property *models.NestedProperty,
	_, _ schema.DataType,
	_, _ bool, propNamePrefix string,
) error {
	return schema.ValidateNestedPropertyName(property.Name, propNamePrefix)
}

func validateNestedPropertyDataType(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, isNested bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	if isPrimitive {
		// DataTypeString and DataTypeStringArray as deprecated since 1.19 are not allowed
		switch primitiveDataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			return fmt.Errorf("property '%s': data type '%s' is deprecated and not allowed as nested property", propName, primitiveDataType)
		case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
			return fmt.Errorf("property '%s': data type '%s' not allowed as nested property", propName, primitiveDataType)
		default:
			// do nothing
		}
		return nil
	}
	if isNested {
		return nil
	}
	return fmt.Errorf("property '%s': reference data type not allowed", propName)
}

// Tokenization allowed only for text/text[] data types
func validateNestedPropertyTokenization(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, isNested bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	if isPrimitive {
		switch primitiveDataType {
		case schema.DataTypeText, schema.DataTypeTextArray:
			switch property.Tokenization {
			case models.PropertyTokenizationField, models.PropertyTokenizationWord,
				models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase:
				return nil
			}
			return fmt.Errorf("property '%s': Tokenization '%s' is not allowed for data type '%s'",
				propName, property.Tokenization, primitiveDataType)
		default:
			if property.Tokenization == "" {
				return nil
			}
			return fmt.Errorf("property '%s': Tokenization is not allowed for data type '%s'",
				propName, primitiveDataType)
		}
	}
	if property.Tokenization == "" {
		return nil
	}
	if isNested {
		return fmt.Errorf("property '%s': Tokenization is not allowed for object/object[] data types", propName)
	}
	return fmt.Errorf("property '%s': Tokenization is not allowed for reference data type", propName)
}

// indexFilterable allowed for primitive & ref data types
func validateNestedPropertyIndexFilterable(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, _ bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	// at this point indexSearchable should be set (either by user or by defaults)
	if property.IndexFilterable == nil {
		return fmt.Errorf("property '%s': `indexFilterable` not set", propName)
	}

	if isPrimitive && primitiveDataType == schema.DataTypeBlob {
		if *property.IndexFilterable {
			return fmt.Errorf("property: '%s': indexFilterable is not allowed for blob data type",
				propName)
		}
	}

	return nil
}

// indexSearchable allowed for text/text[] data types
func validateNestedPropertyIndexSearchable(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, _ bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	// at this point indexSearchable should be set (either by user or by defaults)
	if property.IndexSearchable == nil {
		return fmt.Errorf("property '%s': `indexSearchable` not set", propName)
	}

	if isPrimitive {
		switch primitiveDataType {
		case schema.DataTypeText, schema.DataTypeTextArray:
			return nil
		default:
			// do nothing
		}
	}
	if *property.IndexSearchable {
		return fmt.Errorf("property '%s': `indexSearchable` is not allowed for other than text/text[] data types",
			propName)
	}

	return nil
}

// indexRangeFilters allowed for number/int/date data types
func validateNestedPropertyIndexRangeFilters(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, _ bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	// at this point indexRangeFilters should be set (either by user or by defaults)
	if property.IndexRangeFilters == nil {
		return fmt.Errorf("property '%s': `indexRangeFilters` not set", propName)
	}

	if isPrimitive {
		switch primitiveDataType {
		case schema.DataTypeNumber, schema.DataTypeInt, schema.DataTypeDate:
			return nil
		default:
			// do nothing
		}
	}
	if *property.IndexRangeFilters {
		return fmt.Errorf("property '%s': `indexRangeFilters` is not allowed for other than number/int/date data types",
			propName)
	}

	return nil
}
