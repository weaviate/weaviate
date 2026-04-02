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
	"fmt"
	"unicode/utf8"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"golang.org/x/text/unicode/norm"
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
	validateNestedPropertyProcessing,
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

// validateNestedPropertyProcessing validates TextAnalyzerConfig on nested properties,
// analogous to validatePropertyProcessing for top-level properties.
func validateNestedPropertyProcessing(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, _ bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	// Normalize empty config to nil.
	if property.TextAnalyzer != nil && !property.TextAnalyzer.ASCIIFold && len(property.TextAnalyzer.ASCIIFoldIgnore) == 0 && property.TextAnalyzer.StopwordPreset == "" {
		property.TextAnalyzer = nil
	}
	if property.TextAnalyzer == nil {
		return nil
	}

	if !isPrimitive {
		return fmt.Errorf("property '%s': processing options are only allowed for text and text[] data types", propName)
	}
	switch primitiveDataType {
	case schema.DataTypeText, schema.DataTypeTextArray:
		// allowed
	default:
		return fmt.Errorf("property '%s': processing options are only allowed for text and text[] data types, got '%s'", propName, primitiveDataType)
	}

	if (property.IndexSearchable == nil || !*property.IndexSearchable) && (property.IndexFilterable == nil || !*property.IndexFilterable) {
		return fmt.Errorf("property '%s': processing options are only allowed for properties with an inverted index", propName)
	}

	if !property.TextAnalyzer.ASCIIFold && len(property.TextAnalyzer.ASCIIFoldIgnore) > 0 {
		return fmt.Errorf("property '%s': asciiFoldIgnore requires asciiFold to be enabled", propName)
	}

	for _, entry := range property.TextAnalyzer.ASCIIFoldIgnore {
		if utf8.RuneCountInString(norm.NFC.String(entry)) != 1 {
			return fmt.Errorf("property '%s': each asciiFoldIgnore entry must be a single character, got %q",
				propName, entry)
		}
	}

	if property.Tokenization != "" {
		switch property.Tokenization {
		case models.NestedPropertyTokenizationLowercase,
			models.NestedPropertyTokenizationWord,
			models.NestedPropertyTokenizationWhitespace,
			models.NestedPropertyTokenizationField,
			models.NestedPropertyTokenizationTrigram:
			// supported
		default:
			return fmt.Errorf("property '%s': unsupported tokenization '%s' for textAnalyzer", propName, property.Tokenization)
		}
	}

	if property.TextAnalyzer.StopwordPreset != "" {
		if _, ok := stopwords.Presets[property.TextAnalyzer.StopwordPreset]; !ok {
			return fmt.Errorf("property '%s': unknown stopword preset %q, valid options are: ['en', 'none']",
				propName, property.TextAnalyzer.StopwordPreset)
		}
	}

	return nil
}
