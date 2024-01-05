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
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func (m *Manager) validateClassNameUniqueness(name string) error {
	pred := func(c *models.Class) bool {
		return strings.EqualFold(name, c.Class)
	}
	existingName := ""
	m.schemaCache.RLockGuard(func() error {
		if cls := m.schemaCache.unsafeFindClassIf(pred); cls != nil {
			existingName = cls.Class
		}
		return nil
	})

	if existingName == "" {
		return nil
	}
	if name != existingName {
		// It's a permutation
		return fmt.Errorf(
			"class name %q already exists as a permutation of: %q. class names must be unique when lowercased",
			name, existingName)
	}
	return fmt.Errorf("class name %q already exists", name)
}

// Check that the format of the name is correct
func (m *Manager) validateClassName(ctx context.Context, className string) error {
	_, err := schema.ValidateClassName(className)
	return err
}

func (m *Manager) validatePropertyTokenization(tokenization string, propertyDataType schema.PropertyDataType) error {
	if propertyDataType.IsPrimitive() {
		primitiveDataType := propertyDataType.AsPrimitive()

		switch primitiveDataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// deprecated as of v1.19, will be migrated to DataTypeText/DataTypeTextArray
			switch tokenization {
			case models.PropertyTokenizationField, models.PropertyTokenizationWord:
				return nil
			}
		case schema.DataTypeText, schema.DataTypeTextArray:
			switch tokenization {
			case models.PropertyTokenizationField, models.PropertyTokenizationWord,
				models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase:
				return nil
			}
		default:
			if tokenization == "" {
				return nil
			}
			return fmt.Errorf("Tokenization is not allowed for data type '%s'", primitiveDataType)
		}
		return fmt.Errorf("Tokenization '%s' is not allowed for data type '%s'", tokenization, primitiveDataType)
	}

	if tokenization == "" {
		return nil
	}

	if propertyDataType.IsNested() {
		return fmt.Errorf("Tokenization is not allowed for object/object[] data types")
	}
	return fmt.Errorf("Tokenization is not allowed for reference data type")
}

func (m *Manager) validatePropertyIndexing(prop *models.Property) error {
	if prop.IndexInverted != nil {
		if prop.IndexFilterable != nil || prop.IndexSearchable != nil {
			return fmt.Errorf("`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`")
		}
	}

	primitiveDataType, isPrimitive := schema.AsPrimitive(prop.DataType)

	// TODO nested - should not be allowed for blobs (verify backward compat)
	// if prop.IndexFilterable != nil {
	// 	if isPrimitive && primitiveDataType == schema.DataTypeBlob {
	// 		return fmt.Errorf("`indexFilterable` is not allowed for blob data type")
	// 	}
	// }

	if prop.IndexSearchable != nil {
		validateSet := true
		if isPrimitive {
			switch primitiveDataType {
			case schema.DataTypeString, schema.DataTypeStringArray:
				// string/string[] are migrated to text/text[] later,
				// at this point they are still valid data types, therefore should be handled here
				// true or false allowed
				validateSet = false
			case schema.DataTypeText, schema.DataTypeTextArray:
				// true or false allowed
				validateSet = false
			default:
				// do nothing
			}
		}

		if validateSet && *prop.IndexSearchable {
			return fmt.Errorf("`indexSearchable` is not allowed for other than text/text[] data types")
		}
	}

	return nil
}

type validatorNestedProperty func(property *models.NestedProperty,
	primitiveDataType, nestedDataType schema.DataType,
	isPrimitive, isNested bool, propNamePrefix string) error

var validatorsNestedProperty = []validatorNestedProperty{
	validateNestedPropertyName,
	validateNestedPropertyDataType,
	validateNestedPropertyTokenization,
	validateNestedPropertyIndexFilterable,
	validateNestedPropertyIndexSearchable,
}

func validateNestedProperties(properties []*models.NestedProperty, propNamePrefix string) error {
	if len(properties) == 0 {
		return fmt.Errorf("Property '%s': At least one nested property is required for data type object/object[]",
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
			return fmt.Errorf("Property '%s': data type '%s' is deprecated and not allowed as nested property", propName, primitiveDataType)
		case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
			return fmt.Errorf("Property '%s': data type '%s' not allowed as nested property", propName, primitiveDataType)
		default:
			// do nothing
		}
		return nil
	}
	if isNested {
		return nil
	}
	return fmt.Errorf("Property '%s': reference data type not allowed", propName)
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
			return fmt.Errorf("Property '%s': Tokenization '%s' is not allowed for data type '%s'",
				propName, property.Tokenization, primitiveDataType)
		default:
			if property.Tokenization == "" {
				return nil
			}
			return fmt.Errorf("Property '%s': Tokenization is not allowed for data type '%s'",
				propName, primitiveDataType)
		}
	}
	if property.Tokenization == "" {
		return nil
	}
	if isNested {
		return fmt.Errorf("Property '%s': Tokenization is not allowed for object/object[] data types", propName)
	}
	return fmt.Errorf("Property '%s': Tokenization is not allowed for reference data type", propName)
}

// indexFilterable allowed for primitive & ref data types
func validateNestedPropertyIndexFilterable(property *models.NestedProperty,
	primitiveDataType, _ schema.DataType,
	isPrimitive, _ bool, propNamePrefix string,
) error {
	propName := propNamePrefix + "." + property.Name

	// at this point indexSearchable should be set (either by user or by defaults)
	if property.IndexFilterable == nil {
		return fmt.Errorf("Property '%s': `indexFilterable` not set", propName)
	}

	if isPrimitive && primitiveDataType == schema.DataTypeBlob {
		if *property.IndexFilterable {
			return fmt.Errorf("Property: '%s': indexFilterable is not allowed for blob data type",
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
		return fmt.Errorf("Property '%s': `indexSearchable` not set", propName)
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
		return fmt.Errorf("Property '%s': `indexSearchable` is not allowed for other than text/text[] data types",
			propName)
	}

	return nil
}

func (m *Manager) validateVectorSettings(ctx context.Context, class *models.Class) error {
	if err := m.validateVectorizer(ctx, class); err != nil {
		return err
	}

	if err := m.validateVectorIndex(ctx, class); err != nil {
		return err
	}

	return nil
}

func (m *Manager) validateVectorizer(ctx context.Context, class *models.Class) error {
	if class.Vectorizer == config.VectorizerModuleNone {
		return nil
	}

	if err := m.vectorizerValidator.ValidateVectorizer(class.Vectorizer); err != nil {
		return errors.Wrap(err, "vectorizer")
	}

	return nil
}

func (m *Manager) validateVectorIndex(ctx context.Context, class *models.Class) error {
	switch class.VectorIndexType {
	case "hnsw":
		return nil
	case "flat":
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			class.VectorIndexType)
	}
}
