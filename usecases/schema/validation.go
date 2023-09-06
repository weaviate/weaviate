//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	exitingName := ""
	m.schemaCache.RLockGuard(func() error {
		if cls := m.schemaCache.unsafeFindClassIf(pred); cls != nil {
			exitingName = cls.Class
		}
		return nil
	})

	if exitingName == "" {
		return nil
	}
	if name != exitingName {
		// It's a permutation
		return fmt.Errorf(
			"class name %q already exists as a permutation of: %q. class names must be unique when lowercased",
			name, exitingName)
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
	return fmt.Errorf("Tokenization is not allowed for reference data type")
}

func (m *Manager) validatePropertyIndexing(prop *models.Property) error {
	if prop.IndexInverted != nil {
		if prop.IndexFilterable != nil || prop.IndexSearchable != nil {
			return fmt.Errorf("`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`.")
		}
	}

	if prop.IndexSearchable != nil {
		switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// string/string[] are migrated to text/text[] later,
			// at this point they are still valid data types, therefore should be handled here
			// true or false allowed
		case schema.DataTypeText, schema.DataTypeTextArray:
			// true or false allowed
		default:
			if *prop.IndexSearchable {
				return fmt.Errorf("`indexSearchable` is allowed only for text/text[] data types. " +
					"For other data types set false or leave empty")
			}
		}
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
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			class.VectorIndexType)
	}
}
