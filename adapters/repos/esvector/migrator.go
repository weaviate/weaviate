//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"context"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Migrator is a wrapper around a "primitive" esvector.Repo which implements
// the migrate.Migrator interface
type Migrator struct {
	repo *Repo
}

// NewMigrator from esvector.Repo to implement migrate.Migrator interface
func NewMigrator(repo *Repo) *Migrator {
	return &Migrator{repo: repo}
}

// AddClass creates an index, then puts the desired mappings
func (m *Migrator) AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error {
	index := classIndexFromClass(kind, class)
	err := m.repo.PutIndex(ctx, index)
	if err != nil {
		return fmt.Errorf("add class %s: create index: %v", class.Class, err)
	}

	err = m.setMappings(ctx, index, class.Properties)
	if err != nil {
		return fmt.Errorf("add class %s: map properties: %v", class.Class, err)
	}

	return nil
}

// DropClass deletes a class specific index
func (m *Migrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	index := classIndexFromClassName(kind, className)
	err := m.repo.DeleteIndex(ctx, index)
	if err != nil {
		return fmt.Errorf("drop class %s: delete index: %v", className, err)
	}

	return nil
}

// UpdateClass does nothing if the keywords should be changed and errors if the
// className should be changed - the class name must be immutable when the
// esvector index is active
func (m *Migrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.Keywords) error {
	if newClassName != nil {
		return fmt.Errorf("esvector does not support renaming of classes")
	}

	return nil
}

// AddProperty adds the new property without affecting existing properties
func (m *Migrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.Property) error {
	// put mappings does not delete existing properties, so we can use it to add
	// a new one, too
	index := classIndexFromClassName(kind, className)
	err := m.setMappings(ctx, index, []*models.Property{prop})
	if err != nil {
		return fmt.Errorf("add property %s to class %s: map properties: %v",
			className, prop.Name, err)
	}

	return nil
}

// DropProperty has no effect since mapped property types cannot be deleted in
// elasticsearch
func (m *Migrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	// ignore but don't error
	return nil
}

// UpdateProperty will do nothing if keywords should be updated and error if a
// name should be updated. Property names must be immutable when the esvector
// index is enabled
func (m *Migrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.Keywords) error {
	if newName != nil {
		return fmt.Errorf("esvector does not support renaming of properties")
	}

	return nil
}

// UpdatePropertyAddDataType is ignored, since the vectorindex does not support
// cross-ref types
func (m *Migrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	return nil
}

const indexPrefix = "class_"

func classIndexFromClass(kind kind.Kind, class *models.Class) string {
	return classIndexFromClassName(kind, class.Class)
}

func classIndexFromClassName(kind kind.Kind, className string) string {
	return fmt.Sprintf("%s%s_%s",
		indexPrefix, kind.Name(), strings.ToLower(className))
}

const allThingIndices = indexPrefix + "thing_*"
const allActionIndices = indexPrefix + "action_*"
const allClassIndices = indexPrefix + "*"

func (m *Migrator) setMappings(ctx context.Context, index string,
	props []*models.Property) error {
	esProperties, err := m.esPropsFromClassProps(props, 0)
	if err != nil {
		return err
	}

	return m.repo.SetMappings(ctx, index, esProperties)
}

func (m *Migrator) esPropsFromClassProps(props []*models.Property, depth int) (map[string]interface{}, error) {
	esProperties := map[string]interface{}{}
	cache := map[string]interface{}{}

	for _, prop := range props {
		// index everything unless explicitly turned off
		index := true
		if prop.Index != nil && *prop.Index == false {
			index = false
		}

		switch prop.DataType[0] {
		case string(schema.DataTypeString):
			esProperties[prop.Name] = typeMap(Keyword, index)
		case string(schema.DataTypeText):
			esProperties[prop.Name] = typeMap(Text, index)
		case string(schema.DataTypeInt):
			esProperties[prop.Name] = typeMap(Integer, index)
		case string(schema.DataTypeNumber):
			esProperties[prop.Name] = typeMap(Float, index)
		case string(schema.DataTypeBoolean):
			esProperties[prop.Name] = typeMap(Boolean, index)
		case string(schema.DataTypeDate):
			esProperties[prop.Name] = typeMap(Date, index)
		case string(schema.DataTypeGeoCoordinates):
			esProperties[prop.Name] = typeMap(GeoPoint, index)
		case string(schema.DataTypePhoneNumber):
			esProperties[prop.Name] = typeMapPhoneNumber(index)
		default:
			// must be a ref

			// mapping for the actual field containing the beacon
			if depth == 0 {
				esProperties[prop.Name] = map[string]interface{}{
					"properties": map[string]interface{}{
						"beacon": map[string]interface{}{
							"type": Keyword,
						},
					},
				}

			}

			// mapping for the cache
			if depth+1 > m.repo.denormalizationDepthLimit {
				continue
			}

			refProp, err := m.mapRefProp(prop.DataType, depth+1)
			if err != nil {
				return nil, fmt.Errorf("ref prop '%s': %v", prop.Name, err)
			}

			if depth == 0 {
				cache[prop.Name] = refProp
			} else {
				esProperties[prop.Name] = refProp
			}
		}
	}

	return esProperties, nil
}

func typeMap(ft FieldType, index bool) map[string]interface{} {
	return map[string]interface{}{
		"type":  ft,
		"index": index,
	}
}

func typeMapPhoneNumber(index bool) map[string]interface{} {
	return map[string]interface{}{
		"properties": map[string]interface{}{
			"countryCode":            typeMap(Long, index),
			"defaultCountry":         typeMap(Keyword, index),
			"input":                  typeMap(Keyword, index),
			"internationalFormatted": typeMap(Keyword, index),
			"national":               typeMap(Long, index),
			"nationalFormatted":      typeMap(Keyword, index),
			"valid":                  typeMap(Boolean, index),
		},
	}
}

func (m *Migrator) mapRefProp(classNames []string, depth int) (map[string]interface{}, error) {
	s := m.repo.schemaGetter.GetSchemaSkipAuth()

	properties := map[string]interface{}{}

	for _, className := range classNames {
		class := s.FindClassByName(schema.ClassName(className))
		if class == nil {
			return nil, fmt.Errorf("class '%s' not found", className)
		}

		esProperties, err := m.esPropsFromClassProps(class.Properties, depth)
		if err != nil {
			return nil, fmt.Errorf("target class '%s': %#v", className, err)
		}

		properties[className] = map[string]interface{}{
			"properties": esProperties,
			"type":       "nested",
		}
	}

	return map[string]interface{}{
		"properties": properties,
	}, nil

}
