//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type autoSchemaManager struct {
	mutex         sync.RWMutex
	schemaManager schemaManager
	vectorRepo    VectorRepo
	config        config.AutoSchema
}

func newAutoSchemaManager(schemaManager schemaManager, vectorRepo VectorRepo,
	config *config.WeaviateConfig) *autoSchemaManager {
	return &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config:        config.Config.AutoSchema,
	}
}

func (m *autoSchemaManager) autoSchema(ctx context.Context, principal *models.Principal,
	object *models.Object) error {
	if m.config.Enabled {
		return m.performAutoSchema(ctx, principal, object)
	}
	return nil
}

func (m *autoSchemaManager) performAutoSchema(ctx context.Context, principal *models.Principal,
	object *models.Object) error {
	if len(object.Class) == 0 {
		// stop performing auto schema
		return fmt.Errorf(validation.ErrorMissingClass)
	}
	s, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return err
	}
	schemaClass := s.GetClass(schema.ClassName(object.Class))
	properties := m.getProperties(object)
	if schemaClass == nil {
		return m.createClass(ctx, principal, object.Class, properties)
	}
	return m.updateClass(ctx, principal, object.Class, properties, schemaClass.Properties)
}

func (m *autoSchemaManager) createClass(ctx context.Context, principal *models.Principal,
	className string, properties []*models.Property) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	class := &models.Class{
		Class:       className,
		Properties:  properties,
		Description: "Auto generated class",
	}
	return m.schemaManager.AddClass(ctx, principal, class)
}

func (m *autoSchemaManager) updateClass(ctx context.Context, principal *models.Principal,
	className string, properties []*models.Property, existingProperties []*models.Property) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	propertiesToAdd := []*models.Property{}
	for _, prop := range properties {
		found := false
		for _, classProp := range existingProperties {
			if classProp.Name == prop.Name {
				found = true
				break
			}
		}
		if !found {
			propertiesToAdd = append(propertiesToAdd, prop)
		}
	}
	for _, newProp := range propertiesToAdd {
		err := m.schemaManager.AddClassProperty(ctx, principal, className, newProp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *autoSchemaManager) getProperties(object *models.Object) []*models.Property {
	properties := []*models.Property{}
	if props, ok := object.Properties.(map[string]interface{}); ok {
		for name, value := range props {
			dt := m.determineType(value)
			property := &models.Property{
				Name:        name,
				DataType:    m.getDataTypes(dt),
				Description: "Auto generated property",
			}
			properties = append(properties, property)
		}
	}
	return properties
}

func (m *autoSchemaManager) getDataTypes(dataTypes []schema.DataType) []string {
	dtypes := make([]string, len(dataTypes))
	for i := range dataTypes {
		dtypes[i] = string(dataTypes[i])
	}
	return dtypes
}

func (m *autoSchemaManager) determineType(value interface{}) []schema.DataType {
	switch v := value.(type) {
	case string:
		_, err := time.Parse(time.RFC3339, v)
		if err == nil {
			return []schema.DataType{schema.DataType(m.config.DefaultDate)}
		}
		return []schema.DataType{schema.DataType(m.config.DefaultString)}
	case json.Number:
		return []schema.DataType{schema.DataType(m.config.DefaultNumber)}
	case bool:
		return []schema.DataType{schema.DataTypeBoolean}
	case map[string]interface{}:
		if v["latitude"] != nil && v["longitude"] != nil {
			return []schema.DataType{schema.DataTypeGeoCoordinates}
		}
		if v["input"] != nil {
			return []schema.DataType{schema.DataTypePhoneNumber}
		}
		// fallback to String
		return []schema.DataType{schema.DataTypeString}
	case []interface{}:
		if len(v) > 0 {
			crossRefs := []schema.DataType{}
			for i := range v {
				if values, ok := v[i].(map[string]interface{}); ok && len(values) > 0 {
					for k, v := range values {
						if k == "beacon" {
							if beacon, ok := v.(string); ok {
								ref, err := crossref.Parse(beacon)
								if err == nil {
									res, err := m.vectorRepo.ObjectByID(context.Background(), ref.TargetID,
										traverser.SelectProperties{}, traverser.AdditionalProperties{})
									if err == nil && res != nil {
										crossRefs = append(crossRefs, schema.DataType(res.ClassName))
									}
								}
							}
						}
					}
				}
			}
			return crossRefs
		}
		// fallback to String
		return []schema.DataType{schema.DataTypeString}
	default:
		return []schema.DataType{schema.DataTypeString}
	}
}
