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

	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
	"github.com/sirupsen/logrus"
)

type autoSchemaManager struct {
	mutex         sync.RWMutex
	schemaManager schemaManager
	vectorRepo    VectorRepo
	config        config.AutoSchema
	logger        logrus.FieldLogger
}

func newAutoSchemaManager(schemaManager schemaManager, vectorRepo VectorRepo,
	config *config.WeaviateConfig, logger logrus.FieldLogger) *autoSchemaManager {
	return &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config:        config.Config.AutoSchema,
		logger:        logger,
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(object.Class) == 0 {
		// stop performing auto schema
		return fmt.Errorf(validation.ErrorMissingClass)
	}
	schemaClass, err := m.getClass(principal, object)
	if err != nil {
		return err
	}
	properties := m.getProperties(object)
	if schemaClass == nil {
		return m.createClass(ctx, principal, object.Class, properties)
	}
	return m.updateClass(ctx, principal, object.Class, properties, schemaClass.Properties)
}

func (m *autoSchemaManager) getClass(principal *models.Principal,
	object *models.Object) (*models.Class, error) {
	s, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return nil, err
	}
	schemaClass := s.GetClass(schema.ClassName(object.Class))
	return schemaClass, nil
}

func (m *autoSchemaManager) createClass(ctx context.Context, principal *models.Principal,
	className string, properties []*models.Property) error {
	class := &models.Class{
		Class:       className,
		Properties:  properties,
		Description: "Auto generated class",
	}
	m.logger.
		WithField("auto_schema", "createClass").
		Debugf("create class %s", className)
	return m.schemaManager.AddClass(ctx, principal, class)
}

func (m *autoSchemaManager) updateClass(ctx context.Context, principal *models.Principal,
	className string, properties []*models.Property, existingProperties []*models.Property) error {
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
		m.logger.
			WithField("auto_schema", "updateClass").
			Debugf("update class %s add property %s", className, newProp.Name)
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
			dataType := []schema.DataType{}
			for i := range v {
				switch arrayVal := v[i].(type) {
				case map[string]interface{}:
					if len(arrayVal) > 0 {
						for k, v := range arrayVal {
							if k == "beacon" {
								if beacon, ok := v.(string); ok {
									ref, err := crossref.Parse(beacon)
									if err == nil {
										res, err := m.vectorRepo.ObjectByID(context.Background(), ref.TargetID,
											search.SelectProperties{}, additional.Properties{})
										if err == nil && res != nil {
											dataType = append(dataType, schema.DataType(res.ClassName))
										}
									}
								}
							}
						}
					}
				case string:
					_, err := time.Parse(time.RFC3339, arrayVal)
					if err == nil {
						return []schema.DataType{schema.DataTypeDateArray}
					}
					if schema.DataType(m.config.DefaultString) == schema.DataTypeText {
						return []schema.DataType{schema.DataTypeTextArray}
					}
					return []schema.DataType{schema.DataTypeStringArray}
				case json.Number:
					if schema.DataType(m.config.DefaultNumber) == schema.DataTypeInt {
						return []schema.DataType{schema.DataTypeIntArray}
					}
					return []schema.DataType{schema.DataTypeNumberArray}
				case bool:
					return []schema.DataType{schema.DataTypeBooleanArray}
				}
			}
			return dataType
		}
		// fallback to String
		return []schema.DataType{schema.DataTypeString}
	default:
		return []schema.DataType{schema.DataTypeString}
	}
}
