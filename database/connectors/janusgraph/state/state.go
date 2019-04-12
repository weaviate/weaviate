/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package state

import (
	"fmt"
	"strconv"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
)

// Stores the internal JanusGraph connector state.
// - the version of the connector state
// - the mappings of class names and property names
type JanusGraphConnectorState struct {
	Version int64 `json:"version"`
	LastId  int64 `json:"next_id"`

	ClassMap    map[schema.ClassName]MappedClassName                            `json:"classMap"`
	PropertyMap map[schema.ClassName]map[schema.PropertyName]MappedPropertyName `json:"propertyMap"`
}

type MappedClassName string
type MappedPropertyName string

// Generates a new hex number based on the lastId.
// Increments lastId, does _not_ commit the internal state;
// you probably want to use this function whilst modifying more,
// so that would trigger an unnecessary sync across all instances.
func (s *JanusGraphConnectorState) getNextId() string {
	s.LastId += 1
	return strconv.FormatInt(s.LastId, 16)
}

// Add a mapping of a classname to mapped name.
func (s *JanusGraphConnectorState) AddMappedClassName(className schema.ClassName) MappedClassName {
	_, exists := s.ClassMap[className]

	if exists {
		panic(fmt.Sprintf("Fatal error; class name %v is already mapped to a janus name", className))
	}

	mappedName := MappedClassName(fmt.Sprintf("class_%s", s.getNextId()))
	s.ClassMap[className] = mappedName
	return mappedName
}

// Map a schema name to the internal janusgraph name
func (s *JanusGraphConnectorState) GetMappedClassName(className schema.ClassName) (MappedClassName, error) {
	mappedName, exists := s.ClassMap[className]
	if !exists {
		return "", fmt.Errorf("class name %v is not mapped to a janus name", className)
	}

	return mappedName, nil
}

// Map a schema name to the internal janusgraph name
func (s *JanusGraphConnectorState) MustGetMappedClassName(className schema.ClassName) MappedClassName {
	mappedName, err := s.GetMappedClassName(className)
	if err != nil {
		panic(err)
	}

	return mappedName
}

// Get the schema name, from a mapped name.
func (s *JanusGraphConnectorState) GetClassNameFromMapped(className MappedClassName) schema.ClassName {
	for org, mapped := range s.ClassMap {
		if mapped == className {
			return org
		}
	}

	panic(fmt.Sprintf("Fatal error; class name %v is not mapped from a name", className))
}

// Remove mapped class name, and all properties.
func (s *JanusGraphConnectorState) RemoveMappedClassName(className schema.ClassName) {
	delete(s.ClassMap, className)
	delete(s.PropertyMap, className)
}

// Add mapping from class/property name to mapped property namej
func (s *JanusGraphConnectorState) AddMappedPropertyName(className schema.ClassName, propName schema.PropertyName) MappedPropertyName {
	propsOfClass, exists := s.PropertyMap[className]

	if !exists {
		propsOfClass = make(map[schema.PropertyName]MappedPropertyName)
		s.PropertyMap[className] = propsOfClass
	}

	_, exists = propsOfClass[propName]
	if exists {
		panic(fmt.Sprintf("Fatal error; property %v for class name %v is already mapped to a janus name", propName, className))
	}

	mappedName := MappedPropertyName(fmt.Sprintf("prop_%s", s.getNextId()))
	propsOfClass[propName] = mappedName
	return mappedName
}

// GetMappedPropertyName or error
func (s *JanusGraphConnectorState) GetMappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) (MappedPropertyName, error) {
	propsOfClass, exists := s.PropertyMap[className]
	if !exists {
		return "", fmt.Errorf("class name %v does not exist or does not have mapped properties", className)
	}

	mappedName, exists := propsOfClass[propName]
	if !exists {
		return "", fmt.Errorf("property %v for class name %v is not mapped to a janus name", propName, className)
	}

	return mappedName, nil
}

// MustGetMappedPropertyName panics if it can't get the name
func (s *JanusGraphConnectorState) MustGetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) MappedPropertyName {
	name, err := s.GetMappedPropertyName(className, propName)
	if err != nil {
		panic(err)
	}

	return name
}

// GetMappedPropertyNames can retrieve the mapped names for many properties in bulk
func (s *JanusGraphConnectorState) GetMappedPropertyNames(rawProps []schema.ClassAndProperty) ([]MappedPropertyName, error) {
	result := make([]MappedPropertyName, len(rawProps), len(rawProps))

	for i, rawProp := range rawProps {
		mapped, err := s.GetMappedPropertyName(rawProp.ClassName, rawProp.PropertyName)
		if err != nil {
			return nil, err
		}
		result[i] = mapped
	}

	return result, nil
}

// Add mapping from class/property name to mapped property namej
func (s *JanusGraphConnectorState) GetPropertyNameFromMapped(className schema.ClassName, mappedPropName MappedPropertyName) (schema.PropertyName, error) {
	propsOfClass, ok := s.PropertyMap[className]
	if !ok {
		return "", fmt.Errorf("class name %v does not have mapped properties", className)
	}

	for name, mapped := range propsOfClass {
		if mapped == mappedPropName {
			return name, nil
		}
	}

	return "", fmt.Errorf("property %v for class name %v is not mapped from a janus name", mappedPropName, className)
}

// Add mapping from class/property name to mapped property namej
func (s *JanusGraphConnectorState) MustGetPropertyNameFromMapped(className schema.ClassName, mappedPropName MappedPropertyName) schema.PropertyName {
	name, err := s.GetPropertyNameFromMapped(className, mappedPropName)
	if err != nil {
		panic(err.Error())
	}

	return name
}

func (s *JanusGraphConnectorState) RemoveMappedPropertyName(className schema.ClassName, propName schema.PropertyName) {
	propsOfClass, exists := s.PropertyMap[className]

	if !exists {
		panic(fmt.Sprintf("Fatal error; class name %v does not have mapped properties", className))
	}

	_, exists = propsOfClass[propName]
	if !exists {
		panic(fmt.Sprintf("Fatal error; property %v for class name %v is not mapped to a janus name", propName, className))
	}

	delete(propsOfClass, propName)
}

func (s *JanusGraphConnectorState) RenameClass(oldName schema.ClassName, newName schema.ClassName) {
	mappedName := s.ClassMap[oldName]
	mappedProperties := s.PropertyMap[oldName]

	delete(s.ClassMap, oldName)
	delete(s.PropertyMap, oldName)

	s.ClassMap[newName] = mappedName
	s.PropertyMap[newName] = mappedProperties
}

func (s *JanusGraphConnectorState) RenameProperty(className schema.ClassName, oldName schema.PropertyName, newName schema.PropertyName) {
	propsOfClass, exists := s.PropertyMap[className]

	if !exists {
		panic(fmt.Sprintf("Fatal error; class name %v does not have mapped properties", className))
	}

	mappedName, exists := propsOfClass[oldName]
	if !exists {
		panic(fmt.Sprintf("Fatal error; property %v for class name %v is not mapped to a janus name", oldName, className))
	}

	delete(propsOfClass, oldName)
	propsOfClass[newName] = mappedName
}
