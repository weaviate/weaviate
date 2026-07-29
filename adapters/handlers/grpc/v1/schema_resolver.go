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

package v1

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// schemaResolver memoizes property lookups for a single reply: it resolves each
// class once through getClass and caches its property datatypes, so mapping N
// results scans each class's properties once rather than once per result.
// getClass is the same authorized, cloning getter used to parse the request, so
// each class is authorized and cloned once per request.
type schemaResolver struct {
	getClass func(className string) (*models.Class, error)
	classes  map[string]*resolvedClass
}

type resolvedClass struct {
	class     *models.Class
	dataTypes map[string]*schema.DataType
	props     map[string]*models.Property
}

func newSchemaResolver(getClass func(className string) (*models.Class, error)) *schemaResolver {
	return &schemaResolver{
		getClass: getClass,
		classes:  map[string]*resolvedClass{},
	}
}

func (s *schemaResolver) resolve(className string) (*resolvedClass, error) {
	if rc, ok := s.classes[className]; ok {
		return rc, nil
	}
	class, err := s.getClass(className)
	if err != nil {
		return nil, err
	}
	if class == nil {
		return nil, fmt.Errorf("could not find class %s in schema", className)
	}
	rc := &resolvedClass{
		class:     class,
		dataTypes: map[string]*schema.DataType{},
		props:     map[string]*models.Property{},
	}
	s.classes[className] = rc
	return rc, nil
}

func (rc *resolvedClass) primitiveDataType(propName string) (*schema.DataType, error) {
	if dt, ok := rc.dataTypes[propName]; ok {
		return dt, nil
	}
	dt, err := schema.GetPropertyDataType(rc.class, propName)
	if err != nil {
		return nil, err
	}
	rc.dataTypes[propName] = dt
	return dt, nil
}

func (rc *resolvedClass) property(propName string) (*models.Property, error) {
	if p, ok := rc.props[propName]; ok {
		return p, nil
	}
	p, err := schema.GetPropertyByName(rc.class, propName)
	if err != nil {
		return nil, err
	}
	rc.props[propName] = p
	return p, nil
}
