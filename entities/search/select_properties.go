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

package search

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// AllNonRefNonBlobProperties selects every property of class except
// references and blobs, descending into object / object-array properties via
// AllNonRefNonBlobNestedProperties. It is the canonical "return all
// properties" selection shared by the gRPC and REST search APIs.
//
// This is a faithful port of the original gRPC getAllNonRefNonBlobProperties
// with no intentional behavioral change.
func AllNonRefNonBlobProperties(class *models.Class) ([]SelectProperty, error) {
	var props []SelectProperty
	for _, prop := range class.Properties {
		dt, err := schema.GetPropertyDataType(class, prop.Name)
		if err != nil {
			return []SelectProperty{}, fmt.Errorf("get property data type: %w", err)
		}
		switch *dt {
		case schema.DataTypeCRef, schema.DataTypeBlob, schema.DataTypeBlobHash:
			continue
		case schema.DataTypeObject, schema.DataTypeObjectArray:
			nested, err := schema.GetPropertyByName(class, prop.Name)
			if err != nil {
				return []SelectProperty{}, fmt.Errorf("get nested property by name: %w", err)
			}
			nestedProps, err := AllNonRefNonBlobNestedProperties(&propertyAdapter{nested})
			if err != nil {
				return []SelectProperty{}, fmt.Errorf("get all non ref non blob nested properties: %w", err)
			}
			props = append(props, SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		default:
			props = append(props, SelectProperty{Name: prop.Name, IsPrimitive: true})
		}
	}
	return props, nil
}

// AllNonRefNonBlobNestedProperties is the nested-property counterpart of
// AllNonRefNonBlobProperties, generic so that callers can pass their own
// schema.PropertyInterface adapters. It is a faithful port of the original
// gRPC getAllNonRefNonBlobNestedProperties.
func AllNonRefNonBlobNestedProperties[P schema.PropertyInterface](property P) ([]SelectProperty, error) {
	var props []SelectProperty
	for _, prop := range property.GetNestedProperties() {
		dt, err := schema.GetNestedPropertyDataType(property, prop.Name)
		if err != nil {
			return []SelectProperty{}, fmt.Errorf("get nested property data type: %w", err)
		}
		switch *dt {
		case schema.DataTypeCRef, schema.DataTypeBlob, schema.DataTypeBlobHash:
			continue
		case schema.DataTypeObject, schema.DataTypeObjectArray:
			nested, err := schema.GetNestedPropertyByName(property, prop.Name)
			if err != nil {
				return []SelectProperty{}, fmt.Errorf("get nested property by name: %w", err)
			}
			nestedProps, err := AllNonRefNonBlobNestedProperties(&nestedPropertyAdapter{nested})
			if err != nil {
				return []SelectProperty{}, fmt.Errorf("get all non ref non blob nested properties: %w", err)
			}
			props = append(props, SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		default:
			props = append(props, SelectProperty{Name: prop.Name, IsPrimitive: true})
		}
	}
	return props, nil
}

// propertyAdapter / nestedPropertyAdapter adapt the models types to
// schema.PropertyInterface for the class-level walk.
type propertyAdapter struct {
	*models.Property
}

func (p *propertyAdapter) GetName() string {
	return p.Name
}

func (p *propertyAdapter) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperties
}

type nestedPropertyAdapter struct {
	*models.NestedProperty
}

func (p *nestedPropertyAdapter) GetName() string {
	return p.Name
}

func (p *nestedPropertyAdapter) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperties
}
