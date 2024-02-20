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

package moduletools

import (
	"sort"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type PropsComparatorFactory func() (VectorizablePropsComparator, error)

type VectorizablePropsComparator interface {
	PropsIterator() VectorizablePropsIterator
	IsChanged(propName string) bool
	PrevVector() []float32
	PrevVectorForName(targetVector string) []float32
}

func NewVectorizablePropsComparator(propsSchema []*models.Property,
	nextProps, prevProps models.PropertySchema, prevVector []float32,
	prevVectors models.Vectors,
) VectorizablePropsComparator {
	props, propsMap := extractVectorizablePropNames(propsSchema)
	return &vectorizablePropsComparator{
		prevProps:            notNilPropsMap(prevProps),
		nextProps:            notNilPropsMap(nextProps),
		vectorizableProps:    props,
		vectorizablePropsMap: propsMap,
		prevVector:           prevVector,
		prevVectors:          prevVectors,
	}
}

type vectorizablePropsComparator struct {
	prevProps            map[string]interface{}
	nextProps            map[string]interface{}
	vectorizableProps    []string
	vectorizablePropsMap map[string]struct{}
	prevVector           []float32
	prevVectors          models.Vectors
}

func (c *vectorizablePropsComparator) PropsIterator() VectorizablePropsIterator {
	return &vectorizablePropsIterator{pos: 0, vectorizable: c.vectorizableProps, props: c.nextProps}
}

func (c *vectorizablePropsComparator) IsChanged(propName string) bool {
	// not vectorizable prop -> not changed
	if _, ok := c.vectorizablePropsMap[propName]; !ok {
		return false
	}

	prevVal := c.prevProps[propName]
	nextVal := c.nextProps[propName]

	// both nil / missing / empty array -> not changed
	switch typedPrevVal := prevVal.(type) {
	case nil:
		if nextVal == nil {
			return false
		}
		if arr, ok := nextVal.([]string); ok && len(arr) == 0 {
			return false
		}
		return true

	case string:
		if typedNextVal, ok := nextVal.(string); ok {
			return typedNextVal != typedPrevVal
		}
		return true

	case []string:
		switch typedNextVal := nextVal.(type) {
		case nil:
			return len(typedPrevVal) != 0
		case []string:
			if len(typedNextVal) != len(typedPrevVal) {
				return true
			}
			for i := range typedNextVal {
				if typedNextVal[i] != typedPrevVal[i] {
					return true
				}
			}
			return false
		default:
			return true
		}

	// unmarshalled empty array might be []interface{} instead of []string
	case []interface{}:
		switch typedNextVal := nextVal.(type) {
		case nil:
			return len(typedPrevVal) != 0
		case []string:
			return len(typedPrevVal) != 0 || len(typedNextVal) != 0
		default:
			return true
		}

	default:
		// fallback (vectorizable types should be string/[]string)
		return false
	}
}

func (c *vectorizablePropsComparator) PrevVector() []float32 {
	return c.prevVector
}

func (c *vectorizablePropsComparator) PrevVectorForName(targetVector string) []float32 {
	if c.prevVectors != nil {
		return c.prevVectors[targetVector]
	}
	return nil
}

func NewVectorizablePropsComparatorDummy(propsSchema []*models.Property, nextProps models.PropertySchema,
) VectorizablePropsComparator {
	props, propsMap := extractVectorizablePropNames(propsSchema)
	return &vectorizablePropsComparatorDummy{
		nextProps:            notNilPropsMap(nextProps),
		vectorizableProps:    props,
		vectorizablePropsMap: propsMap,
	}
}

type vectorizablePropsComparatorDummy struct {
	nextProps            map[string]interface{}
	vectorizableProps    []string
	vectorizablePropsMap map[string]struct{}
}

func (c *vectorizablePropsComparatorDummy) PropsIterator() VectorizablePropsIterator {
	return &vectorizablePropsIterator{pos: 0, vectorizable: c.vectorizableProps, props: c.nextProps}
}

func (c *vectorizablePropsComparatorDummy) IsChanged(propName string) bool {
	// not vectorizable prop -> not changed
	if _, ok := c.vectorizablePropsMap[propName]; !ok {
		return false
	}

	switch typedNextVal := c.nextProps[propName].(type) {
	case nil:
		return false

	case string:
		return true

	case []string:
		return len(typedNextVal) != 0

	default:
		// fallback (vectorizable types should be string/[]string)
		return false
	}
}

func (c *vectorizablePropsComparatorDummy) PrevVector() []float32 {
	return nil
}

func (c *vectorizablePropsComparatorDummy) PrevVectorForName(targetVector string) []float32 {
	return nil
}

type VectorizablePropsIterator interface {
	Next() (propName string, propValue interface{}, ok bool)
}

type vectorizablePropsIterator struct {
	pos          int
	vectorizable []string
	props        map[string]interface{}
}

func (it *vectorizablePropsIterator) Next() (propName string, propValue interface{}, ok bool) {
	if it.pos >= len(it.vectorizable) {
		return "", nil, false
	}

	propName = it.vectorizable[it.pos]
	it.pos++
	return propName, it.props[propName], true
}

func notNilPropsMap(props models.PropertySchema) map[string]interface{} {
	if props != nil {
		if propsMap, ok := props.(map[string]interface{}); ok {
			return propsMap
		}
	}
	return map[string]interface{}{}
}

func extractVectorizablePropNames(propsSchema []*models.Property) ([]string, map[string]struct{}) {
	props := make([]string, 0, len(propsSchema))
	propsMap := make(map[string]struct{})

	for _, propSchema := range propsSchema {
		if isVectorizableType(propSchema) {
			props = append(props, propSchema.Name)
			propsMap[propSchema.Name] = struct{}{}
		}
	}
	sort.Strings(props)

	return props, propsMap
}

func isVectorizableType(propSchema *models.Property) bool {
	switch pdt, _ := schema.AsPrimitive(propSchema.DataType); pdt {
	case schema.DataTypeText, schema.DataTypeTextArray, schema.DataTypeBlob:
		return true
	default:
		return false
	}
}
