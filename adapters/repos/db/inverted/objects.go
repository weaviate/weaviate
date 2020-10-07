//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (a *Analyzer) Object(input map[string]interface{}, props []*models.Property) ([]Property, error) {
	propsMap := map[string]*models.Property{}
	for _, prop := range props {
		propsMap[prop.Name] = prop
	}

	var out []Property
	for key, prop := range propsMap {
		if len(prop.DataType) < 1 {
			return nil, fmt.Errorf("prop %q has no datatype", prop.Name)
		}

		var property *Property
		var err error
		if schema.IsRefDataType(prop.DataType) {
			value, ok := input[key]
			if !ok {
				// explicitly set zero-value, so we can index for "ref not set"
				value = make(models.MultipleRef, 0)
			}
			property, err = a.analyzeRefProp(prop, value)
		} else {
			value, ok := input[key]
			if !ok {
				// skip any primitive prop that's not set
				continue
			}
			property, err = a.analyzePrimitiveProp(prop, value)
		}
		if err != nil {
			return nil, errors.Wrap(err, "analyze primitive prop")
		}
		if property == nil {
			continue
		}

		out = append(out, *property)
	}

	return out, nil
}

func (a *Analyzer) analyzePrimitiveProp(prop *models.Property, value interface{}) (*Property, error) {
	var hasFrequency bool
	var items []Countable
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeText:
		hasFrequency = true
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		items = a.Text(asString)
	case schema.DataTypeString:
		hasFrequency = true
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		items = a.String(asString)
	case schema.DataTypeInt:
		hasFrequency = false
		if asFloat, ok := value.(float64); ok {
			// unmarshaling from json into a dynamic schema will assume every number
			// is a float64
			value = int64(asFloat)
		}

		asInt, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type int64, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Int(int(asInt))
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeNumber:
		hasFrequency = false
		asFloat, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type float64, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Float(asFloat) // convert to int before analyzing
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeBoolean:
		hasFrequency = false
		asBool, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type bool, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Bool(asBool) // convert to int before analyzing
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}

	default:
		// ignore unsupported prop type
		return nil, nil
	}

	return &Property{
		Name:         prop.Name,
		Items:        items,
		HasFrequency: hasFrequency,
	}, nil
}

func (a *Analyzer) analyzeRefProp(prop *models.Property, value interface{}) (*Property, error) {
	// TODO: return multiple properties when support ref-indexing. For now we
	// only support counting the refs

	asRefs, ok := value.(models.MultipleRef)
	if !ok {
		return nil, fmt.Errorf("expected property %q to be of type models.MutlipleRef, but got %T", prop.Name, value)
	}

	items, err := a.RefCount(asRefs)
	if err != nil {
		return nil, errors.Wrapf(err, "analyze ref-property %q", prop.Name)
	}

	return &Property{
		Name:         helpers.MetaCountProp(prop.Name),
		Items:        items,
		HasFrequency: false,
	}, nil
}
