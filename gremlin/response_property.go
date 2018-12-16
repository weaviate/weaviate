/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package gremlin

import (
	"fmt"
)

type Property struct {
	Id    string
	Value PropertyValue
}

type PropertyValue struct {
	Value interface{}
}

func (p *PropertyValue) String() (string, bool) {
	val, ok := p.Value.(string)
	return val, ok
}

func (p *PropertyValue) AssertString() string {
	val, ok := p.String()
	if ok {
		return val
	} else {
		panic(fmt.Sprintf("Expected a string, but got %#v", p.Value))
	}
}

func (p *PropertyValue) Float() (float64, bool) {
	val, ok := p.Value.(float64)
	return val, ok
}

func (p *PropertyValue) AssertFloat() float64 {
	val, ok := p.Float()
	if ok {
		return val
	} else {
		panic(fmt.Sprintf("Expected a float, but got %#v", p.Value))
	}
}

func (p *PropertyValue) Int() (int, bool) {
	val, ok := p.Value.(float64)
	return int(val), ok
}

func (p *PropertyValue) AssertInt() int {
	val, ok := p.Int()
	if ok {
		return val
	} else {
		panic(fmt.Sprintf("Expected a int, but got %#v", p.Value))
	}
}

func (p *PropertyValue) Int64() (int64, bool) {
	val, ok := p.Value.(float64)
	return int64(val), ok
}

func (p *PropertyValue) AssertInt64() int64 {
	val, ok := p.Int64()
	if ok {
		return val
	} else {
		panic(fmt.Sprintf("Expected a int, but got %#v", p.Value))
	}
}

func (p *PropertyValue) Bool() (bool, bool) {
	val, ok := p.Value.(bool)
	return val, ok
}

func (p *PropertyValue) AssertBool() bool {
	val, ok := p.Bool()
	if ok {
		return val
	} else {
		panic(fmt.Sprintf("Expected a bool, but got %#v", p.Value))
	}
}

func extractVertexProperties(props map[string]interface{}) (map[string]Property, error) {
	properties := make(map[string]Property)
	for key, prop_val := range props {
		prop_val_maps, ok := prop_val.([]interface{})

		if !ok {
			return nil, fmt.Errorf("Property is not a list %#v", prop_val)
		}

		if len(prop_val_maps) != 1 {
			panic(fmt.Sprintf("should be exactly 1, but got %#v", prop_val_maps))
		}

		prop_val_map, ok := prop_val_maps[0].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Property value map is not an object %#v", prop_val)
		}

		prop_val, ok := prop_val_map["value"]
		if !ok {
			return nil, fmt.Errorf("no 'value' in property object")
		}

		prop_id_interface, ok := prop_val_map["id"]
		if !ok {
			return nil, fmt.Errorf("no 'id' in property object")
		}

		prop_id, ok := prop_id_interface.(string)
		if !ok {
			return nil, fmt.Errorf("'id' in property object is not a string")
		}

		property := Property{
			Id:    prop_id,
			Value: PropertyValue{Value: prop_val},
		}

		properties[key] = property
	}

	return properties, nil
}

func extractEdgeProperties(props map[string]interface{}) (map[string]PropertyValue, error) {
	properties := make(map[string]PropertyValue)
	for key, prop_val := range props {
		property := PropertyValue{
			Value: prop_val,
		}

		properties[key] = property
	}

	return properties, nil
}
