//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package gremlin

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
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

func (p *PropertyValue) GeoCoordinates() (*models.GeoCoordinates, error) {
	if p.Value == nil {
		return nil, nil
	}
	// value will look like
	// {"type":"Point", "coordinates":[]interface {}{4.9, 52.366669}

	pointMap, ok := p.Value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("point property is not a map, but %T", p)
	}

	if t := pointMap["type"].(string); t != "Point" {
		return nil, fmt.Errorf("property.type needs to be point, but is %s", t)
	}

	c9s, ok := pointMap["coordinates"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("property.coordinates is not set, got %#v", pointMap)
	}

	if l := len(c9s); l != 2 {
		return nil, fmt.Errorf("property.coordinates[] must have len 2, but got %#v with len %d", c9s, l)
	}

	// WARNING: Although all create queries in Janusgraph
	// always take the coordinates in the form of latitude,
	// longitude, they are actually stored (and therefore
	// returned) in the oppposite order! So this array is
	// longitude, latitude!
	return &models.GeoCoordinates{
		Latitude:  float32(c9s[1].(float64)),
		Longitude: float32(c9s[0].(float64)),
	}, nil
}

func (p *PropertyValue) AssertGeoCoordinates() *models.GeoCoordinates {
	val, err := p.GeoCoordinates()
	if err == nil {
		return val
	}
	panic(fmt.Sprintf("Expected a geoCoordinates, but got %#v with error: %s", p.Value, err))
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
