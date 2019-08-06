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
	"errors"
	"fmt"
	"reflect"
)

// A single piece of data returned by a Gremlin query.
type Datum struct {
	Datum interface{}
}

// If the Datum is a map, look up the key and return a datum for that key.
func (d *Datum) Key(key string) (*Datum, error) {
	x, ok := d.Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected a map, but got something else as the result")
	}

	element, ok := x[key]

	if !ok {
		return nil, fmt.Errorf("No such key '%v'", key)
	}

	return &Datum{Datum: element}, nil
}

func (d *Datum) AssertKey(key string) *Datum {
	datum, err := d.Key(key)
	if err != nil {
		panic(err)
	}
	return datum
}

func (d *Datum) Path() (*Path, error) {
	m, ok := d.Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected a Path, but got something else as the result")
	}

	// Paths have a a 'labels' and 'objects' key.
	_, label_ok := m["labels"]
	objects_interface, object_ok := m["objects"]

	if !label_ok || !object_ok {
		return nil, fmt.Errorf("Expected a Path, but got something else as the result")
	}

	objects_list, ok := objects_interface.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected a Path, but got something else as the result")
	}

	segments := make([]Datum, 0)
	for _, datum := range objects_list {
		segment := Datum{Datum: datum}
		segments = append(segments, segment)
	}

	return &Path{Segments: segments}, nil
}

func (d *Datum) AssertPath() *Path {
	path, err := d.Path()
	if err != nil {
		panic(err)
	}
	return path
}

// Extract the type of a datum. This method is intended to be a debugging tool,
// don't use it in a critical path.
func (d *Datum) Type() string {
	switch d.Datum.(type) {
	case int:
		return "int"
	default:
		return fmt.Sprintf("Unknown type for '%s'", reflect.TypeOf(d.Datum).Name())
	}
}

// Attempt to extract a Vertex from this datum.
func (d *Datum) Vertex() (*Vertex, error) {
	v, ok := d.Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected a vertex, but got something else as the result")
	}

	type_, ok := v["type"]
	if !ok || type_ != "vertex" {
		return nil, errors.New("Vertex element in result does not have type 'vertex'")
	}

	label_interface, ok := v["label"]
	if !ok {
		return nil, errors.New("Vertex element does not have a label")
	}
	label, ok := label_interface.(string)
	if !ok {
		return nil, errors.New("Vertex element does not have a string as a label")
	}

	id_interface, ok := v["id"]
	if !ok {
		return nil, errors.New("Vertex element does not have a id")
	}
	id_f, ok := id_interface.(float64)
	if !ok {
		return nil, errors.New("Vertex element does not have a string as a id")
	}
	id := int(id_f)

	properties_interface, ok := v["properties"]
	if !ok {
		return nil, errors.New("Vertex element does not have a properties key")
	}
	properties_map, ok := properties_interface.(map[string]interface{})
	if !ok {
		return nil, errors.New("Vertex element does not have an object for properties ")
	}

	properties, err := extractVertexProperties(properties_map)
	if err != nil {
		return nil, err
	}

	vertex := Vertex{
		Id:         id,
		Label:      label,
		Properties: properties,
	}

	return &vertex, nil
}

func (d *Datum) AssertVertex() *Vertex {
	v, err := d.Vertex()

	if err != nil {
		panic(fmt.Sprintf("Expected datum to be an Vertex, but it was '%s'", d.Type()))
	}

	return v
}

// Attempt to extract a edge from this datum.
func (d *Datum) Edge() (*Edge, error) {
	v, ok := d.Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected a edge, but got something else as the result")
	}

	type_, ok := v["type"]
	if !ok || type_ != "edge" {
		return nil, errors.New("edge element in result does not have type 'edge'")
	}

	label_interface, ok := v["label"]
	if !ok {
		return nil, errors.New("edge element does not have a label")
	}
	label, ok := label_interface.(string)
	if !ok {
		return nil, errors.New("edge element does not have a string as a label")
	}

	id_interface, ok := v["id"]
	if !ok {
		return nil, errors.New("edge element does not have a id")
	}
	id, ok := id_interface.(string)
	if !ok {
		return nil, errors.New("edge element does not have a string as a id")
	}

	var properties map[string]PropertyValue

	properties_interface, ok := v["properties"]
	if ok {
		properties_map, ok := properties_interface.(map[string]interface{})
		if !ok {
			return nil, errors.New("edge element does not have an object for properties ")
		}

		var err error
		properties, err = extractEdgeProperties(properties_map)
		if err != nil {
			return nil, err
		}
	}

	edge := Edge{
		Id:         id,
		Label:      label,
		Properties: properties,
	}

	return &edge, nil
}

func (d *Datum) AssertEdge() *Edge {
	v, err := d.Edge()

	if err != nil {
		panic(fmt.Sprintf("Expected datum to be an Edge, but %s", err))
	}

	return v
}

func (d *Datum) AssertStringSlice() []string {
	var stringSlice []string
	slice, ok := d.Datum.([]interface{})

	if !ok {
		panic("Expected this datum to be a slice")
	}

	for _, mightBeStr := range slice {
		str, ok := mightBeStr.(string)
		if !ok {
			panic("epxected this to be a string of slices")
		}

		stringSlice = append(stringSlice, str)
	}

	return stringSlice
}
