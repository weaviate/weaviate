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
)

// The result of a successful Gremlin query.
type Response struct {
	Data []Datum
}

func (r *Response) OneInt() (int, error) {
	if len(r.Data) != 1 {
		return 0, fmt.Errorf("Query resulted in %v results, whilst we expected just 1", len(r.Data))
	}

	i, ok := r.Data[0].Datum.(float64)
	if !ok {
		return 0, fmt.Errorf("Expected to get see an int, but got %#v instead", r.Data[0])
	}

	return int(i), nil
}

func (r *Response) First() (*Datum, error) {
	if len(r.Data) < 1 {
		return nil, errors.New("Tried to access first datum in empty response")
	} else {
		return &r.Data[0], nil
	}
}

func (r *Response) AssertFirst() *Datum {
	first, err := r.First()
	if err != nil {
		panic(err)
	}
	return first
}

// Check if the output of the query are all vertices.
func (r *Response) Vertices() ([]Vertex, error) {
	vertices := make([]Vertex, 0)

	for _, datum := range r.Data {
		vertex, err := datum.Vertex()
		if err != nil {
			return nil, err
		}
		vertices = append(vertices, *vertex)
	}

	return vertices, nil
}

func (r *Response) AssertStringSlice() []string {
	var stringSlice []string

	for _, mightBeStr := range r.Data {
		str, ok := mightBeStr.Datum.(string)
		if !ok {
			panic("epxected this to be a string of slices")
		}

		stringSlice = append(stringSlice, str)
	}

	return stringSlice
}
