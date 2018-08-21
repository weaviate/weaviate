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

// Asserts that the Datum's in the reponse in the range 'from' until 'to' are of Edges.
// Return those edges.
func (r *Response) AssertEdgeSlice(from, to int) []*Edge {
	// TODO
	return nil
}
