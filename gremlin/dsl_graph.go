package gremlin

import (
	"fmt"
)

type Graph struct{}

// This is the starting point for building queries.
var G Graph

func (g *Graph) V() *Query {
	q := Query{query: "g.V()"}
	return &q
}

func Current() *Query {
	return &Query{query: "__"}
}

func (g *Graph) AddV(label string) *Query {
	query := fmt.Sprintf(`g.addV("%s")`, EscapeString(label))
	q := Query{query: query}
	return &q
}

func (g *Graph) AddE(label string) *Query {
	query := fmt.Sprintf(`g.addE("%s")`, EscapeString(label))
	q := Query{query: query}
	return &q
}
