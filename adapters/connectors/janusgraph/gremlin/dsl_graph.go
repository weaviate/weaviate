//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

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

func (g *Graph) E() *Query {
	q := Query{query: "g.E()"}
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
