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
	"strconv"
	"strings"
)

// A query represents the (partial) query build with the DSL
type Query struct {
	query string
}

// Return the string representation of this Query.
func (q *Query) String() string {
	return q.query
}

func RawQuery(query string) *Query {
	return &Query{query: query}
}

func (q *Query) Raw(query string) *Query {
	return extend_query(q, query)
}

func (q *Query) V() *Query {
	return extend_query(q, ".V()")
}

func (q *Query) E() *Query {
	return extend_query(q, ".E()")
}

// Count how many vertices or edges are selected by the previous query.
func (q *Query) Count() *Query {
	return extend_query(q, ".count()")
}

func (q *Query) Fold() *Query {
	return extend_query(q, ".fold()")
}

func (q *Query) Unfold() *Query {
	return extend_query(q, ".unfold()")
}

func (q *Query) Properties(names []string) *Query {
	sanitized := make([]string, 0)

	for _, name := range names {
		sanitized = append(sanitized, fmt.Sprintf(`"%s"`, EscapeString(name)))
	}

	return extend_query(q, ".properties(%s)", strings.Join(sanitized, ","))
}

func (q *Query) Select(refs []string) *Query {
	sanitized := make([]string, 0)

	for _, ref := range refs {
		sanitized = append(sanitized, fmt.Sprintf(`"%s"`, EscapeString(ref)))
	}

	return extend_query(q, ".select(%s)", strings.Join(sanitized, ","))
}

// Get the values of these property names.
func (q *Query) Values(propNames []string) *Query {
	sanitized := make([]string, 0)

	for _, propName := range propNames {
		sanitized = append(sanitized, fmt.Sprintf(`"%s"`, EscapeString(propName)))
	}

	return extend_query(q, ".values(%s)", strings.Join(sanitized, ","))
}

func (q *Query) Range(offset int, limit int) *Query {
	return extend_query(q, ".range(%d, %d)", offset, limit)
}

func (q *Query) AddV(label string) *Query {
	return extend_query(q, `.addV("%s")`, EscapeString(label))
}

func (q *Query) AddE(label string) *Query {
	return extend_query(q, `.addE("%s")`, EscapeString(label))
}

// Set the expected label of the vertex/edge.
func (q *Query) HasLabel(label string) *Query {
	return extend_query(q, `.hasLabel("%s")`, EscapeString(label))
}

func (q *Query) HasString(key string, value string) *Query {
	return extend_query(q, `.has("%s", "%s")`, EscapeString(key), EscapeString(value))
}

func (q *Query) HasBool(key string, value bool) *Query {
	return extend_query(q, `.has("%s", %v)`, EscapeString(key), value)
}

func (q *Query) StringProperty(key string, value string) *Query {
	return extend_query(q, `.property("%s", "%s")`, EscapeString(key), EscapeString(value))
}

func (q *Query) BoolProperty(key string, value bool) *Query {
	return extend_query(q, `.property("%s", %v)`, EscapeString(key), value)
}

func (q *Query) Int64Property(key string, value int64) *Query {
	return extend_query(q, `.property("%s", (long) %v)`, EscapeString(key), value)
}

func (q *Query) Float64Property(key string, value float64) *Query {
	return extend_query(q, `.property("%s", (double) %v)`, EscapeString(key), strconv.FormatFloat(value, 'g', -1, 64))
}

func (q *Query) In() *Query {
	return extend_query(q, ".in()")
}

func (q *Query) InWithLabel(label string) *Query {
	return extend_query(q, `.in("%s")`, EscapeString(label))
}

func (q *Query) Out() *Query {
	return extend_query(q, ".out()")
}

func (q *Query) OutWithLabel(label string) *Query {
	return extend_query(q, `.out("%s")`, EscapeString(label))
}

func (q *Query) InE() *Query {
	return extend_query(q, ".inE()")
}

func (q *Query) InEWithLabel(label string) *Query {
	return extend_query(q, `.inE("%s")`, EscapeString(label))
}

func (q *Query) OutE() *Query {
	return extend_query(q, ".outE()")
}

func (q *Query) OutEWithLabel(label string) *Query {
	return extend_query(q, `.outE("%s")`, EscapeString(label))
}

func (q *Query) InV() *Query {
	return extend_query(q, ".inV()")
}

func (q *Query) OutV() *Query {
	return extend_query(q, ".outV()")
}

// Return the travelled path
func (q *Query) Path() *Query {
	return extend_query(q, ".path()")
}

// Create a reference
func (q *Query) As(name string) *Query {
	return extend_query(q, `.as("%s")`, EscapeString(name))
}

// Point to a reference
func (q *Query) FromRef(reference string) *Query {
	return extend_query(q, `.from("%s")`, EscapeString(reference))
}

func (q *Query) ToQuery(query *Query) *Query {
	return extend_query(q, `.to(%s)`, query.String())
}

// Coalesce can be used in Upsert or GetOrCreate scenarios
func (q *Query) Coalesce(query *Query) *Query {
	return extend_query(q, `.coalesce(%s)`, query.String())
}

// Has can be used for arbitrary filtering on props
//
// Example: Has("population", EqInt(1000))
//
// for population == 1000
//
// which in turn translates to Gremlin: .has("population", eq(1000))
func (q *Query) Has(key string, query *Query) *Query {
	hasQuery := fmt.Sprintf(`has("%s", %s)`, key, query.String())
	if q.query == "" {
		return &Query{query: hasQuery}
	}

	return extend_query(q, fmt.Sprintf(".%s", hasQuery))
}

// And can combine 0..n queries together
//
// If used on an existing query it will lead with a dot, e.g:
//
// existingQuery().and(<some joined queries>)
//
// Otherwise it will not lead with a dot, e.g.:
//
// and(<some joined queries>)
func (q *Query) And(queries ...*Query) *Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, single := range queries {
		queryStrings[i] = single.String()
	}

	queryStringsConcat := strings.Join(queryStrings, ", ")
	if q.query == "" {
		return &Query{query: fmt.Sprintf("and(%s)", queryStringsConcat)}
	}

	return extend_query(q, `.and(%s)`, queryStringsConcat)
}

// Or can combine 0..n queries together
//
// If used on an existing query it will lead with a dot, e.g:
//
// existingQuery().or(<some joined queries>)
//
// Otherwise it will not lead with a dot, e.g.:
//
// or(<some joined queries>)
func (q *Query) Or(queries ...*Query) *Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, single := range queries {
		queryStrings[i] = single.String()
	}

	queryStringsConcat := strings.Join(queryStrings, ", ")
	if q.query == "" {
		return &Query{query: fmt.Sprintf("or(%s)", queryStringsConcat)}
	}

	return extend_query(q, `.or(%s)`, queryStringsConcat)
}

func (q *Query) Optional(query *Query) *Query {
	return extend_query(q, `.optional(%s)`, query.String())
}

func (q *Query) Drop() *Query {
	return extend_query(q, ".drop()")
}
