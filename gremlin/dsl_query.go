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

func (q *Query) Optional(query *Query) *Query {
	return extend_query(q, `.optional(%s)`, query.String())
}

func (q *Query) Drop() *Query {
	return extend_query(q, ".drop()")
}
