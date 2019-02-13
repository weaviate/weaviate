/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
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

// New returns a new (empty) query. Most useful for subqueries which don't
// start with g.V() or g.E().
func New() *Query {
	return &Query{}
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
	return smartExtendQuery(q, "count()")
}

// Mean of the underlying values
func (q *Query) Mean() *Query {
	return smartExtendQuery(q, "mean()")
}

// Max of the underlying values
func (q *Query) Max() *Query {
	return smartExtendQuery(q, "max()")
}

// Min of the unerlying values
func (q *Query) Min() *Query {
	return smartExtendQuery(q, "min()")
}

// Sum up all the underlying values
func (q *Query) Sum() *Query {
	return smartExtendQuery(q, "sum()")
}

// CountLocal is most likely used in conjuction with an aggregation query and
// wrapped in a By() statement
func (q *Query) CountLocal() *Query {
	return extend_query(q, "count(local)")
}

// SumLocal is most likely used in conjuction with an aggregation query and
// wrapped in a By() statement
func (q *Query) SumLocal() *Query {
	return extend_query(q, "sum(local)")
}

// MaxLocal is most likely used in conjuction with an aggregation query and
// wrapped in a By() statement
func (q *Query) MaxLocal() *Query {
	return extend_query(q, "max(local)")
}

// MinLocal is most likely used in conjuction with an aggregation query and
// wrapped in a By() statement
func (q *Query) MinLocal() *Query {
	return extend_query(q, "min(local)")
}

// MeanLocal is most likely used in conjuction with an aggregation query and
// wrapped in a By() statement
func (q *Query) MeanLocal() *Query {
	return extend_query(q, "mean(local)")
}

// Group by values. Will most likely be followed by a `By()`
func (q *Query) Group() *Query {
	if q.query == "" {
		return &Query{query: "group()"}
	}

	return extend_query(q, ".group()")
}

// GroupCount by values. Will most likely be followed by a `By()`
func (q *Query) GroupCount() *Query {
	if q.query == "" {
		return &Query{query: "groupCount()"}
	}

	return extend_query(q, ".groupCount()")
}

// By filters down previous segement, most likely used after a count,
// groupCount or select statement
func (q *Query) By(label string) *Query {
	return extend_query(q, `.by("%s")`, label)
}

// Project can be used to project a result into a map. Most likely used in
// combination with Select().By(), example: select("foo").by(project("bar")).
func (q *Query) Project(label string) *Query {
	return smartExtendQuery(q, `project("%s")`, label)
}

// ByQuery filters down previous segement, most likely used after a group(), count(),
// groupCount() or select() statement. It takes a query rather than a label string
func (q *Query) ByQuery(subquery *Query) *Query {
	return extend_query(q, `.by(%s)`, subquery.String())
}

func (q *Query) Fold() *Query {
	return smartExtendQuery(q, "fold()")
}

func (q *Query) Unfold() *Query {
	return smartExtendQuery(q, "unfold()")
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

// Limit results
func (q *Query) Limit(limit int) *Query {
	return extend_query(q, ".limit(%d)", limit)
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
	return smartExtendQuery(q, `has("%s", "%s")`, EscapeString(key), EscapeString(value))
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
	return smartExtendQuery(q, `out("%s")`, EscapeString(label))
}

func (q *Query) InE() *Query {
	return extend_query(q, ".inE()")
}

func (q *Query) InEWithLabel(label string) *Query {
	return extend_query(q, `.inE("%s")`, EscapeString(label))
}

func (q *Query) OutE() *Query {
	if q.query == "" {
		return extend_query(q, "outE()")
	}
	return extend_query(q, ".outE()")
}

func (q *Query) OutEWithLabel(label string) *Query {
	if q.query == "" {
		return extend_query(q, `outE("%s")`, EscapeString(label))
	}
	return extend_query(q, `.outE("%s")`, EscapeString(label))
}

// Aggregate results to perform analyses on them
func (q *Query) Aggregate(label string) *Query {
	if q.query == "" {
		return extend_query(q, `aggregate("%s")`, EscapeString(label))
	}

	return extend_query(q, `.aggregate("%s")`, EscapeString(label))
}

// Cap runs queries up until this point
func (q *Query) Cap(label string) *Query {
	if q.query == "" {
		return extend_query(q, `cap("%s")`, EscapeString(label))
	}

	return extend_query(q, `.cap("%s")`, EscapeString(label))
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

// As create a reference or label
//
// As is a special case in that it cannot lead a query in Gremlin, because it
// is a reserved word in Groovy. That means it's fine if it is appended to an
// existing query like so: existingQuery.as(), but it cannot start a query like
// so: as().somethingElse(). That's why there is a workaround in Germlin to
// start a query with as like so: __.as()
func (q *Query) As(names ...string) *Query {
	quoted := make([]string, len(names), len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf(`"%s"`, EscapeString(name))
	}

	if q.query != "" {
		return extend_query(q, `.as(%s)`, strings.Join(quoted, ", "))
	}
	return extend_query(q, `__.as(%s)`, strings.Join(quoted, ", "))
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

// Has Property checks if a property is set, regardless of its value
func (q *Query) HasProperty(key string) *Query {
	hasQuery := fmt.Sprintf(`has("%s")`, key)
	if q.query == "" {
		return &Query{query: hasQuery}
	}

	return extend_query(q, fmt.Sprintf(".%s", hasQuery))
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

// Where can combine 0..n queries together
func (q *Query) Where(queries ...*Query) *Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, single := range queries {
		queryStrings[i] = single.String()
	}

	queryStringsConcat := strings.Join(queryStrings, ", ")
	return smartExtendQuery(q, `where(%s)`, queryStringsConcat)
}

func (q *Query) Optional(query *Query) *Query {
	return extend_query(q, `.optional(%s)`, query.String())
}

func (q *Query) Drop() *Query {
	return extend_query(q, ".drop()")
}

// Union can combine 0..n queries together
func (q *Query) Union(queries ...*Query) *Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, single := range queries {
		queryStrings[i] = single.String()
	}

	queryStringsConcat := strings.Join(queryStrings, ", ")
	return smartExtendQuery(q, `union(%s)`, queryStringsConcat)
}

// Match can combine 0..n queries together
func (q *Query) Match(queries ...*Query) *Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, single := range queries {
		queryStrings[i] = single.String()
	}

	queryStringsConcat := strings.Join(queryStrings, ", ")
	return smartExtendQuery(q, `match(%s)`, queryStringsConcat)
}

// AsProjectBy is a helper construct to wrap a result in a map, it a query like
// so: .as("isCapital").project("isCapital").by(select("isCapital")))
func (q *Query) AsProjectBy(labels ...string) *Query {
	if len(labels) <= 1 {
		label := EscapeString(labels[0])
		return extend_query(q, `.as("%s").project("%s").by(select("%s"))`, label, label, label)
	}

	asLabel := EscapeString(labels[0])
	projectLabel := EscapeString(labels[1])
	return extend_query(q, `.as("%s").project("%s").by(select("%s"))`, asLabel, projectLabel, asLabel)
}

// OrderLocalByValuesLimit is a helper construct to select the most occuring
// items, like so if called with "decr", 3:
// .order(local).by(values, decr).limit(local, 3)
func (q *Query) OrderLocalByValuesLimit(order string, limit int) *Query {
	return extend_query(q, `.order(local).by(values, %s).limit(local, %d)`, EscapeString(order), limit)
}

// OrderLocalByValuesSelectKeysLimit is a helper construct to select the most occuring
// items and extract the keys, like so if called with "decr", 3:
// .order(local).by(values, decr).select(keys).limit(local, 3)
func (q *Query) OrderLocalByValuesSelectKeysLimit(order string, limit int) *Query {
	return extend_query(q, `.order(local).by(values, %s).select(keys).limit(local, %d)`, EscapeString(order), limit)
}
