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
package filters

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// FilterQuery from filter params. Can be appended to any GremlinFilterQuery
type FilterQuery struct {
	filter     *common_filters.LocalFilter
	nameSource nameSource
}

type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
	GetMappedClassName(className schema.ClassName) state.MappedClassName
}

// New FilterQuery from local filter params
func New(filter *common_filters.LocalFilter, nameSource nameSource) *FilterQuery {
	return &FilterQuery{
		filter:     filter,
		nameSource: nameSource,
	}
}

func (f *FilterQuery) String() (string, error) {
	if f.filter == nil {
		return "", nil
	}

	clause := f.filter.Root
	q, err := f.buildClause(clause)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(".union(%s)", q.String()), nil
}

// A clause must meet either of 2 conditions:
//
// 1.) It has a value operator (such as Equal, NotEqual, LessThan, etc.) If
// this is the case, then a clause will contain exactly one value clause
//
// 2.) If we instead encounter an operand clause (with operators such as 'And'
// or 'Or'), then it can combine subClauses. Each sub-clause must in turn meet
// these two conditions.
func (f *FilterQuery) buildClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	if clause.Operator.OnValue() {
		return f.buildValueClause(clause)
	}

	switch clause.Operator {
	case common_filters.OperatorAnd, common_filters.OperatorOr:
		return f.buildOperandClause(clause)
	default:
		return nil, fmt.Errorf("unknown operator '%#v'", clause.Operator)
	}
}

func (f *FilterQuery) buildValueClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	if clause.On.Child == nil {
		return f.buildPrimitiveValueClause(clause)
	}
	return f.buildReferenceValueClause(clause)
}

// A primitive value clause has exactly one matcher. Example:
// has("population", gte(1000000)
//
// Primitive value clauses can be combined if they are used as operand clauses.
// Independent of that they can be appended to a reference prop which consists
// of an edge path (i.e. how to get the final class) and a primitive value
// clause (i.e. once I'm there what should I filter)
func (f *FilterQuery) buildPrimitiveValueClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	q := &gremlin.Query{}
	predicate, err := gremlinPredicateFromOperator(clause.Operator, clause.Value)
	if err != nil {
		return q, fmt.Errorf("operator %v with value '%v': %s", clause.Operator,
			clause.Value.Value, err)
	}

	path := clause.On.GetInnerMost()
	propName := f.mappedPropertyName(path.Class, path.Property)
	q = q.Has(propName, predicate)
	return q, nil
}

// The reference value clause has the form where(<edgePath>.<primtiveFilter>),
// this means it always ends with a primtive Filter that is preceded by an edge
// path. This edge path can have have any length between 1..n.
//
// Valid reference value clauses could look like:
//
// where(outE("inCountry").inV().has("classId", "Country").has("population", gt(1000)))
//       |----------------- edge path -------------------||----primitive filter -----|
//
// However an edge path can also be longer, the above example shows 1 hop deep.
// See the unit tests for a full example.
func (f *FilterQuery) buildReferenceValueClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	q := gremlin.New()

	pathToEdge, err := f.buildEdgePath(clause.On)
	if err != nil {
		return q, fmt.Errorf("reference filter: cannot build path to edge: %s", err)
	}

	primitiveFilter, err := f.buildPrimitiveValueClause(clause)
	if err != nil {
		return q, fmt.Errorf("reference filter: cannot build value clause: %s", err)
	}

	q = q.Where(pathToEdge.Raw(fmt.Sprintf(".%s", primitiveFilter.String())))

	return q, nil
}

// An edge path is the way to get to a linked vertex. Depending on the level of
// nesting it will consist of 1 or more segments.
//
// An example for a 1-level deep edge path looks like so:
// outE("inCountry").inV().has("classId", "Country")
//
// An example for a 2-level deep edge path looks like so:
// outE("inCity").inV().has("classId", "City").outE("inCountry").inV().has("classId", "Country")
// See the unit tests for a full example.
func (f *FilterQuery) buildEdgePath(path *common_filters.Path) (*gremlin.Query, error) {
	q := gremlin.New()
	edgeLabel := f.mappedPropertyName(path.Class, path.Property)
	referencedClass := f.mappedClassName(path.Child.Class)
	q = q.OutEWithLabel(edgeLabel).
		InV().HasString("classId", referencedClass)

	if path.Child.Child == nil {
		// the child clause doesn't have any more children this means we have reached the end
		return q, nil
	}

	// Since there are more children, we need to go deeper
	childEdgePath, err := f.buildEdgePath(path.Child)
	if err != nil {
		return q, err
	}

	return q.Raw(fmt.Sprintf(".%s", childEdgePath.String())), nil
}

// An operandClause has the form of
// <combinator>(<valueClause1>, <valueClause2>, ...<valueClauseN>)
// where the combinator is either 'and' or 'or'
func (f *FilterQuery) buildOperandClause(clause *common_filters.Clause) (*gremlin.Query, error) {
	q := &gremlin.Query{}
	individualQueries := make([]*gremlin.Query, len(clause.Operands), len(clause.Operands))
	for i, operand := range clause.Operands {
		result, err := f.buildClause(&operand)
		if err != nil {
			return q, fmt.Errorf("build operand '%s': %s", clause.Operator.Name(), err)
		}
		individualQueries[i] = result
	}

	switch clause.Operator {
	case common_filters.OperatorAnd:
		q = q.And(individualQueries...)
		return q, nil
	case common_filters.OperatorOr:
		q = q.Or(individualQueries...)
		return q, nil
	}
	return nil, fmt.Errorf("unknown operator '%#v'", clause.Operator)
}

func (f *FilterQuery) mappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) string {
	if f.nameSource == nil {
		return string(propName)
	}

	return string(f.nameSource.GetMappedPropertyName(className, propName))
}

func (f *FilterQuery) mappedClassName(className schema.ClassName) string {
	if f.nameSource == nil {
		return string(className)
	}

	return string(f.nameSource.GetMappedClassName(className))
}
