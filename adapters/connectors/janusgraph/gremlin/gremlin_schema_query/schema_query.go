/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
// These package is a small DSL for doing schema modifications on JanusGraph.
// Note that under the hood, it uses a mutable buffer, so you cannot re-use partial queries.
package gremlin_schema_query

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
)

type Cardinality string

const CARDINALITY_SINGLE Cardinality = "SINGLE"

type Multiplicity string

const MULTIPLICITY_MANY2ONE Multiplicity = "MANY2ONE"
const MULTIPLICITY_MULTI Multiplicity = "MULTI"

type DataType string

const DATATYPE_BOOLEAN DataType = "Boolean"
const DATATYPE_STRING DataType = "String"
const DATATYPE_LONG DataType = "Long"
const DATATYPE_DOUBLE DataType = "Double"
const DATATYPE_GEOSHAPE DataType = "Geoshape"

type SchemaQuery interface {
	MakePropertyKey(name string, datatype DataType, cardinality Cardinality) SchemaQuery
	MakeIndexedPropertyKey(name string, datatype DataType, cardinality Cardinality, index string) SchemaQuery
	MakeIndexedPropertyKeyTextString(name string, datatype DataType, cardinality Cardinality, index string) SchemaQuery
	MakeEdgeLabel(name string, multiplicity Multiplicity) SchemaQuery
	MakeVertexLabel(name string) SchemaQuery

	// Add a graph wide composite index of potential a few properties.
	// If uniqueness is true, a uniqueness constraint will be placed on the combination of properties.
	AddGraphCompositeIndex(name string, propertyNames []string, uniqueness bool) SchemaQuery

	AddGraphMixedIndex(name string, propertyNames []string, indexName string) SchemaQuery
	AddGraphMixedIndexString(name string, propertyNames []string, indexName string) SchemaQuery

	Commit() SchemaQuery
	String() string
}

type schemaQuery struct {
	builder *strings.Builder
}

func New() SchemaQuery {
	var builder strings.Builder
	builder.WriteString("graph.tx().rollback()\nmgmt = graph.openManagement()\n")

	return &schemaQuery{
		builder: &builder,
	}
}

func (s *schemaQuery) extend(format string, vals ...interface{}) SchemaQuery {
	s.builder.WriteString(fmt.Sprintf(format, vals...))
	return s
}

func (s *schemaQuery) MakePropertyKey(name string, datatype DataType, cardinality Cardinality) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	return s.extend(`
		newProp = mgmt.makePropertyKey("%s").dataType(%s.class).cardinality(org.janusgraph.core.Cardinality.%s).make()
	`, escapedName, datatype, cardinality)
}

func (s *schemaQuery) MakeIndexedPropertyKey(name string, datatype DataType, cardinality Cardinality, index string) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	return s.extend(`
		newProp = mgmt.makePropertyKey("%s").dataType(%s.class).cardinality(org.janusgraph.core.Cardinality.%s).make()
		existingIndex = mgmt.getGraphIndex("%s")
		mgmt.addIndexKey(existingIndex, newProp)
	`, escapedName, datatype, cardinality, index)
}

func (s *schemaQuery) MakeIndexedPropertyKeyTextString(name string, datatype DataType, cardinality Cardinality, index string) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	return s.extend(`
		newProp = mgmt.makePropertyKey("%s").dataType(%s.class).cardinality(org.janusgraph.core.Cardinality.%s).make()
		existingIndex = mgmt.getGraphIndex("%s")
		mgmt.addIndexKey(existingIndex, newProp, Mapping.TEXTSTRING.asParameter())
	`, escapedName, datatype, cardinality, index)
}

func (s *schemaQuery) MakeEdgeLabel(name string, multiplicity Multiplicity) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	return s.extend("mgmt.makeEdgeLabel(\"%s\").multiplicity(%s).make()\n", escapedName, multiplicity)
}

func (s *schemaQuery) MakeVertexLabel(name string) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	return s.extend("mgmt.makeVertexLabel(\"%s\").make()\n", escapedName)
}

func (s *schemaQuery) Commit() SchemaQuery {
	return s.extend("mgmt.commit()")
}

func (s *schemaQuery) String() string {
	return s.builder.String()
}

func (s *schemaQuery) AddGraphCompositeIndex(name string, propertyNames []string, uniqueness bool) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	s.extend("mgmt.buildIndex(\"%s\", Vertex.class)", escapedName)

	for _, propertyName := range propertyNames {
		escapedPropName := gremlin.EscapeString(propertyName)
		s.extend(".addKey(mgmt.getPropertyKey(\"%s\"))", escapedPropName)
	}

	if uniqueness {
		s.extend(".unique()")
	}

	return s.extend(".buildCompositeIndex()\n")
}

func (s *schemaQuery) AddGraphMixedIndex(name string, propertyNames []string, indexName string) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	s.extend("mgmt.buildIndex(\"%s\", Vertex.class)", escapedName)

	for _, propertyName := range propertyNames {
		escapedPropName := gremlin.EscapeString(propertyName)
		s.extend(".addKey(mgmt.getPropertyKey(\"%s\"))", escapedPropName)
	}

	escapedIndexName := gremlin.EscapeString(indexName)
	return s.extend(".buildMixedIndex(\"%s\")\n", escapedIndexName)
}

func (s *schemaQuery) AddGraphMixedIndexString(name string, propertyNames []string, indexName string) SchemaQuery {
	escapedName := gremlin.EscapeString(name)
	s.extend("mgmt.buildIndex(\"%s\", Vertex.class)", escapedName)

	for _, propertyName := range propertyNames {
		escapedPropName := gremlin.EscapeString(propertyName)
		s.extend(".addKey(mgmt.getPropertyKey(\"%s\"), Mapping.STRING.asParameter())", escapedPropName)
	}

	escapedIndexName := gremlin.EscapeString(indexName)
	return s.extend(".buildMixedIndex(\"%s\")\n", escapedIndexName)
}

func AwaitGraphIndicesAvailable(indices []string) SchemaQuery {
	var builder strings.Builder
	q := &schemaQuery{builder: &builder}

	for _, index := range indices {
		escapedIndex := gremlin.EscapeString(index)
		q.extend("mgmt = graph.openManagement()\n mgmt.awaitGraphIndexStatus(graph, \"%s\").call()\n", escapedIndex)
	}

	return q
}
