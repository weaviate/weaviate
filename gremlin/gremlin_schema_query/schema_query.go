// These package is a small DSL for doing schema modifications on JanusGraph.
// Note that under the hood, it uses a mutable buffer, so you cannot re-use partial queries.
package gremlin_schema_query

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"strings"
)

type Cardinality string

const CARDINALITY_SINGLE Cardinality = "SINGLE"

type Multiplicity string

const MULTIPLICITY_MANY2ONE Multiplicity = "MANY2ONE"

type DataType string

const DATATYPE_BOOLEAN DataType = "Boolean"
const DATATYPE_STRING DataType = "String"
const DATATYPE_LONG DataType = "Long"
const DATATYPE_DOUBLE DataType = "Double"

type SchemaQuery interface {
	MakePropertyKey(name string, datatype DataType, cardinality Cardinality) SchemaQuery
	MakeEdgeLabel(name string, multiplicity Multiplicity) SchemaQuery
	MakeVertexLabel(name string) SchemaQuery

	// Add a graph wide composite index of potential a few properties.
	// If uniqueness is true, a uniqueness constraint will be placed on the combination of properties.
	AddGraphCompositeIndex(name string, propertyNames []string, uniqueness bool) SchemaQuery

	Commit() SchemaQuery
	String() string
}

type schemaQuery struct {
	builder *strings.Builder
}

func New() SchemaQuery {
	var builder strings.Builder
	builder.WriteString("mgmt = graph.openManagement()\n")

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
	return s.extend("mgmt.makePropertyKey(\"%s\").dataType(%s.class).cardinality(Cardinality.%s).make()\n", escapedName, datatype, cardinality)
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
	return s.extend("mgmt.commit()\n")
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

func AwaitGraphIndicesAvailable(indices []string) SchemaQuery {
	var builder strings.Builder
	q := &schemaQuery{builder: &builder}

	for _, index := range indices {
		escapedIndex := gremlin.EscapeString(index)
		q.extend("ManagementSystem.awaitGraphIndexStatus(g, \"%s\").call()\n", escapedIndex)
	}

	return q
}
