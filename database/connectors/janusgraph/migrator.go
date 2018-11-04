package janusgraph

import (
	"errors"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/gremlin/gremlin_schema_query"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
	log "github.com/sirupsen/logrus"
	"strings"
)

// Called during initialization of the connector.
func (j *Janusgraph) ensureBasicSchema() error {
	// No basic schema has been created yet.
	if j.state.Version == 0 {
		query := gremlin_schema_query.New()
		// Enforce UUID's to be unique across Janus
		query.MakePropertyKey(PROP_UUID, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.AddGraphCompositeIndex(INDEX_BY_UUID, []string{PROP_UUID}, true)

		query.MakePropertyKey(PROP_KIND, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_CLASS_ID, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.AddGraphCompositeIndex(INDEX_BY_KIND_AND_CLASS, []string{PROP_KIND, PROP_CLASS_ID}, false)

		query.MakeVertexLabel(KEY_VERTEX_LABEL)

		query.MakeEdgeLabel(KEY_EDGE_LABEL, gremlin_schema_query.MULTIPLICITY_MANY2ONE)
		query.MakeEdgeLabel(KEY_PARENT_LABEL, gremlin_schema_query.MULTIPLICITY_MANY2ONE)

		query.MakePropertyKey(PROP_KEY_IS_ROOT, gremlin_schema_query.DATATYPE_BOOLEAN, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_DELETE, gremlin_schema_query.DATATYPE_BOOLEAN, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_EXECUTE, gremlin_schema_query.DATATYPE_BOOLEAN, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_READ, gremlin_schema_query.DATATYPE_BOOLEAN, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_WRITE, gremlin_schema_query.DATATYPE_BOOLEAN, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_EMAIL, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_IP_ORIGIN, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_EXPIRES_UNIX, gremlin_schema_query.DATATYPE_LONG, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_KEY_TOKEN, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)

		query.Commit()

		_, err := j.client.Execute(query)

		if err != nil {
			return fmt.Errorf("Could not initialize the basic Janus schema.")
		}

		gremlin_schema_query.AwaitGraphIndicesAvailable([]string{INDEX_BY_UUID, INDEX_BY_KIND_AND_CLASS})
		_, err = j.client.Execute(query)

		if err != nil {
			return fmt.Errorf("Could not initialize the basic Janus schema; could not await until the base indices were available.")
		}

		// Set initial version in state.
		j.state.Version = 1
		j.state.lastId = 0
		j.state.ClassMap = make(map[schema.ClassName]MappedClassName)
		j.state.PropertyMap = make(map[schema.ClassName]map[schema.PropertyName]MappedPropertyName)
		j.UpdateStateInStateManager()
	}
	return nil
}

// Add a class to the Thing or Action schema, depending on the kind parameter.
func (j *Janusgraph) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	log.Debugf("Adding class '%v' in JanusGraph", class.Class)
	// Extra sanity check
	sanitizedClassName := schema.AssertValidClassName(class.Class)

	vertexLabel := j.state.addMappedClassName(sanitizedClassName)

	var rawQuery strings.Builder
	graphOpenManagement(&rawQuery)
	graphAddClass(&rawQuery, vertexLabel)

	for _, prop := range class.Properties {
		sanitziedPropertyName := schema.AssertValidPropertyName(prop.Name)
		janusPropertyName := j.state.addMappedPropertyName(sanitizedClassName, sanitziedPropertyName)

		if len(prop.AtDataType) != 1 {
			return fmt.Errorf("Only primitive types supported for now")
		}

		graphAddProperty(&rawQuery, janusPropertyName, schema.DataType(prop.AtDataType[0]))
	}

	graphCommit(&rawQuery)

	q := gremlin.RawQuery(rawQuery.String())
	fmt.Printf("\n#\n#\n#\nEXECUTING QUERY: %#v\n", q)

	_, err := j.client.Execute(q)

	if err != nil {
		return fmt.Errorf("could not create vertex/property types in JanusGraph")
	}

	// Update mapping
	j.UpdateStateInStateManager()
	return nil
}

// Drop a class from the schema.
func (j *Janusgraph) DropClass(kind kind.Kind, name string) error {
	log.Debugf("Removing class '%v' in JanusGraph", name)
	sanitizedClassName := schema.AssertValidClassName(name)

	vertexLabel := j.state.addMappedClassName(sanitizedClassName)

	var rawQuery strings.Builder
	graphOpenManagement(&rawQuery)
	graphDropClass(&rawQuery, vertexLabel)
	graphCommit(&rawQuery)

	q := gremlin.RawQuery(rawQuery.String())
	_, err := j.client.Execute(q)

	if err != nil {
		return fmt.Errorf("could not remove all data of the dropped class in JanusGraph")
	}

	// Update mapping
	j.state.removeMappedClassName(sanitizedClassName)
	j.UpdateStateInStateManager()

	return nil
}

func (j *Janusgraph) UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	//  if newClassName != nil {
	//	  oldName := schema.AssertValidClassName(className)
	//	  newName := schema.AssertValidClassName(*newClassName)
	//    return j.state.renameClass(oldName, newName)
	//  }
	//
	//  return nil
	return nil
}

func (j *Janusgraph) AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return errors.New("Not supported")
}

func (j *Janusgraph) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	// todo: update mapping
	return errors.New("Not supported")
}

func (j *Janusgraph) DropProperty(kind kind.Kind, className string, propName string) error {
	return errors.New("Not supported")
	// g.V().has('lat-old').properties('lat-old').drop()
}

/////////////////////////////////
// Helper functions

//// TODO: add to Gremlin DSL
//func graphOpenManagement(s *strings.Builder) {
//	s.WriteString("mgmt = graph.openManagement()\n")
//}
//
//func graphCommit(s *strings.Builder) {
//	s.WriteString("mgmt.commit()\n")
//}
//
//func graphAddClass(s *strings.Builder, name MappedClassName) {
//	s.WriteString(fmt.Sprintf("mgmt.makeVertexLabel(\"%s\").make()\n", name))
//}
//
//func graphAddProperty(s *strings.Builder, name MappedPropertyName, type_ schema.DataType) {
//	propDataType := getJanusDataType(type_)
//	s.WriteString(fmt.Sprintf("mgmt.makePropertyKey(\"%s\").cardinality(Cardinality.SINGLE).dataType(%s.class).make()\n", name, propDataType))
//}
//
//func graphDropClass(s *strings.Builder, name MappedClassName) {
//	// Simply all vertices of this class.
//	//g.V().has("object", "classId", name).drop()
//}
//
//func graphDropProperty(s *strings.Builder, name MappedPropertyName) {
//	//g.V().has("object", name).properties(name).drop()
//	//s.WriteString(fmt.Sprintf("mgmt.getPropertyKey(\"%s\").remove()\n", janusPropName))
//}
//
//// Get Janus data type from a weaviate data type.
//// Panics if passed a wrong type.
//func getJanusDataType(type_ schema.DataType) string {
//	switch type_ {
//	case schema.DataTypeString:
//		return "String"
//	case schema.DataTypeInt:
//		return "Long"
//	case schema.DataTypeNumber:
//		return "Double"
//	case schema.DataTypeBoolean:
//		return "Boolean"
//	default:
//		panic(fmt.Sprintf("unsupported data type '%v'", type_))
//	}
//}
