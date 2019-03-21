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
package janusgraph

import (
	"context"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/gremlin/gremlin_schema_query"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
)

// Called during initialization of the connector.
func (j *Janusgraph) ensureBasicSchema(ctx context.Context) error {
	// No basic schema has been created yet.
	if j.state.Version == 0 {
		query := gremlin_schema_query.New()
		// Enforce UUID's to be unique across Janus
		query.MakePropertyKey(PROP_UUID, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.AddGraphCompositeIndex(INDEX_BY_UUID, []string{PROP_UUID}, true)

		// For all classes
		query.MakePropertyKey(PROP_KIND, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_CLASS_ID, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_REF_ID, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)

		query.AddGraphMixedIndexString(INDEX_SEARCH, []string{PROP_UUID, PROP_KIND, PROP_CLASS_ID}, INDEX_SEARCH)

		query.MakePropertyKey(PROP_AT_CONTEXT, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_CREATION_TIME_UNIX, gremlin_schema_query.DATATYPE_LONG, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_LAST_UPDATE_TIME_UNIX, gremlin_schema_query.DATATYPE_LONG, gremlin_schema_query.CARDINALITY_SINGLE)

		query.MakePropertyKey(PROP_REF_EDGE_CREF, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_REF_EDGE_LOCATION, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)
		query.MakePropertyKey(PROP_REF_EDGE_TYPE, gremlin_schema_query.DATATYPE_STRING, gremlin_schema_query.CARDINALITY_SINGLE)

		query.Commit()
		_, err := j.client.Execute(query)
		if err != nil {
			return fmt.Errorf("could not initialize the basic Janus schema: %s", spew.Sdump(err))
		}

		// // TODO gh-613: await answer from janus consultants; it's not avaible in our janus setup.
		// query = gremlin_schema_query.AwaitGraphIndicesAvailable([]string{"byFoo"})
		// _, err = j.client.Execute(query)
		// if err != nil {
		// 	return fmt.Errorf("Could not initialize the basic Janus schema; could not await until the base indices were available: %s", err)
		// }

		// Set initial version in state.
		j.state.Version = 1
		j.state.LastId = 0
		j.state.ClassMap = make(map[schema.ClassName]state.MappedClassName)
		j.state.PropertyMap = make(map[schema.ClassName]map[schema.PropertyName]state.MappedPropertyName)
		j.UpdateStateInStateManager(ctx)
	}
	return nil
}

// Add a class to the Thing or Action schema, depending on the kind parameter.
func (j *Janusgraph) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {
	log.Debugf("Adding class '%v' in JanusGraph", class.Class)
	// Extra sanity check
	sanitizedClassName := schema.AssertValidClassName(class.Class)

	vertexLabel := j.state.AddMappedClassName(sanitizedClassName)

	query := gremlin_schema_query.New()
	query.MakeVertexLabel(string(vertexLabel))

	for _, prop := range class.Properties {
		sanitziedPropertyName := schema.AssertValidPropertyName(prop.Name)
		janusPropertyName := j.state.AddMappedPropertyName(sanitizedClassName, sanitziedPropertyName)

		// Determine the type of the property
		propertyDataType, err := j.schema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			// This must already be validated.
			panic(fmt.Sprintf("Data type fo property '%s' is invalid; %v", prop.Name, err))
		}

		if propertyDataType.IsPrimitive() {
			if propertyDataType.AsPrimitive() == schema.DataTypeString {
				query.MakeIndexedPropertyKeyTextString(string(janusPropertyName), weaviatePrimitivePropTypeToJanusPropType(schema.DataType(prop.AtDataType[0])), gremlin_schema_query.CARDINALITY_SINGLE, INDEX_SEARCH)
			} else {
				query.MakeIndexedPropertyKey(string(janusPropertyName), weaviatePrimitivePropTypeToJanusPropType(schema.DataType(prop.AtDataType[0])), gremlin_schema_query.CARDINALITY_SINGLE, INDEX_SEARCH)
			}
		} else {
			// In principle, we could use a Many2One edge for SingleRefs
			query.MakeEdgeLabel(string(janusPropertyName), gremlin_schema_query.MULTIPLICITY_MULTI)
		}
	}

	query.Commit()

	_, err := j.client.Execute(query)

	if err != nil {
		return fmt.Errorf("could not create vertex/property types in JanusGraph")
	}

	// Update mapping
	j.UpdateStateInStateManager(ctx)
	return nil
}

// Drop a class from the schema.
func (j *Janusgraph) DropClass(ctx context.Context, kind kind.Kind, name string) error {
	log.Debugf("Removing class '%v' in JanusGraph", name)
	sanitizedClassName := schema.AssertValidClassName(name)

	vertexLabel := j.state.MustGetMappedClassName(sanitizedClassName)

	query := gremlin.G.V().HasLabel(string(vertexLabel)).
		HasString(PROP_KIND, kind.Name()).
		HasString(PROP_CLASS_ID, string(vertexLabel)).
		Drop()

	_, err := j.client.Execute(query)
	if err != nil {
		return fmt.Errorf("could not remove all data of the dropped class in JanusGraph: %s", err)
	}

	// Update mapping
	j.state.RemoveMappedClassName(sanitizedClassName)
	j.UpdateStateInStateManager(ctx)

	return nil
}

func (j *Janusgraph) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	if newClassName != nil {
		oldName := schema.AssertValidClassName(className)
		newName := schema.AssertValidClassName(*newClassName)
		j.state.RenameClass(oldName, newName)
		j.UpdateStateInStateManager(ctx)
	}

	return nil
}
func (j *Janusgraph) AddUnindexedProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	// Extra sanity check
	sanitizedClassName := schema.AssertValidClassName(className)

	query := gremlin_schema_query.New()
	sanitziedPropertyName := schema.AssertValidPropertyName(prop.Name)
	janusPropertyName := j.state.AddMappedPropertyName(sanitizedClassName, sanitziedPropertyName)

	// Determine the type of the property
	propertyDataType, err := j.schema.FindPropertyDataType(prop.AtDataType)
	if err != nil {
		// This must already be validated.
		panic(fmt.Sprintf("Data type fo property '%s' is invalid; %v", prop.Name, err))
	}

	if propertyDataType.IsPrimitive() {
		query.MakePropertyKey(string(janusPropertyName), weaviatePrimitivePropTypeToJanusPropType(schema.DataType(prop.AtDataType[0])), gremlin_schema_query.CARDINALITY_SINGLE)
	} else {
		// In principle, we could use a Many2One edge for SingleRefs
		query.MakeEdgeLabel(string(janusPropertyName), gremlin_schema_query.MULTIPLICITY_MULTI)
	}

	query.Commit()

	_, err = j.client.Execute(query)

	if err != nil {
		return fmt.Errorf("could not create property type in JanusGraph")
	}

	// Update mapping
	j.UpdateStateInStateManager(ctx)
	return nil
}

func (j *Janusgraph) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	// Extra sanity check
	sanitizedClassName := schema.AssertValidClassName(className)

	query := gremlin_schema_query.New()
	sanitziedPropertyName := schema.AssertValidPropertyName(prop.Name)
	janusPropertyName := j.state.AddMappedPropertyName(sanitizedClassName, sanitziedPropertyName)

	// Determine the type of the property
	propertyDataType, err := j.schema.FindPropertyDataType(prop.AtDataType)
	if err != nil {
		// This must already be validated.
		panic(fmt.Sprintf("Data type fo property '%s' is invalid; %v", prop.Name, err))
	}

	if propertyDataType.IsPrimitive() {
		if propertyDataType.AsPrimitive() == schema.DataTypeString {
			query.MakeIndexedPropertyKeyTextString(string(janusPropertyName), weaviatePrimitivePropTypeToJanusPropType(schema.DataType(prop.AtDataType[0])), gremlin_schema_query.CARDINALITY_SINGLE, INDEX_SEARCH)
		} else {
			query.MakeIndexedPropertyKey(string(janusPropertyName), weaviatePrimitivePropTypeToJanusPropType(schema.DataType(prop.AtDataType[0])), gremlin_schema_query.CARDINALITY_SINGLE, INDEX_SEARCH)
		}
	} else {
		// In principle, we could use a Many2One edge for SingleRefs
		query.MakeEdgeLabel(string(janusPropertyName), gremlin_schema_query.MULTIPLICITY_MULTI)
	}

	query.Commit()

	_, err = j.client.Execute(query)

	if err != nil {
		return fmt.Errorf("could not create property type in JanusGraph")
	}

	// Update mapping
	j.UpdateStateInStateManager(ctx)
	return nil
}

func (j *Janusgraph) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	if newName != nil {
		sanitizedClassName := schema.AssertValidClassName(className)
		oldName := schema.AssertValidPropertyName(propName)
		newName := schema.AssertValidPropertyName(*newName)
		j.state.RenameProperty(sanitizedClassName, oldName, newName)
		j.UpdateStateInStateManager(ctx)
	}
	return nil
}

func (j *Janusgraph) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	return nil
}

func (j *Janusgraph) DropProperty(ctx context.Context, kind kind.Kind, className string, propName string) error {
	sanitizedClassName := schema.AssertValidClassName(className)
	sanitizedPropName := schema.AssertValidPropertyName(propName)

	vertexLabel := j.state.MustGetMappedClassName(sanitizedClassName)
	mappedPropertyName := j.state.MustGetMappedPropertyName(sanitizedClassName, sanitizedPropName)

	err, prop := j.schema.GetProperty(kind, sanitizedClassName, sanitizedPropName)
	if err != nil {
		panic("could not get property")
	}

	propertyDataType, err := j.schema.FindPropertyDataType(prop.AtDataType)
	if err != nil {
		// This must already be validated.
		panic(fmt.Sprintf("Data type fo property '%s' is invalid; %v", prop.Name, err))
	}

	var query gremlin.Gremlin

	if propertyDataType.IsPrimitive() {
		query = gremlin.G.V().
			HasLabel(string(vertexLabel)).
			HasString(PROP_CLASS_ID, string(vertexLabel)).
			HasString(PROP_KIND, kind.Name()).
			Properties([]string{string(mappedPropertyName)}).
			Drop()
	} else {
		query = gremlin.G.E().
			HasLabel(string(mappedPropertyName)).
			HasString(PROP_REF_ID, string(mappedPropertyName)).
			Drop()
	}

	_, err = j.client.Execute(query)
	if err != nil {
		return fmt.Errorf("could not remove all data of the dropped class in JanusGraph: %s", err)
	}

	j.state.RemoveMappedPropertyName(sanitizedClassName, sanitizedPropName)
	j.UpdateStateInStateManager(ctx)
	return nil
}

// Get Janus data type from a weaviate data type.
// Panics if passed a wrong type.
func weaviatePrimitivePropTypeToJanusPropType(type_ schema.DataType) gremlin_schema_query.DataType {
	switch type_ {
	case schema.DataTypeString:
		return gremlin_schema_query.DATATYPE_STRING
	case schema.DataTypeText:
		return gremlin_schema_query.DATATYPE_STRING
	case schema.DataTypeInt:
		return gremlin_schema_query.DATATYPE_LONG
	case schema.DataTypeNumber:
		return gremlin_schema_query.DATATYPE_DOUBLE
	case schema.DataTypeBoolean:
		return gremlin_schema_query.DATATYPE_BOOLEAN
	case schema.DataTypeDate:
		return gremlin_schema_query.DATATYPE_STRING
	case schema.DataTypeGeoCoordinates:
		return gremlin_schema_query.DATATYPE_GEOSHAPE
	default:
		panic(fmt.Sprintf("unsupported data type '%v'", type_))
	}
}
