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
package janusgraph

import (
	"errors"
	"fmt"
	"strings"

	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) getClass(k kind.Kind, searchUUID strfmt.UUID, atClass *string, atContext *string, foundUUID *strfmt.UUID, creationTimeUnix *int64, lastUpdateTimeUnix *int64, properties *models.Schema, key **models.SingleRef) error {
	// Fetch the class, it's key, and it's relations.
	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, string(searchUUID)).
		As("class").
		OutEWithLabel(KEY_VERTEX_LABEL).As("keyEdge").
		InV().Path().FromRef("keyEdge").As("key"). // also get the path, so that we can learn about the location of the key.
		V().
		HasString(PROP_UUID, string(searchUUID)).
		Raw(`.optional(outE().has("refId").as("ref")).choose(select("ref"), select("class", "key", "ref"), select("class", "key"))`)
	result, err := j.client.Execute(q)

	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return errors.New(connutils.StaticThingNotFound)
	}

	// The outputs 'thing' and 'key' will be repeated over all results. Just get them for one for now.
	vertex := result.Data[0].AssertKey("class").AssertVertex()
	keyPath := result.Data[0].AssertKey("key").AssertPath()

	// However, we can get multiple refs. In that case, we'll have multiple datums,
	// each with the same thing & key, but a different ref.
	// Let's extract those refs.
	var refEdges []*gremlin.Edge
	for _, datum := range result.Data {
		ref, err := datum.Key("ref")
		if err == nil {
			refEdges = append(refEdges, ref.AssertEdge())
		}
	}

	if key != nil {
		*key = newKeySingleRefFromKeyPath(keyPath)
	}

	if foundUUID != nil {
		*foundUUID = strfmt.UUID(vertex.AssertPropertyValue(PROP_UUID).AssertString())
	}

	kind := kind.KindByName(vertex.AssertPropertyValue(PROP_KIND).AssertString())
	mappedClassName := MappedClassName(vertex.AssertPropertyValue(PROP_CLASS_ID).AssertString())
	className := j.state.getClassNameFromMapped(mappedClassName)
	class := j.schema.GetClass(kind, className)
	if class == nil {
		panic(fmt.Sprintf("Could not get %s class '%s' from schema", kind.Name(), className))
	}

	if atClass != nil {
		*atClass = className.String()
	}

	if atContext != nil {
		*atContext = vertex.AssertPropertyValue(PROP_AT_CONTEXT).AssertString()
	}

	if creationTimeUnix != nil {
		*creationTimeUnix = vertex.AssertPropertyValue(PROP_CREATION_TIME_UNIX).AssertInt64()
	}

	if lastUpdateTimeUnix != nil {
		*lastUpdateTimeUnix = vertex.AssertPropertyValue(PROP_LAST_UPDATE_TIME_UNIX).AssertInt64()
	}

	classSchema := make(map[string]interface{})

	// Walk through all properties, check if they start with 'prop_', and then consider them to be 'schema' properties.
	// Just copy in the value directly. We're not doing any sanity check/casting to proper types for now.
	for key, val := range vertex.Properties {
		if strings.HasPrefix(key, "prop_") {
			mappedPropertyName := MappedPropertyName(key)
			propertyName := j.state.getPropertyNameFromMapped(className, mappedPropertyName)
			err, property := j.schema.GetProperty(kind, className, propertyName)
			if err != nil {
				panic(fmt.Sprintf("Could not get property '%s' in class %s ; %v", propertyName, className, err))
			}

			propType, err := j.schema.FindPropertyDataType(property.AtDataType)
			if err != nil {
				panic(fmt.Sprintf("Could not decode property '%s'; %v", propertyName, err))
			}

			if propType.IsPrimitive() {
				classSchema[propertyName.String()] = decodeJanusPrimitiveType(propType.AsPrimitive(), val.Value)
			} else {
				panic(fmt.Sprintf("Property '%s' should be a primitive type!", propertyName))
			}
		}
	}

	// For each of the connected edges, get the property values,
	// and store the reference.
	for _, edge := range refEdges {
		locationUrl := edge.AssertPropertyValue(PROP_REF_EDGE_LOCATION).AssertString()
		type_ := edge.AssertPropertyValue(PROP_REF_EDGE_TYPE).AssertString()
		mappedPropertyName := MappedPropertyName(edge.AssertPropertyValue(PROP_REF_ID).AssertString())
		uuid := edge.AssertPropertyValue(PROP_REF_EDGE_CREF).AssertString()

		propertyName := j.state.getPropertyNameFromMapped(className, mappedPropertyName)
		err, property := j.schema.GetProperty(kind, className, propertyName)
		if err != nil {
			panic(fmt.Sprintf("Could not get property '%s' in class %s ; %v", propertyName, className, err))
		}

		propType, err := j.schema.FindPropertyDataType(property.AtDataType)
		if err != nil {
			panic(fmt.Sprintf("Could not get property type of '%s' in class %s; %v", property.AtDataType, className, err))
		}

		if propType.IsReference() {
			ref := make(map[string]interface{})
			ref["$cref"] = uuid
			ref["locationUrl"] = locationUrl
			ref["type"] = type_
			switch schema.CardinalityOfProperty(property) {
			case schema.CardinalityAtMostOne:
				classSchema[propertyName.String()] = ref
			case schema.CardinalityMany:
				var potentialMany []interface{}
				potentialMany_, present := classSchema[propertyName.String()]
				if present {
					potentialMany = potentialMany_.([]interface{})
				} else {
					potentialMany = make([]interface{}, 0)
					classSchema[propertyName.String()] = potentialMany
				}
				classSchema[propertyName.String()] = append(potentialMany, ref)
			default:
				panic(fmt.Sprintf("Unexpected cardinality %v", schema.CardinalityOfProperty(property)))
			}
		} else {
			panic(fmt.Sprintf("Property '%s' should be a reference type!", propertyName))
		}
	}

	if properties != nil {
		*properties = classSchema
	}

	return nil
}

func (j *Janusgraph) listClass(k kind.Kind, className *schema.ClassName, first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, yield func(id strfmt.UUID)) error {
	if len(wheres) > 0 {
		return errors.New("Wheres are not supported in ListThings")
	}

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name())

	if className != nil {
		vertexLabel := j.state.getMappedClassName(*className)
		q = q.HasString(PROP_CLASS_ID, string(vertexLabel))
	}

	q = q.
		Range(offset, first).
		Values([]string{PROP_UUID})

	result, err := j.client.Execute(q)

	if err != nil {
		return err
	}

	// Get the UUIDs from the first query.
	UUIDs := result.AssertStringSlice()

	for _, uuid := range UUIDs {
		yield(strfmt.UUID(uuid))
	}

	return nil
}

func (j *Janusgraph) deleteClass(k kind.Kind, UUID strfmt.UUID) error {
	q := gremlin.G.V().HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, string(UUID)).
		Drop()

	_, err := j.client.Execute(q)

	return err
}

func decodeJanusPrimitiveType(dataType schema.DataType, value gremlin.PropertyValue) interface{} {
	switch dataType {
	case schema.DataTypeInt:
		return value.AssertInt()
	case schema.DataTypeNumber:
		return value.AssertFloat()
	case schema.DataTypeString:
		return value.AssertString()
	case schema.DataTypeText:
		return value.AssertString()
	case schema.DataTypeBoolean:
		return value.AssertBool()
	case schema.DataTypeDate:
		// TODO; reformat?
		return value.AssertString()
	default:
		panic(fmt.Sprintf("Unknown primitive datatype '%v'", dataType))
	}
}
