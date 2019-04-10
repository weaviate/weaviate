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
	"errors"
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/filters"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	libkind "github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

// kindClass is a helper struct that is independent of fixed types such as
// *models.Thing or *models.Action which are specific to their respecitve kind
// and don't contain a UUID
//
// kindClass groups all the fields required to convert them into one of the above
// mentioned stricter types, yet allow us to have generic methods to retrieve
// classes inside this package, such as passing data from the getClasses()
// helper to the graphqlresolver method, etc.
type kindClass struct {
	kind               kind.Kind
	atClass            string
	atContext          string
	properties         map[string]interface{}
	uuid               strfmt.UUID
	creationTimeUnix   int64
	lastUpdateTimeUnix int64
}

func (j *Janusgraph) getClass(k kind.Kind, searchUUID strfmt.UUID, atClass *string, foundUUID *strfmt.UUID, creationTimeUnix *int64, lastUpdateTimeUnix *int64, properties *models.Schema) error {
	// Fetch the class, it's key, and it's relations.
	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, string(searchUUID)).
		As("class").
		V().
		HasString(PROP_UUID, string(searchUUID)).
		Raw(`.optional(outE().has("refId").as("ref")).choose(select("ref"), select("class", "ref"), identity().project("class").by(select("class")))`)
	result, err := j.client.Execute(q)

	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return errors.New(connutils.StaticThingNotFound)
	}

	// The outputs 'thing' and 'key' will be repeated over all results. Just get them for one for now.
	vertex := result.Data[0].AssertKey("class").AssertVertex()

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

	if foundUUID != nil {
		*foundUUID = strfmt.UUID(vertex.AssertPropertyValue(PROP_UUID).AssertString())
	}

	kind := libkind.KindByName(vertex.AssertPropertyValue(PROP_KIND).AssertString())
	mappedClassName := state.MappedClassName(vertex.AssertPropertyValue(PROP_CLASS_ID).AssertString())
	className := j.state.GetClassNameFromMapped(mappedClassName)
	class := j.schema.GetClass(kind, className)
	if class == nil {
		panic(fmt.Sprintf("Could not get %s class '%s' from schema", kind.Name(), className))
	}

	if atClass != nil {
		*atClass = className.String()
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
			mappedPropertyName := state.MappedPropertyName(key)
			propertyName := j.state.GetPropertyNameFromMapped(className, mappedPropertyName)
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
		mappedPropertyName := state.MappedPropertyName(edge.AssertPropertyValue(PROP_REF_ID).AssertString())
		uuid := edge.AssertPropertyValue(PROP_REF_EDGE_CREF).AssertString()

		propertyName := j.state.GetPropertyNameFromMapped(className, mappedPropertyName)
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

			crefURI := crossref.New(locationUrl, strfmt.UUID(uuid), libkind.KindByName(type_)).String()
			ref["$cref"] = crefURI
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

func (j *Janusgraph) getClasses(k kind.Kind, className *schema.ClassName, first int, offset int,
	filter *common_filters.LocalFilter) ([]kindClass, error) {

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name())

	if className != nil {
		vertexLabel := j.state.MustGetMappedClassName(*className)
		q = q.HasString(PROP_CLASS_ID, string(vertexLabel))
	}

	filterQuery, err := filters.New(filter, &j.state).String()
	if err != nil {
		return nil, fmt.Errorf("could not build fiter query: %s", err)
	}

	q = q.Raw(filterQuery)

	q = q.
		Range(offset, first).
		AsProjectBy("class")
		// Raw(`.local(optional(outE().has("refId").as("ref")).choose(select("ref"), select("class", "ref"), identity().project("class").by(select("class"))))`)
	result, err := j.client.Execute(q)

	if err != nil {
		return nil, err
	}

	if len(result.Data) == 0 {
		return nil, errors.New(connutils.StaticThingNotFound)
	}

	classes := make([]kindClass, len(result.Data), len(result.Data))
	for i, datum := range result.Data {
		// The outputs 'thing' and 'key' will be repeated over all results. Just get them for one for now.
		vertex := datum.AssertKey("class").AssertVertex()

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

		uuid := strfmt.UUID(vertex.AssertPropertyValue(PROP_UUID).AssertString())

		kind := libkind.KindByName(vertex.AssertPropertyValue(PROP_KIND).AssertString())
		mappedClassName := state.MappedClassName(vertex.AssertPropertyValue(PROP_CLASS_ID).AssertString())
		className := j.state.GetClassNameFromMapped(mappedClassName)
		class := j.schema.GetClass(kind, className)
		if class == nil {
			panic(fmt.Sprintf("Could not get %s class '%s' from schema", kind.Name(), className))
		}

		atClass := className.String()
		atContext := vertex.AssertPropertyValue(PROP_AT_CONTEXT).AssertString()
		creationTimeUnix := vertex.AssertPropertyValue(PROP_CREATION_TIME_UNIX).AssertInt64()
		lastUpdateTimeUnix := vertex.AssertPropertyValue(PROP_LAST_UPDATE_TIME_UNIX).AssertInt64()

		classSchema := make(map[string]interface{})

		// Walk through all properties, check if they start with 'prop_', and then consider them to be 'schema' properties.
		// Just copy in the value directly. We're not doing any sanity check/casting to proper types for now.
		for key, val := range vertex.Properties {
			if strings.HasPrefix(key, "prop_") {
				mappedPropertyName := state.MappedPropertyName(key)
				propertyName := j.state.GetPropertyNameFromMapped(className, mappedPropertyName)
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
			mappedPropertyName := state.MappedPropertyName(edge.AssertPropertyValue(PROP_REF_ID).AssertString())
			uuid := edge.AssertPropertyValue(PROP_REF_EDGE_CREF).AssertString()

			propertyName := j.state.GetPropertyNameFromMapped(className, mappedPropertyName)
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

				crefURI := crossref.New(locationUrl, strfmt.UUID(uuid), libkind.KindByName(type_)).String()
				ref["$cref"] = crefURI
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

		classes[i] = kindClass{
			kind:               kind,
			atClass:            atClass,
			atContext:          atContext,
			properties:         classSchema,
			uuid:               uuid,
			creationTimeUnix:   creationTimeUnix,
			lastUpdateTimeUnix: lastUpdateTimeUnix,
		}
	}

	return classes, nil
}

func (j *Janusgraph) listClass(k kind.Kind, className *schema.ClassName, limit int, filter *common_filters.LocalFilter, yield func(id strfmt.UUID)) error {

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name())

	if className != nil {
		vertexLabel := j.state.MustGetMappedClassName(*className)
		q = q.HasString(PROP_CLASS_ID, string(vertexLabel))
	}

	filterQuery, err := filters.New(filter, &j.state).String()
	if err != nil {
		return fmt.Errorf("could not build fiter query: %s", err)
	}

	q = q.Raw(filterQuery)

	q = q.
		Limit(limit).
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
	case schema.DataTypeGeoCoordinates:
		return value.AssertGeoCoordinates()
	default:
		panic(fmt.Sprintf("Unknown primitive datatype '%v'", dataType))
	}
}

func pluralizeKindName(k string) string {
	switch k {
	case "NetworkAction":
		return "actions"
	case "NetworkThing":
		return "things"
	default:
		return strings.ToLower(k) + "s"
	}
}
