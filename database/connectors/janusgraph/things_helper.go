package janusgraph

import (
	"strings"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) fillThingResponseFromVertexAndEdges(vertex *gremlin.Vertex, refEdges []*gremlin.Edge, thingResponse *models.ThingGetResponse) error {
	thingResponse.ThingID = strfmt.UUID(vertex.AssertPropertyValue(PROP_UUID).AssertString())
	mappedClassName := MappedClassName(vertex.AssertPropertyValue(PROP_CLASS_ID).AssertString())
	className := j.state.getClassNameFromMapped(mappedClassName)

	thingResponse.AtClass = className.String()
	thingResponse.AtContext = vertex.AssertPropertyValue(PROP_AT_CONTEXT).AssertString()

	thingResponse.CreationTimeUnix = vertex.AssertPropertyValue(PROP_CREATION_TIME_UNIX).AssertInt64()
	thingResponse.LastUpdateTimeUnix = vertex.AssertPropertyValue(PROP_LAST_UPDATE_TIME_UNIX).AssertInt64()

	schema := make(map[string]interface{})

	// Walk through all properties, check if they start with 'prop_', and then consider them to be 'schema' properties.
	// Just copy in the value directly. We're not doing any sanity check/casting to proper types for now.
	for key, val := range vertex.Properties {
		if strings.HasPrefix(key, "prop_") {
			mappedPropertyName := MappedPropertyName(key)
			propertyName := j.state.getPropertyNameFromMapped(className, mappedPropertyName)
			schema[propertyName.String()] = val.Value.Value
		}
	}

	// For each of the connected edges, get the property values,
	// and store the reference.
	for _, edge := range refEdges {
		locationUrl := edge.AssertPropertyValue("locationUrl").AssertString()
		type_ := edge.AssertPropertyValue("type").AssertString()
		edgeName := edge.AssertPropertyValue("propertyEdge").AssertString()
		uuid := edge.AssertPropertyValue("$cref").AssertString()

		key := edgeName[8:len(edgeName)]
		ref := make(map[string]interface{})
		ref["$cref"] = uuid
		ref["locationUrl"] = locationUrl
		ref["type"] = type_
		schema[key] = ref
	}

	thingResponse.Schema = schema

	return nil
}
