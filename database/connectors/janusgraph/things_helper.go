package janusgraph

import (
	"strings"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/go-openapi/strfmt"
)

func fillThingResponseFromVertexAndEdges(vertex *gremlin.Vertex, refEdges []*gremlin.Edge, thingResponse *models.ThingGetResponse) error {
	// TODO: We should actually read stuff from the database schema, then get only that stuff from JanusGraph.
	// At this moment, we're just parsing whetever there is in JanusGraph, which might not agree with the database schema
	// that is defined in Weaviate.

	thingResponse.ThingID = strfmt.UUID(vertex.AssertPropertyValue("uuid").AssertString())
	thingResponse.AtClass = vertex.AssertPropertyValue("atClass").AssertString()
	thingResponse.AtContext = vertex.AssertPropertyValue("context").AssertString()

	thingResponse.CreationTimeUnix = vertex.AssertPropertyValue("creationTimeUnix").AssertInt64()
	thingResponse.LastUpdateTimeUnix = vertex.AssertPropertyValue("lastUpdateTimeUnix").AssertInt64()

	schema := make(map[string]interface{})

	// Walk through all properties, check if they start with 'schema__', and then consider them to be 'schema' properties.
	// Just copy in the value directly. We're not doing any sanity check/casting to proper types for now.
	for key, val := range vertex.Properties {
		if strings.HasPrefix(key, "schema__") {
			key = key[8:len(key)]
			schema[key] = val.Value.Value
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
