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
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-openapi/strfmt"

	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/creativesoftwarefdn/weaviate/gremlin"

	"encoding/json"
)

func (j *Janusgraph) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	k := kind.THING_KIND

	fmt.Printf("########### ADDING THING #######\n")
	fmt.Printf("########### ADDING THING #######\n")
	fmt.Printf("########### ADDING THING #######\n")
	fmt.Printf("########### ADDING THING #######\n")
	fmt.Printf("########### ADDING THING #######\n")

	sanitizedClassName := schema.AssertValidClassName(thing.AtClass)
	vertexLabel := j.state.getMappedClassName(sanitizedClassName)

	q := gremlin.G.AddV(string(vertexLabel)).
		As("newClass").
		StringProperty(PROP_KIND, k.Name()).
		StringProperty(PROP_UUID, UUID.String()).
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		StringProperty(PROP_AT_CONTEXT, thing.AtContext).
		Int64Property(PROP_CREATION_TIME_UNIX, thing.CreationTimeUnix).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, thing.LastUpdateTimeUnix)

	// map properties in thing.Schema according to the mapping.

	type edgeToAdd struct {
		PropertyName string
		Type         string
		Reference    string
		Location     string
	}

	var edgesToAdd []edgeToAdd

	thingSchema, schema_ok := thing.Schema.(map[string]interface{})
	if schema_ok {
		for propName, value := range thingSchema {
			// TODO relation type
			// if primitive type:
			sanitziedPropertyName := schema.AssertValidPropertyName(propName)
			janusPropertyName := string(j.state.getMappedPropertyName(sanitizedClassName, sanitziedPropertyName))

			switch t := value.(type) {
			case string:
				q = q.StringProperty(janusPropertyName, t)
			case int:
				q = q.Int64Property(janusPropertyName, int64(t))
			case int8:
				q = q.Int64Property(janusPropertyName, int64(t))
			case int16:
				q = q.Int64Property(janusPropertyName, int64(t))
			case int32:
				q = q.Int64Property(janusPropertyName, int64(t))
			case int64:
				q = q.Int64Property(janusPropertyName, t)
			case bool:
				q = q.BoolProperty(janusPropertyName, t)
			case float32:
				q = q.Float64Property(janusPropertyName, float64(t))
			case float64:
				q = q.Float64Property(janusPropertyName, t)
			case time.Time:
				q = q.StringProperty(janusPropertyName, time.Time.String(t))
			case *models.SingleRef:
				panic("not supported yet")
				// Postpone creation of edges
				edgesToAdd = append(edgesToAdd, edgeToAdd{
					PropertyName: janusPropertyName,
					Reference:    t.NrDollarCref.String(),
					Type:         t.Type,
					Location:     *t.LocationURL,
				})
			default:
				j.messaging.ExitError(78, "The type "+reflect.TypeOf(value).String()+" is not supported for Thing properties.")
			}
		}
	}
	//
	//	// Add edges to all referened things.
	//	for _, edge := range edgesToAdd {
	//		q = q.AddE("thingEdge").
	//			FromRef("newClass").
	//			ToQuery(gremlin.G.V().HasLabel(THING_LABEL).HasString("uuid", edge.Reference)).
	//			StringProperty(PROPERTY_EDGE_LABEL, edge.PropertyName).
	//			StringProperty("$cref", edge.Reference).
	//			StringProperty("type", edge.Type).
	//			StringProperty("locationUrl", edge.Location)
	//	}

	// Link to key
	q = q.AddE(KEY_VERTEX_LABEL).
		StringProperty("locationUrl", *thing.Key.LocationURL).
		FromRef("newClass").
		ToQuery(gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).HasString(PROP_UUID, thing.Key.NrDollarCref.String()))

	_, err := j.client.Execute(q)

	return err
}

func (f *Janusgraph) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	// Fetch the thing, it's key, and it's relations.
	q := gremlin.G.V().
		HasString(PROP_UUID, string(UUID)).
		As("class").
		OutEWithLabel(KEY_VERTEX_LABEL).As("keyEdge").
		InV().Path().FromRef("keyEdge").As("key"). // also get the path, so that we can learn about the location of the key.
		V().
		HasString(PROP_UUID, string(UUID)).
		Raw(`.optional(outE("thingEdge").as("thingEdge").as("ref")).choose(select("ref"), select("class", "key", "ref"), select("class", "key"))`)

	result, err := f.client.Execute(q)

	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return errors.New(connutils.StaticThingNotFound)
	}

	// The outputs 'thing' and 'key' will be repeated over all results. Just get them for one for now.
	thingVertex := result.Data[0].AssertKey("class").AssertVertex()
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

	thingResponse.Key = newKeySingleRefFromKeyPath(keyPath)
	return f.fillThingResponseFromVertexAndEdges(thingVertex, refEdges, thingResponse)
	return nil
}

// TODO check
func (f *Janusgraph) GetThings(ctx context.Context, UUIDs []strfmt.UUID, response *models.ThingsListResponse) error {
	//	// TODO: Optimize query to perform just _one_ JanusGraph lookup.
	//
	//	response.TotalResults = 0
	//	response.Things = make([]*models.ThingGetResponse, 0)
	//
	//	for _, uuid := range UUIDs {
	//		var thing_response models.ThingGetResponse
	//		err := f.GetThing(ctx, uuid, &thing_response)
	//
	//		if err == nil {
	//			response.TotalResults += 1
	//			response.Things = append(response.Things, &thing_response)
	//		} else {
	//			return fmt.Errorf("%s: thing with UUID '%v' not found", connutils.StaticThingNotFound, uuid)
	//		}
	//	}
	//
	//	return nil
	return nil
}

// TODO check
func (f *Janusgraph) ListThings(ctx context.Context, first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, response *models.ThingsListResponse) error {
	//
	//	if len(wheres) > 0 {
	//		return errors.New("Wheres are not supported in LisThings")
	//	}
	//
	//	q := gremlin.G.V().
	//		HasLabel(THING_LABEL).
	//		Range(offset, first).
	//		Values([]string{"uuid"})
	//
	//	result, err := f.client.Execute(q)
	//
	//	if err != nil {
	//		return err
	//	}
	//
	//	response.TotalResults = 0
	//	response.Things = make([]*models.ThingGetResponse, 0)
	//
	//	// Get the UUIDs from the first query.
	//	UUIDs := result.AssertStringSlice()
	//
	//	for _, uuid := range UUIDs {
	//		var thing_response models.ThingGetResponse
	//		err := f.GetThing(ctx, strfmt.UUID(uuid), &thing_response)
	//
	//		if err == nil {
	//			response.TotalResults += 1
	//			response.Things = append(response.Things, &thing_response)
	//		} else {
	//			// skip silently; it's probably deleted.
	//		}
	//	}
	//
	return nil
}

func (f *Janusgraph) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	//	// Base settings
	//	q := gremlin.G.V().HasLabel(THING_LABEL).
	//		HasString("uuid", string(UUID)).
	//		As("thing").
	//		StringProperty("atClass", thing.AtClass).
	//		StringProperty("context", thing.AtContext).
	//		Int64Property("creationTimeUnix", thing.CreationTimeUnix).
	//		Int64Property("lastUpdateTimeUnix", thing.LastUpdateTimeUnix)
	//
	//	type expectedEdge struct {
	//		PropertyName string
	//		Type         string
	//		Reference    string
	//		Location     string
	//	}
	//
	//	var expectedEdges []expectedEdge
	//
	//	schema, schema_ok := thing.Schema.(map[string]interface{})
	//	if schema_ok {
	//		for key, value := range schema {
	//			janusgraphPropertyName := "schema__" + key
	//			switch t := value.(type) {
	//			case string:
	//				q = q.StringProperty(janusgraphPropertyName, t)
	//			case int:
	//				q = q.Int64Property(janusgraphPropertyName, int64(t))
	//			case int8:
	//				q = q.Int64Property(janusgraphPropertyName, int64(t))
	//			case int16:
	//				q = q.Int64Property(janusgraphPropertyName, int64(t))
	//			case int32:
	//				q = q.Int64Property(janusgraphPropertyName, int64(t))
	//			case int64:
	//				q = q.Int64Property(janusgraphPropertyName, t)
	//			case bool:
	//				q = q.BoolProperty(janusgraphPropertyName, t)
	//			case float32:
	//				q = q.Float64Property(janusgraphPropertyName, float64(t))
	//			case float64:
	//				q = q.Float64Property(janusgraphPropertyName, t)
	//			case time.Time:
	//				q = q.StringProperty(janusgraphPropertyName, time.Time.String(t))
	//			case *models.SingleRef:
	//				// Postpone creation of edges
	//				expectedEdges = append(expectedEdges, expectedEdge{
	//					PropertyName: janusgraphPropertyName,
	//					Reference:    t.NrDollarCref.String(),
	//					Type:         t.Type,
	//					Location:     *t.LocationURL,
	//				})
	//			default:
	//				f.messaging.ExitError(78, "The type "+reflect.TypeOf(value).String()+" is not supported for Thing properties.")
	//			}
	//		}
	//	}
	//
	//	// Update all edges to all referened things.
	//	// TODO: verify what to if we're not mentioning some reference? how should we remove such a reference?
	//	for _, edge := range expectedEdges {
	//		// First drop the edge
	//		q = q.Optional(gremlin.Current().OutEWithLabel("thingEdge").HasString(PROPERTY_EDGE_LABEL, edge.PropertyName).Drop()).
	//			AddE("thingEdge").
	//			FromRef("thing").
	//			ToQuery(gremlin.G.V().HasLabel(THING_LABEL).HasString("uuid", edge.Reference)).
	//			StringProperty(PROPERTY_EDGE_LABEL, edge.PropertyName).
	//			StringProperty("$cref", edge.Reference).
	//			StringProperty("type", edge.Type).
	//			StringProperty("locationUrl", edge.Location)
	//	}
	//
	//	// Don't update the key.
	//	// TODO verify that indeed this is the desired behaviour.
	//
	//	_, err := f.client.Execute(q)
	//
	//	return err
	return nil
}

func (f *Janusgraph) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	//	q := gremlin.G.V().HasLabel(THING_LABEL).
	//		HasString("uuid", string(UUID)).
	//		Drop()
	//
	//	_, err := f.client.Execute(q)
	//
	//	return err
	return nil
}

func (f *Janusgraph) HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error {
	return nil
}

func (f *Janusgraph) MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	return nil
}

func debug(result interface{}) {
	j, _ := json.MarshalIndent(result, "", " ")
	fmt.Printf("%v\n", string(j))
}
