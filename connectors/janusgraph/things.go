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

	connutils "github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/creativesoftwarefdn/weaviate/gremlin"

	"encoding/json"
)

func (f *Janusgraph) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	// Base settings
	q := gremlin.G.AddV(THING_LABEL).
		As("newThing").
		StringProperty("uuid", string(UUID)).
		StringProperty("atClass", thing.AtClass).
		StringProperty("context", thing.AtContext).
		Int64Property("creationTimeUnix", thing.CreationTimeUnix).
		Int64Property("lastUpdateTimeUnix", thing.LastUpdateTimeUnix)

	type edgeToAdd struct {
		PropertyName string
		Type         string
		Reference    string
		Location     string
	}

	var edgesToAdd []edgeToAdd

	schema, schema_ok := thing.Schema.(map[string]interface{})
	if schema_ok {
		for key, value := range schema {
			janusgraphPropertyName := "schema__" + key
			switch t := value.(type) {
			case string:
				q = q.StringProperty(janusgraphPropertyName, t)
			case int:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int8:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int16:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int32:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int64:
				q = q.Int64Property(janusgraphPropertyName, t)
			case bool:
				q = q.BoolProperty(janusgraphPropertyName, t)
			case float32:
				q = q.Float64Property(janusgraphPropertyName, float64(t))
			case float64:
				q = q.Float64Property(janusgraphPropertyName, t)
			case time.Time:
				q = q.StringProperty(janusgraphPropertyName, time.Time.String(t))
			case *models.SingleRef:
				// Postpone creation of edges
				edgesToAdd = append(edgesToAdd, edgeToAdd{
					PropertyName: janusgraphPropertyName,
					Reference:    t.NrDollarCref.String(),
					Type:         t.Type,
					Location:     *t.LocationURL,
				})
			default:
				f.messaging.ExitError(78, "The type "+reflect.TypeOf(value).String()+" is not supported for Thing properties.")
			}
		}
	}

	// Add edges to all referened things.
	for _, edge := range edgesToAdd {
		q = q.AddE("thingEdge").
			FromRef("newThing").
			ToQuery(gremlin.G.V().HasLabel(THING_LABEL).HasString("uuid", edge.Reference)).
			StringProperty(PROPERTY_EDGE_LABEL, edge.PropertyName).
			StringProperty("$cref", edge.Reference).
			StringProperty("type", edge.Type).
			StringProperty("locationUrl", edge.Location)
	}

	// Link to key
	q = q.AddE(KEY_LABEL).
		StringProperty("locationUrl", *thing.Key.LocationURL).
		FromRef("newThing").
		ToQuery(gremlin.G.V().HasLabel(KEY_LABEL).HasString("uuid", thing.Key.NrDollarCref.String()))

	_, err := f.client.Execute(q)

	return err
}

func (f *Janusgraph) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	// Fetch the thing, it's key, and it's relations.
	q := gremlin.G.V().
		HasLabel(THING_LABEL).
		HasString("uuid", string(UUID)).
		As("thing").
		OutEWithLabel(KEY_LABEL).As("keyEdge").
		InV().Path().FromRef("keyEdge").As("key"). // also get the path, so that we can learn about the location of the key.
		V().
		HasLabel(THING_LABEL).
		HasString("uuid", string(UUID)).
		Raw(`.optional(outE("thingEdge").as("thingEdge").as("ref")).choose(select("ref"), select("thing", "key", "ref"), select("thing", "key"))`)

	result, err := f.client.Execute(q)

	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		fmt.Printf("\n\n\n\n\n\n\n\nNOTHING FOUND\n\n\n\n\n")
		return errors.New(connutils.StaticThingNotFound)
	}

	// The outputs 'thing' and 'key' will be repeated over all results. Just get them for one for now.
	thingVertex := result.Data[0].AssertKey("thing").AssertVertex()
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
	return fillThingResponseFromVertexAndEdges(thingVertex, refEdges, thingResponse)
}

// TODO check
func (f *Janusgraph) GetThings(ctx context.Context, UUIDs []strfmt.UUID, response *models.ThingsListResponse) error {
	// TODO: Optimize query to perform just _one_ JanusGraph lookup.

	response.TotalResults = 0
	response.Things = make([]*models.ThingGetResponse, 0)

	for _, uuid := range UUIDs {
		var thing_response models.ThingGetResponse
		err := f.GetThing(ctx, uuid, &thing_response)

		if err != nil {
			response.TotalResults += 1
			response.Things = append(response.Things, &thing_response)
		} else {
			return fmt.Errorf("%s: thing with UUID '%v' not found", connutils.StaticThingNotFound, uuid)
		}
	}

	return nil
}

// TODO check
func (f *Janusgraph) ListThings(ctx context.Context, first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	if len(wheres) > 0 {
		panic("Wheres are not supported in LisThings")
	}
	return nil
}

func (f *Janusgraph) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	// Base settings
	q := gremlin.G.V().HasLabel(THING_LABEL).
		HasString("uuid", string(UUID)).
		As("thing").
		StringProperty("atClass", thing.AtClass).
		StringProperty("context", thing.AtContext).
		Int64Property("creationTimeUnix", thing.CreationTimeUnix).
		Int64Property("lastUpdateTimeUnix", thing.LastUpdateTimeUnix)

	type expectedEdge struct {
		PropertyName string
		Type         string
		Reference    string
		Location     string
	}

	var expectedEdges []expectedEdge

	schema, schema_ok := thing.Schema.(map[string]interface{})
	if schema_ok {
		for key, value := range schema {
			janusgraphPropertyName := "schema__" + key
			switch t := value.(type) {
			case string:
				q = q.StringProperty(janusgraphPropertyName, t)
			case int:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int8:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int16:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int32:
				q = q.Int64Property(janusgraphPropertyName, int64(t))
			case int64:
				q = q.Int64Property(janusgraphPropertyName, t)
			case bool:
				q = q.BoolProperty(janusgraphPropertyName, t)
			case float32:
				q = q.Float64Property(janusgraphPropertyName, float64(t))
			case float64:
				q = q.Float64Property(janusgraphPropertyName, t)
			case time.Time:
				q = q.StringProperty(janusgraphPropertyName, time.Time.String(t))
			case *models.SingleRef:
				// Postpone creation of edges
				expectedEdges = append(expectedEdges, expectedEdge{
					PropertyName: janusgraphPropertyName,
					Reference:    t.NrDollarCref.String(),
					Type:         t.Type,
					Location:     *t.LocationURL,
				})
			default:
				f.messaging.ExitError(78, "The type "+reflect.TypeOf(value).String()+" is not supported for Thing properties.")
			}
		}
	}

	// Update all edges to all referened things.
	// TODO: verify what to if we're not mentioning some reference? how should we remove such a reference?
	for _, edge := range expectedEdges {
		// First drop the edge
		q = q.Optional(gremlin.Current().OutEWithLabel("thingEdge").HasString(PROPERTY_EDGE_LABEL, edge.PropertyName).Drop()).
			AddE("thingEdge").
			FromRef("thing").
			ToQuery(gremlin.G.V().HasLabel(THING_LABEL).HasString("uuid", edge.Reference)).
			StringProperty(PROPERTY_EDGE_LABEL, edge.PropertyName).
			StringProperty("$cref", edge.Reference).
			StringProperty("type", edge.Type).
			StringProperty("locationUrl", edge.Location)
	}

	// Don't update the key.
	// TODO verify that indeed this is the desired behaviour.

	_, err := f.client.Execute(q)

	return err
}

func (f *Janusgraph) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	q := gremlin.G.V().HasLabel(THING_LABEL).
		HasString("uuid", string(UUID)).
		Drop()

	_, err := f.client.Execute(q)

	return err
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
