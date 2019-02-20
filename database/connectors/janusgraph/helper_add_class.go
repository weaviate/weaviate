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
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	batchmodels "github.com/creativesoftwarefdn/weaviate/restapi/batch/models"
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) addClass(k kind.Kind, className schema.ClassName, UUID strfmt.UUID, atContext string, creationTimeUnix int64, lastUpdateTimeUnix int64, rawProperties interface{}) error {
	vertexLabel := j.state.GetMappedClassName(className)
	sourceClassAlias := "classToBeAdded"

	q := gremlin.G.AddV(string(vertexLabel)).
		As(sourceClassAlias).
		StringProperty(PROP_KIND, k.Name()).
		StringProperty(PROP_UUID, UUID.String()).
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		StringProperty(PROP_AT_CONTEXT, atContext).
		Int64Property(PROP_CREATION_TIME_UNIX, creationTimeUnix).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, lastUpdateTimeUnix)

	q, err := j.addEdgesToQuery(q, k, className, rawProperties, sourceClassAlias)
	if err != nil {
		return err
	}

	_, err = j.client.Execute(q)

	return err
}

// MaximumBatchItemsPerQuery is the threshold when batches will be broken up
// into smaller chunks so we avoid StackOverflowExceptions in the Janus backend
const MaximumBatchItemsPerQuery = 50

func (j *Janusgraph) addThingsBatch(things batchmodels.Things) error {
	chunkSize := MaximumBatchItemsPerQuery
	chunks := len(things) / chunkSize
	if len(things) < chunkSize {
		chunks = 1
	}
	chunked := make([][]batchmodels.Thing, chunks)
	chunk := 0

	for i := 0; i < len(things); i++ {
		if i%chunkSize == 0 {
			if i != 0 {
				chunk++
			}

			currentChunkSize := chunkSize
			if len(things)-i < chunkSize {
				currentChunkSize = len(things) - i
			}
			chunked[chunk] = make([]batchmodels.Thing, currentChunkSize)
		}
		chunked[chunk][i%chunkSize] = things[i]
	}

	for _, chunk := range chunked {
		k := kind.THING_KIND

		q := gremlin.New().Raw("g")

		for _, thing := range chunk {
			if thing.Err != nil {
				// an error that happened prior to this point int time could have been a
				// validation error. We simply skip over it right now, as it has
				// already errored. The reason it is still included in this list is so
				// that the result list matches the incoming list exactly in order and
				// length, so the user can easily deduce which individual class could
				// be imported and which failed.
				continue
			}

			q = q.Raw("\n")
			className := schema.AssertValidClassName(thing.Thing.AtClass)
			vertexLabel := j.state.GetMappedClassName(className)
			sourceClassAlias := "classToBeAdded"

			q = q.AddV(string(vertexLabel)).
				As(sourceClassAlias).
				StringProperty(PROP_KIND, k.Name()).
				StringProperty(PROP_UUID, thing.UUID.String()).
				StringProperty(PROP_CLASS_ID, string(vertexLabel)).
				StringProperty(PROP_AT_CONTEXT, thing.Thing.AtContext).
				Int64Property(PROP_CREATION_TIME_UNIX, thing.Thing.CreationTimeUnix).
				Int64Property(PROP_LAST_UPDATE_TIME_UNIX, thing.Thing.LastUpdateTimeUnix)

			var err error
			q, err = j.addEdgesToQuery(q, k, className, thing.Thing.Schema, sourceClassAlias)
			if err != nil {
				return err
			}
		}

		if q.String() == "g" {
			// it seems we didn't get a single valid item, our query is still the same
			// as before. Let's return. The API package is aware of all prior errors
			// and can send them to the user correctly.
			return nil
		}

		_, err := j.client.Execute(q)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *Janusgraph) addActionsBatch(actions batchmodels.Actions) error {
	chunkSize := MaximumBatchItemsPerQuery
	chunks := len(actions) / chunkSize
	if len(actions) < chunkSize {
		chunks = 1
	}
	chunked := make([][]batchmodels.Action, chunks)
	chunk := 0

	for i := 0; i < len(actions); i++ {
		if i%chunkSize == 0 {
			if i != 0 {
				chunk++
			}

			currentChunkSize := chunkSize
			if len(actions)-i < chunkSize {
				currentChunkSize = len(actions) - i
			}
			chunked[chunk] = make([]batchmodels.Action, currentChunkSize)
		}
		chunked[chunk][i%chunkSize] = actions[i]
	}

	for _, chunk := range chunked {
		k := kind.ACTION_KIND

		q := gremlin.New().Raw("g")

		for _, action := range chunk {
			if action.Err != nil {
				// an error that happened prior to this point int time could have been a
				// validation error. We simply skip over it right now, as it has
				// already errored. The reason it is still included in this list is so
				// that the result list matches the incoming list exactly in order and
				// length, so the user can easily deduce which individual class could
				// be imported and which failed.
				continue
			}

			q = q.Raw("\n")
			className := schema.AssertValidClassName(action.Action.AtClass)
			vertexLabel := j.state.GetMappedClassName(className)
			sourceClassAlias := "classToBeAdded"

			q = q.AddV(string(vertexLabel)).
				As(sourceClassAlias).
				StringProperty(PROP_KIND, k.Name()).
				StringProperty(PROP_UUID, action.UUID.String()).
				StringProperty(PROP_CLASS_ID, string(vertexLabel)).
				StringProperty(PROP_AT_CONTEXT, action.Action.AtContext).
				Int64Property(PROP_CREATION_TIME_UNIX, action.Action.CreationTimeUnix).
				Int64Property(PROP_LAST_UPDATE_TIME_UNIX, action.Action.LastUpdateTimeUnix)

			var err error
			q, err = j.addEdgesToQuery(q, k, className, action.Action.Schema, sourceClassAlias)
			if err != nil {
				return err
			}
		}

		if q.String() == "g" {
			// it seems we didn't get a single valid item, our query is still the same
			// as before. Let's return. The API package is aware of all prior errors
			// and can send them to the user correctly.
			return nil
		}

		_, err := j.client.Execute(q)
		if err != nil {
			return err
		}
	}

	return nil
}
