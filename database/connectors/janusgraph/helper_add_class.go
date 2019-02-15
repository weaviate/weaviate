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
	"github.com/creativesoftwarefdn/weaviate/models"
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

func (j *Janusgraph) addThingsBatch(things []*models.Thing, uuids []strfmt.UUID) error {
	k := kind.THING_KIND

	q := gremlin.New().Raw("g")

	for i, thing := range things {

		q = q.Raw("\n")
		className := schema.AssertValidClassName(thing.AtClass)
		vertexLabel := j.state.GetMappedClassName(className)
		sourceClassAlias := "classToBeAdded"
		uuid := uuids[i]

		q = q.AddV(string(vertexLabel)).
			As(sourceClassAlias).
			StringProperty(PROP_KIND, k.Name()).
			StringProperty(PROP_UUID, uuid.String()).
			StringProperty(PROP_CLASS_ID, string(vertexLabel)).
			StringProperty(PROP_AT_CONTEXT, thing.AtContext).
			Int64Property(PROP_CREATION_TIME_UNIX, thing.CreationTimeUnix).
			Int64Property(PROP_LAST_UPDATE_TIME_UNIX, thing.LastUpdateTimeUnix)

		var err error
		q, err = j.addEdgesToQuery(q, k, className, thing.Schema, sourceClassAlias)
		if err != nil {
			return err
		}
	}

	_, err := j.client.Execute(q)
	return err
}
