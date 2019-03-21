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
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) updateClass(k kind.Kind, className schema.ClassName, UUID strfmt.UUID, atContext string, lastUpdateTimeUnix int64, rawProperties interface{}) error {
	vertexLabel := j.state.MustGetMappedClassName(className)

	sourceClassAlias := "classToBeUpdated"

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, UUID.String()).
		As(sourceClassAlias).
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		StringProperty(PROP_AT_CONTEXT, atContext).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, lastUpdateTimeUnix)

	q, err := j.addEdgesToQuery(q, k, className, rawProperties, sourceClassAlias)
	if err != nil {
		return err
	}

	_, err = j.client.Execute(q)
	return err
}
