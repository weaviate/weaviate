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

	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) updateClass(ctx context.Context, k kind.Kind, className schema.ClassName,
	UUID strfmt.UUID, lastUpdateTimeUnix int64, rawProperties interface{}) error {
	vertexLabel := j.state.MustGetMappedClassName(className)

	sourceClassAlias := "classToBeUpdated"

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, UUID.String()).
		As(sourceClassAlias).
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, lastUpdateTimeUnix)

	q, err := j.addEdgesToQuery(ctx, q, k, className, rawProperties, sourceClassAlias)
	if err != nil {
		return err
	}

	_, err = j.client.Execute(ctx, q)
	return err
}
