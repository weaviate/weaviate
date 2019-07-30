//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package janusgraph

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (j *Janusgraph) updateClass(ctx context.Context, k kind.Kind, className schema.ClassName,
	UUID strfmt.UUID, lastUpdateTimeUnix int64, rawProperties interface{}) error {
	vertexLabel := j.state.MustGetMappedClassName(className)

	l := j.logger.
		WithField("action", "janusgraph_class_update").
		WithField("kind", k.Name()).
		WithField("className", className).
		WithField("mappedClassName", vertexLabel).
		WithField("rawProperties", rawProperties)

	l.WithField("event", "request_reveiced").
		Debug("reveiced class update request")

	sourceClassAlias := "classToBeUpdated"

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, UUID.String()).
		As(sourceClassAlias).
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, lastUpdateTimeUnix)

	l.WithField("event", "base_query_built").
		WithField("query", q.String()).
		Debug("built base query")

	q, err := j.addEdgesToQuery(ctx, q, k, className, rawProperties, sourceClassAlias)
	if err != nil {
		l.WithField("event", "add_edges_to_query").
			WithField("query", q.String()).
			WithError(err).
			Error("could not add edges to query")
		return err
	}

	l.WithField("event", "add_edges_to_query").
		WithField("query", q.String()).
		Debug("added edges to query")

	_, err = j.client.Execute(ctx, q)
	return err
}
