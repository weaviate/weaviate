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
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/filters"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/meta"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/davecgh/go-spew/spew"
)

// LocalGetMeta based on GraphQL Query params
func (j *Janusgraph) LocalGetMeta(params *getmeta.Params) (interface{}, error) {
	spew.Dump(params)
	className := j.state.GetMappedClassName(params.ClassName)
	q := gremlin.New().Raw(`g.V()`).
		HasString("kind", params.Kind.Name()).
		HasString("classId", string(className))

	filterProvider := filters.New(params.Filters, &j.state)

	metaQuery, err := meta.NewQuery(params, &j.state, &j.schema, filterProvider).String()
	if err != nil {
		return nil, err
	}

	q = q.Raw(metaQuery)

	typeInfo, err := meta.NewTypeInspector(&j.schema).Process(params)
	if err != nil {
		return nil, err
	}

	return meta.NewProcessor(j.client, j.etcdClient, j.analyticsClient).Process(q, typeInfo, params)
}
