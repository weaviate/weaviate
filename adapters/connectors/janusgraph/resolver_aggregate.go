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
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/aggregate"
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

// LocalAggregate based on GraphQL Query params
func (j *Janusgraph) LocalAggregate(params *kinds.AggregateParams) (interface{}, error) {
	className := j.state.MustGetMappedClassName(params.ClassName)
	q := gremlin.New().Raw(`g.V()`).
		HasString("kind", params.Kind.Name()).
		HasString("classId", string(className))

	filterProvider := filters.New(params.Filters, &j.state)

	metaQuery, err := aggregate.NewQuery(params, &j.state, &j.schema, filterProvider).String()
	if err != nil {
		return nil, err
	}

	q = q.Raw(metaQuery)

	return aggregate.NewProcessor(j.client, j.etcdClient, j.analyticsClient).Process(q, params.GroupBy, params)
}
