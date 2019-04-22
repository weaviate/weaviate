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

package aggregate

import (
	"context"
	"fmt"
	"strings"

	analytics "github.com/SeMI-network/janus-spark-analytics/clients/go"
	"github.com/coreos/etcd/clientv3"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

// AnalyticsAPICachePrefix is prepended to the ids to form the keys in the
// key-value cache storage
const AnalyticsAPICachePrefix = "/weaviate/janusgraph-connector/analytics-cache/"

// Processor is a simple Gremlin-Query Executor that is specific to GetMeta in
// that it merges the results into the expected format and applies
// post-processing where necessary
type Processor struct {
	executor  executor
	cache     etcdClient
	analytics analyticsClient
}

type etcdClient interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

type analyticsClient interface {
	Schedule(ctx context.Context, params analytics.QueryParams) error
}

type executor interface {
	Execute(ctx context.Context, query gremlin.Gremlin) (*gremlin.Response, error)
}

//NewProcessor of type Processor
func NewProcessor(executor executor, cache etcdClient, analytics analyticsClient) *Processor {
	return &Processor{executor: executor, cache: cache, analytics: analytics}
}

// Process the query (i.e. execute it), then post-process it, i.e. transform
// the structure we get from janusgraph into the structure we need for the
// graphQL API
func (p *Processor) Process(ctx context.Context, query *gremlin.Query, groupBy *common_filters.Path,
	params *kinds.AggregateParams) (interface{}, error) {

	result, err := p.getResult(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("could not process aggregate query: %v", err)
	}

	sliced, err := p.sliceResults(result, groupBy)
	if err != nil {
		return nil, fmt.Errorf(
			"got a successful response from janus, but could not slice the results: %s", err)
	}

	return sliced, nil
}

func (p *Processor) getResult(ctx context.Context, query *gremlin.Query, params *kinds.AggregateParams) ([]interface{}, error) {
	if params.Analytics.UseAnaltyicsEngine == false {
		result, err := p.executor.Execute(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("executing query against janusgraph failed: %s", err)
		}

		return datumsToSlice(result), nil
	}

	return p.getResultFromAnalyticsEngine(ctx, query, params)
}

func (p *Processor) sliceResults(input []interface{}, groupBy *common_filters.Path) ([]interface{}, error) {
	if len(input) != 1 {
		return nil, fmt.Errorf("expected exactly one Datum from janus, but got: %#v", input)
	}

	datumAsMap, ok := input[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected datum at pos 0 to be map, but was %#v", input[0])
	}

	result := []interface{}{}
	// we are expecting a structure like so (jsonified for better readibility),
	// assume for this example that we are aggregating by a boolean prop:
	// {
	//   "true": {
	//     "someIntProp": { "count": 17 },
	//     "anotherIntProp": { "count": 17 }
	//   },
	//   "false": {
	//     "someIntProp": { "count": 12 },
	//     "anotherIntProp": { "count": 12 }
	//   }
	// }
	for aggregatedValue, properties := range datumAsMap {
		propertiesMap, ok := properties.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("properties (i.e. value of aggregation map) must be a map, but got: %#v", properties)
		}

		propertiesMap, err := transformKeysOfInnerMap(propertiesMap)
		if !ok {
			return nil, fmt.Errorf("could not transform keys of nested property map: %s", err)
		}

		groupedMap := map[string]interface{}{
			"groupedBy": map[string]interface{}{
				"value": aggregatedValue,
				"path":  groupBy.Slice(),
			},
		}
		merged := mergeMaps(propertiesMap, groupedMap)
		result = append(result, merged)
	}
	return result, nil
}

func transformKeysOfInnerMap(outer map[string]interface{}) (map[string]interface{}, error) {
	for outerKey, aggregation := range outer {
		aggregationMap, ok := aggregation.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf(
				"inner map must be a map of type {aggregationFn: aggregateValue}, but got: %#v", aggregation)
		}

		newAggregationMap := map[string]interface{}{}
		for aggregator, value := range aggregationMap {
			transformedName, err := transformAggregatorName(aggregator)
			if err != nil {
				return nil, fmt.Errorf("could not transform aggregator '%s': %s", aggregator, err)
			}

			newAggregationMap[transformedName] = value
		}

		outer[outerKey] = newAggregationMap
	}

	return outer, nil
}

func transformAggregatorName(name string) (string, error) {
	parts := strings.Split(name, "__")
	if len(parts) != 2 {
		return "", fmt.Errorf(
			"aggregator name does not follow the expected format <prop_id>__<aggregator_name>")
	}

	return parts[1], nil
}

// m2 takes precedence in case of a conflict
func mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1[k] = v
	}

	return m1
}

func keyFromHash(hash string) string {
	return fmt.Sprintf("%s%s", AnalyticsAPICachePrefix, hash)
}

func datumsToSlice(g *gremlin.Response) []interface{} {
	res := make([]interface{}, len(g.Data), len(g.Data))
	for i, datum := range g.Data {
		res[i] = datum.Datum
	}

	return res
}
