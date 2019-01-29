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
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Processor is a simple Gremlin-Query Executor that is specific to GetMeta in
// that it merges the results into the expected format and applies
// post-processing where necessary
type Processor struct {
	executor executor
}

type executor interface {
	Execute(query gremlin.Gremlin) (*gremlin.Response, error)
}

//NewProcessor of type Processor
func NewProcessor(executor executor) *Processor {
	return &Processor{executor: executor}
}

// Process the query (i.e. execute it), then post-process it, i.e. transform
// the structure we get from janusgraph into the structure we need for the
// graphQL API
func (p *Processor) Process(query *gremlin.Query, groupBy *common_filters.Path) (interface{}, error) {
	result, err := p.executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("could not process meta query: executing the query failed: %s", err)
	}

	sliced, err := p.sliceResults(result, groupBy)
	if err != nil {
		return nil, fmt.Errorf(
			"got a successful response from janus, but could not slice the results: %s", err)
	}

	return sliced, nil
}

func (p *Processor) sliceResults(input *gremlin.Response, groupBy *common_filters.Path) ([]interface{}, error) {
	if len(input.Data) != 1 {
		return nil, fmt.Errorf("expected exactly one Datum from janus, but got: %#v", input.Data)
	}

	datumAsMap, ok := input.Data[0].Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected datum at pos 0 to be map, but was %#v", input.Data[0].Datum)
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
