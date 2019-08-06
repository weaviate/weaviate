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

package meta

import (
	"context"
	"fmt"

	analytics "github.com/SeMI-network/janus-spark-analytics/clients/go"
	"github.com/coreos/etcd/clientv3"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

const AnalyticsAPICachePrefix = "/weaviate/janusgraph-connector/analytics-cache/"

// Processor is a simple Gremlin-Query Executor that is specific to Meta in
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

func NewProcessor(executor executor, cache etcdClient, analytics analyticsClient) *Processor {
	return &Processor{executor: executor, cache: cache, analytics: analytics}
}

func (p *Processor) Process(ctx context.Context, query *gremlin.Query, typeInfo map[string]interface{},
	params *traverser.MetaParams) (interface{}, error) {

	result, err := p.getResult(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("could not process meta query: %v", err)
	}

	merged, err := p.mergeResults(result, typeInfo, params)
	if err != nil {
		return nil, fmt.Errorf(
			"got a successful response from janus, but could not merge the results: %s", err)
	}

	return merged, nil
}

func (p *Processor) getResult(ctx context.Context, query *gremlin.Query, params *traverser.MetaParams) ([]interface{}, error) {
	if params.Analytics.UseAnaltyicsEngine == false {
		result, err := p.executor.Execute(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("executing query against janusgraph failed: %s", err)
		}

		return datumsToSlice(result), nil
	}

	return p.getResultFromAnalyticsEngine(ctx, query, params)
}

func (p *Processor) mergeResults(input []interface{},
	typeInfo map[string]interface{}, params *traverser.MetaParams) (interface{}, error) {
	result := map[string]interface{}{}

	for _, datum := range input {
		datumAsMap, ok := datum.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected datum to be map, but was %#v", datum)
		}

		// merge datums from janus
		for k, v := range datumAsMap {
			vAsMap, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected property to be map, but was %#v", v)
			}

			processed, err := p.postProcess(k, vAsMap, params)
			if err != nil {
				return nil, fmt.Errorf("could not postprocess '%#v': %s", v, err)
			}

			if _, ok := result[k]; !ok {
				// first time we see this prop
				result[k] = processed
			} else {
				// we had this prop before let's merge them
				result[k] = mergeMaps(result[k].(map[string]interface{}), processed)
			}
		}
	}

	// merge in type info
	for k, v := range typeInfo {
		if _, ok := result[k]; !ok {
			// first time we see this prop
			result[k] = v
		} else {
			typeMap, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("type property was not a map, but %t", v)

			}
			// we had this prop before let's merge them
			result[k] = mergeMaps(result[k].(map[string]interface{}), typeMap)
		}
	}

	return map[string]interface{}(result), nil
}

func (p *Processor) postProcess(propName string, m map[string]interface{},
	params *traverser.MetaParams) (map[string]interface{}, error) {

	if hasBoolAnalyses(propName, params) {
		return p.postProcessBoolGroupCount(m)
	}

	for k := range m {
		switch k {
		case StringTopOccurrences:
			return p.postProcessStringTopOccurrences(m)
		}
	}

	return m, nil
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

func hasBoolAnalyses(propName string, params *traverser.MetaParams) bool {
	for _, prop := range params.Properties {
		if string(prop.Name) == propName && hasAtLeastOneBooleanAnalysis(prop.StatisticalAnalyses) {
			return true
		}
	}

	return false
}

func hasAtLeastOneBooleanAnalysis(analyses []traverser.StatisticalAnalysis) bool {
	for _, analysis := range analyses {
		if analysis == traverser.PercentageTrue ||
			analysis == traverser.PercentageFalse ||
			analysis == traverser.TotalTrue ||
			analysis == traverser.TotalFalse {
			return true
		}
	}

	return false
}
