//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (r *Repo) Aggregate(ctx context.Context, params traverser.AggregateParams) (*aggregation.Result, error) {
	if len(params.GroupBy.Slice()) > 1 {
		return nil, fmt.Errorf("grouping by cross-refs not supported yet")
	}

	body, err := aggBody(params)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("vector search: encode json: %v", err)
	}

	res, err := r.client.Search(
		r.client.Search.WithContext(ctx),
		r.client.Search.WithIndex(classIndexFromClassName(params.Kind, params.ClassName.String())),
		r.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	path := params.GroupBy.Slice()
	return r.aggregationResponse(res, path)
}

func aggBody(params traverser.AggregateParams) (map[string]interface{}, error) {
	inner, err := innerAggs(params.Properties)
	if err != nil {
		return nil, err
	}

	aggregations := map[string]interface{}{
		"outer": map[string]interface{}{
			"terms": map[string]interface{}{
				"field": params.GroupBy.Property,
				"size":  100,
			},
			"aggs": inner,
		},
	}

	return map[string]interface{}{
		"aggs": aggregations,
		"size": 0,
	}, nil
}

func innerAggs(properties []traverser.AggregateProperty) (map[string]interface{}, error) {
	inner := map[string]interface{}{}
	for _, property := range properties {
		for _, aggregator := range property.Aggregators {
			v, err := aggValue(property.Name, aggregator)
			if err != nil {
				return nil, fmt.Errorf("prop '%s': %v", property.Name, err)
			}

			inner[aggName(property.Name, aggregator)] = v
		}
	}

	return inner, nil
}

func aggName(prop schema.PropertyName, agg traverser.Aggregator) string {
	return fmt.Sprintf("agg.%s.%s", prop, agg)
}

var aggTranslation = map[traverser.Aggregator]string{
	traverser.MeanAggregator:    "avg",
	traverser.MaximumAggregator: "max",
	traverser.MinimumAggregator: "min",
	traverser.SumAggregator:     "sum",
	traverser.CountAggregator:   "value_count",
}

func lookupAgg(input traverser.Aggregator) (string, error) {
	res, ok := aggTranslation[input]
	if !ok {
		return "", fmt.Errorf("aggregator '%s' not supported", input)
	}

	return res, nil
}

func aggValue(prop schema.PropertyName, agg traverser.Aggregator) (map[string]interface{}, error) {
	if agg == traverser.ModeAggregator {
		return aggValueMode(prop)
	}

	if agg == traverser.MedianAggregator {
		return aggValueMedian(prop)
	}

	esAgg, err := lookupAgg(agg)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		esAgg: map[string]interface{}{
			"field": prop,
		},
	}, nil
}

func aggValueMode(prop schema.PropertyName) (map[string]interface{}, error) {
	return map[string]interface{}{
		"terms": map[string]interface{}{
			"field": prop,
			"size":  1,
		},
	}, nil
}

func aggValueMedian(prop schema.PropertyName) (map[string]interface{}, error) {
	return map[string]interface{}{
		"percentiles": map[string]interface{}{
			"field":    prop,
			"percents": []int{50},
		},
	}, nil
}
