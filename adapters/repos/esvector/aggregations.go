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
	if params.GroupBy != nil && len(params.GroupBy.Slice()) > 1 {
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

	var path []string
	if params.GroupBy != nil {
		path = params.GroupBy.Slice()
	}
	return r.aggregationResponse(res, path)
}

func aggBody(params traverser.AggregateParams) (map[string]interface{}, error) {
	inner, err := innerAggs(params.Properties)
	if err != nil {
		return nil, err
	}

	var aggregations map[string]interface{}
	if params.GroupBy == nil {
		aggregations = inner
	} else {
		aggregations = map[string]interface{}{
			"outer": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": params.GroupBy.Property,
					"size":  100,
				},
				"aggs": inner,
			},
		}
	}

	return map[string]interface{}{
		"aggs": aggregations,
		"size": 0,
	}, nil
}

func innerAggs(properties []traverser.AggregateProperty) (map[string]interface{}, error) {
	inner := map[string]interface{}{}
	for _, property := range properties {

		if containsBooleanAggregators(property.Aggregators) {
			// this is a special case as, we only need to do a single aggregation no
			// matter if one or all boolean aggregators are set, therefore we're not
			// iterating over all aggregators, but merely checking for their presence
			inner[aggName(property.Name, "boolean")] = aggValueBoolean(property.Name)

			// additionally, we know that a boolean prop cannot contain any
			// non-boolean aggregators, so it's safe to consider this property
			// complete
			continue
		}

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

func containsBooleanAggregators(aggs []traverser.Aggregator) bool {
	for _, agg := range aggs {
		if agg == traverser.PercentageTrueAggregator ||
			agg == traverser.PercentageFalseAggregator ||
			agg == traverser.TotalTrueAggregator ||
			agg == traverser.TotalFalseAggregator {
			return true
		}
	}

	return false
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
	switch agg {

	case traverser.ModeAggregator:
		return aggValueMode(prop), nil

	case traverser.MedianAggregator:
		return aggValueMedian(prop), nil

	case traverser.TopOccurrencesAggregator:
		return aggValueTopOccurrences(prop, 5), nil

	default:
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
}

func aggValueMode(prop schema.PropertyName) map[string]interface{} {
	return aggValueTopOccurrences(prop, 1)
}

func aggValueBoolean(prop schema.PropertyName) map[string]interface{} {
	return aggValueTopOccurrences(prop, 2)
}

func aggValueTopOccurrences(prop schema.PropertyName, size int) map[string]interface{} {
	return map[string]interface{}{
		"terms": map[string]interface{}{
			"field": prop,
			"size":  size,
		},
	}
}

func aggValueMedian(prop schema.PropertyName) map[string]interface{} {
	return map[string]interface{}{
		"percentiles": map[string]interface{}{
			"field":    prop,
			"percents": []int{50},
		},
	}
}
