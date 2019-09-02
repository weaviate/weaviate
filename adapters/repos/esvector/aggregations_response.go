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
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

var aggBucketKeyRegexp *regexp.Regexp

func init() {
	aggBucketKeyRegexp = regexp.MustCompile(`^agg\.[a-zA-z]+\.[a-zA-z]+$`)

}

func (r *Repo) aggregationResponse(res *esapi.Response, path []string) (*aggregation.Result, error) {
	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("aggregation: %v", err)
	}

	var sr searchResponse

	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("aggregation: decode json: %v", err)
	}

	parsed, err := sr.aggregations(path)
	if err != nil {
		return nil, fmt.Errorf("aggregation: %v", err)
	}

	return parsed, nil
}

func (sr searchResponse) aggregations(path []string) (*aggregation.Result, error) {

	rawBuckets := sr.Aggreagtions["outer"].Buckets
	var buckets aggregationBuckets
	for _, bucket := range rawBuckets {
		bs, err := sr.parseAggBuckets(bucket)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, bs...)
	}

	return buckets.result(path)
}

type aggregationBucket struct {
	groupedValue interface{}
	property     string
	aggregations []aggregatorAndValue
	count        int
}

type aggregatorAndValue struct {
	aggregator string
	value      interface{}
}

type aggregationBuckets []aggregationBucket

func (sr searchResponse) parseAggBuckets(input map[string]interface{}) (aggregationBuckets, error) {
	buckets := map[string]aggregationBucket{}

	groupedValue := input["key"]
	if groupedValue == nil {
		groupedValue = input["key_as_string"]
	}
	count := int(input["doc_count"].(float64))

	for key, value := range input {
		switch key {
		case "key", "key_as_string", "doc_count":
			continue
		default:
			property, aggregator, err := parseAggBucketPropertyKey(key)
			if err != nil {
				return nil, fmt.Errorf("key '%s': %v", key, err)
			}

			av, err := extractAggregatorAndValue(aggregator, value)
			if err != nil {
				return nil, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket := getOrInitBucket(buckets, property, groupedValue, count)
			bucket.aggregations = append(bucket.aggregations, av)
			buckets[property] = bucket
		}
	}

	return bucketMapToSlice(buckets), nil
}

func getOrInitBucket(buckets map[string]aggregationBucket, property string,
	groupedValue interface{}, count int) aggregationBucket {
	b, ok := buckets[property]
	if ok {
		return b
	}
	return aggregationBucket{
		groupedValue: groupedValue,
		count:        count,
		property:     property,
	}
}

func extractAggregatorAndValue(aggregator string, value interface{}) (aggregatorAndValue, error) {
	var (
		parsed interface{}
		err    error
	)

	switch aggregator {
	case string(traverser.ModeAggregator):
		parsed, err = parseAggBucketPropertyValueAsMode(value)
	case string(traverser.MedianAggregator):
		parsed, err = parseAggBucketPropertyValueAsMedian(value)
	default:
		parsed, err = parseAggBucketPropertyValue(value)
	}
	if err != nil {
		return aggregatorAndValue{}, err
	}

	return aggregatorAndValue{
		aggregator: aggregator,
		value:      parsed,
	}, nil
}

func parseAggBucketPropertyValue(input interface{}) (interface{}, error) {
	asMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected value to be a map, but was %T", input)
	}

	v, ok := asMap["value"]
	if !ok {
		return nil, fmt.Errorf("expected map to have key 'value', but got %v", asMap)
	}

	return v, nil
}

func parseAggBucketPropertyValueAsMode(input interface{}) (interface{}, error) {
	asMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("mode: expected value to be a map, but was %T", input)
	}

	b, ok := asMap["buckets"]
	if !ok {
		return nil, fmt.Errorf("mode: expected map to have key 'value', but got %v", asMap)
	}

	buckets := b.([]interface{})

	if len(buckets) != 1 {
		return nil, fmt.Errorf("mode: expected inner buckets of mode query to have len 1, but got %d", len(buckets))
	}

	bucket := buckets[0].(map[string]interface{})

	v, ok := bucket["key"]
	if !ok {
		return nil, fmt.Errorf("mode: expected inner mode bucket to have key 'key', but got %v", bucket)
	}

	return v, nil
}

func parseAggBucketPropertyValueAsMedian(input interface{}) (interface{}, error) {
	asMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("median: expected value to be a map, but was %T", input)
	}

	values, ok := asMap["values"]
	if !ok {
		return nil, fmt.Errorf("median: expected map to have key 'value', but got %v", asMap)
	}

	valuesMap, ok := values.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("median: expected key 'value' to be a map, but got %T", values)
	}

	v, ok := valuesMap["50.0"]
	if !ok {
		return nil, fmt.Errorf("median: expected percentages map to have key '50.0', but got %v", valuesMap)
	}

	return v, nil
}

func parseAggBucketPropertyKey(input string) (string, string, error) {
	ok := aggBucketKeyRegexp.MatchString(input)
	if !ok {
		return "", "", fmt.Errorf("bucket key does not match expected pattern, got: %v", input)
	}

	parts := strings.Split(input, ".")
	return parts[1], parts[2], nil
}

func (b aggregationBuckets) result(path []string) (*aggregation.Result, error) {
	groups, err := b.groups(path)
	if err != nil {
		return nil, err
	}

	return &aggregation.Result{
		Groups: groups,
	}, nil
}

func (b aggregationBuckets) groups(path []string) ([]aggregation.Group, error) {

	groups := map[interface{}]aggregation.Group{}
	for _, bucket := range b {
		aggs, err := bucket.numericalAggregations()
		if err != nil {
			return nil, err
		}

		_, ok := groups[bucket.groupedValue]
		if !ok {
			groups[bucket.groupedValue] = aggregation.Group{
				Count: bucket.count,
				GroupedBy: aggregation.GroupedBy{
					Path:  path,
					Value: bucket.groupedValue,
				},
				Properties: map[string]aggregation.Property{
					bucket.property: aggregation.Property{
						NumericalAggregations: aggs,
					},
				},
			}
		} else {
			groups[bucket.groupedValue].Properties[bucket.property] = aggregation.Property{
				NumericalAggregations: aggs,
			}
		}

	}

	return groupsMapToSlice(groups), nil
}

func (b aggregationBucket) numericalAggregations() (map[string]float64, error) {
	res := map[string]float64{}

	for _, agg := range b.aggregations {
		res[agg.aggregator] = roundDecimals(agg.value.(float64))
	}

	return res, nil
}

func interfaceToStringSlice(input []interface{}) []string {
	output := make([]string, len(input), len(input))
	for i, value := range input {
		output[i] = value.(string)
	}

	return output
}

func groupsMapToSlice(groups map[interface{}]aggregation.Group) []aggregation.Group {
	output := make([]aggregation.Group, len(groups), len(groups))

	i := 0
	for _, group := range groups {
		output[i] = group
		i++
	}

	sort.Slice(output, func(i, j int) bool {
		if output[i].Count != output[j].Count {
			// first priority by count
			return output[i].Count > output[j].Count
		} else {
			// on equal count try using group by property
			iValue := output[i].GroupedBy.Value
			jValue := output[j].GroupedBy.Value

			switch left := iValue.(type) {
			case float64:
				return left > jValue.(float64)
			case string:
				return left > jValue.(string)
			default:
				// can't sort, just return something
				return false
			}
		}
	})

	return output
}

func bucketMapToSlice(buckets map[string]aggregationBucket) aggregationBuckets {
	output := make(aggregationBuckets, len(buckets), len(buckets))

	i := 0
	for _, b := range buckets {
		output[i] = b
		i++
	}

	sort.Slice(output, func(i, j int) bool {
		return output[i].count < output[j].count
	})

	return output
}

func roundDecimals(in float64) float64 {
	decimals := 5.0
	multiplier := math.Pow(10.0, decimals)

	return math.Round(in*multiplier) / multiplier
}
