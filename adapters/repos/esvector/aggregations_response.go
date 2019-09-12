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
	if len(path) == 0 {
		// no grouping
		return sr.ungroupedAggregations(sr.Aggregations)
	} else {
		// grouping
		rawBuckets := sr.Aggregations["outer"].(map[string]interface{})["buckets"].([]interface{})
		return sr.groupedAggregations(rawBuckets, path)
	}
}

func (sr searchResponse) ungroupedAggregations(aggs map[string]interface{}) (*aggregation.Result, error) {
	buckets, err := parseAggBucketsPayload(aggs, nil, nil)
	if err != nil {
		return nil, err
	}

	return bucketMapToSlice(buckets).result(nil)
}

func (sr searchResponse) groupedAggregations(rawBuckets []interface{}, path []string) (*aggregation.Result, error) {
	var buckets aggregationBuckets
	for _, bucket := range rawBuckets {
		bs, err := sr.parseAggBuckets(bucket.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, bs...)
	}

	return buckets.result(path)

}

type aggregationBucket struct {
	groupedValue          interface{}
	property              string
	numericalAggregations []aggregatorAndValue
	booleanAggregation    aggregation.Boolean
	count                 int
}

type aggregatorAndValue struct {
	aggregator string
	value      interface{}
}

type aggregationBuckets []aggregationBucket

func (sr searchResponse) parseAggBuckets(input map[string]interface{}) (aggregationBuckets, error) {

	groupedValue := input["key"]
	if groupedValue == nil {
		groupedValue = input["key_as_string"]
	}
	count := int(input["doc_count"].(float64))

	buckets, err := parseAggBucketsPayload(input, groupedValue, &count)
	if err != nil {
		return nil, err
	}

	return bucketMapToSlice(buckets), nil
}

// only the aggregators themselves, without the control fields, so it can be
// used for both grouped and ungrouped aggregations where the control fields
// differ
func parseAggBucketsPayload(input map[string]interface{}, groupedValue interface{},
	outsideCount *int) (map[string]aggregationBucket, error) {
	buckets := map[string]aggregationBucket{}
	for key, value := range input {
		switch key {
		case "key", "key_as_string", "doc_count":
			continue
		default:
			property, aggregator, err := parseAggBucketPropertyKey(key)
			if err != nil {
				return nil, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket := getOrInitBucket(buckets, property, groupedValue, outsideCount)
			if aggregator == "boolean" {
				err = addBooleanAggregationsToBucket(&bucket, value, outsideCount)
			} else {
				// numerical
				err = addNumericalAggregationsToBucket(&bucket, aggregator, value, outsideCount)
			}
			if err != nil {
				return nil, fmt.Errorf("key '%s': %v", key, err)
			}

			buckets[property] = bucket
		}
	}

	return buckets, nil
}

func getOrInitBucket(buckets map[string]aggregationBucket, property string,
	groupedValue interface{}, outsideCount *int) aggregationBucket {

	var count int
	if outsideCount != nil {
		count = *outsideCount
	}

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

func addNumericalAggregationsToBucket(bucket *aggregationBucket, aggregator string,
	value interface{}, outsideCount *int) error {
	av, err := extractAggregatorAndValue(aggregator, value)
	if err != nil {
		return err
	}

	if outsideCount == nil && aggregator == string(traverser.CountAggregator) {
		// this would be the case in an ungrouped aggregation, we need to
		// extract the count manually
		bucket.count = int(av.value.(float64))
	}
	bucket.numericalAggregations = append(bucket.numericalAggregations, av)

	return nil
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

func addBooleanAggregationsToBucket(bucket *aggregationBucket, value interface{}, outsideCount *int) error {
	// boolean is a special case, as we need to create four different
	// aggregator keys based on a single es aggregation
	agg, err := extractBooleanAggregation(value)
	if err != nil {
		return err
	}

	bucket.booleanAggregation = agg
	if outsideCount == nil {
		// this would be the case in an ungrouped aggregation, we need to
		// extract the count manually
		bucket.count = agg.Count
	}

	return nil
}

func extractBooleanAggregation(value interface{}) (aggregation.Boolean, error) {

	return parseAggBucketPropertyValueAsBoolean(value)
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
		return nil, fmt.Errorf("mode: expected map to have key 'buckets', but got %v", asMap)
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

func parseAggBucketPropertyValueAsBoolean(input interface{}) (aggregation.Boolean, error) {
	var res aggregation.Boolean

	asMap, ok := input.(map[string]interface{})
	if !ok {
		return res, fmt.Errorf("boolean: expected value to be a map, but was %T", input)
	}

	buckets, ok := asMap["buckets"]
	if !ok {
		return res, fmt.Errorf("boolean: expected map to have key 'buckets', but got %v", asMap)
	}

	return parseBooleanTrueFalseBuckets(buckets.([]interface{}))
}

func parseBooleanTrueFalseBuckets(buckets []interface{}) (aggregation.Boolean, error) {
	var res aggregation.Boolean

	for i, bucket := range buckets {
		value, count, err := parseBooleanValueAndCount(bucket)
		if err != nil {
			return res, fmt.Errorf("boolean: inner bucket at pos %d: %v", i, err)
		}

		if value {
			res.TotalTrue += count
		} else {
			res.TotalFalse += count
		}
		res.Count += count
	}

	return calculatePercentages(res), nil
}

func parseBooleanValueAndCount(bucket interface{}) (bool, int, error) {
	asMap, ok := bucket.(map[string]interface{})
	if !ok {
		return false, 0, fmt.Errorf("expected map, but got %T", bucket)
	}

	value, err := parseBooleanValueKey(asMap)
	if err != nil {
		return false, 0, err
	}

	count, err := parseBooleanValueCount(asMap)
	if err != nil {
		return false, 0, err
	}

	return value, count, nil
}

func parseBooleanValueKey(bucket map[string]interface{}) (bool, error) {
	keyAsString, ok := bucket["key_as_string"]
	if !ok {
		return false, fmt.Errorf("expected to find key 'key_as_string', but got %#v", bucket)
	}

	switch keyAsString {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf(
			"unexpected 'key_as_string', expected either \"true\" or \"false\", but got: %v", keyAsString)
	}
}

func parseBooleanValueCount(bucket map[string]interface{}) (int, error) {
	docCount, ok := bucket["doc_count"]
	if !ok {
		return 0, fmt.Errorf("expected to find key 'doc_count', but got %#v", bucket)
	}

	return int(docCount.(float64)), nil
}

func calculatePercentages(agg aggregation.Boolean) aggregation.Boolean {
	if agg.Count == 0 {
		return agg
	}

	agg.PercentageTrue = roundDecimals(float64(agg.TotalTrue) / float64(agg.Count))
	agg.PercentageFalse = roundDecimals(float64(agg.TotalFalse) / float64(agg.Count))

	return agg
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
		numerical, err := bucket.numerical()
		if err != nil {
			return nil, err
		}

		_, ok := groups[bucket.groupedValue]
		if !ok {
			var groupedBy *aggregation.GroupedBy

			if path != nil {
				// a nil path indicates an ungrouped aggregation
				groupedBy = &aggregation.GroupedBy{
					Path:  path,
					Value: bucket.groupedValue,
				}
			}

			groups[bucket.groupedValue] = aggregation.Group{
				Count:     bucket.count,
				GroupedBy: groupedBy,
				Properties: map[string]aggregation.Property{
					bucket.property: aggregation.Property{
						NumericalAggregations: numerical,
						BooleanAggregation:    bucket.booleanAggregation,
					},
				},
			}
		} else {
			groups[bucket.groupedValue].Properties[bucket.property] = aggregation.Property{
				NumericalAggregations: numerical,
				BooleanAggregation:    bucket.booleanAggregation,
			}
		}
	}

	return groupsMapToSlice(groups), nil
}

func (b aggregationBucket) numerical() (map[string]float64, error) {
	res := map[string]float64{}

	for _, agg := range b.numericalAggregations {
		res[agg.aggregator] = roundDecimals(agg.value.(float64))
	}

	if len(res) == 0 {
		return nil, nil
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
