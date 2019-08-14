package esvector

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
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
	buckets := make(aggregationBuckets, len(rawBuckets), len(rawBuckets))
	for i, bucket := range rawBuckets {
		b, err := sr.parseAggBucket(bucket)
		if err != nil {
			return nil, err
		}
		buckets[i] = b
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

func (sr searchResponse) parseAggBucket(input map[string]interface{}) (aggregationBucket, error) {
	var (
		bucket aggregationBucket
	)

	for key, value := range input {
		switch key {
		case "key":
			bucket.groupedValue = value
		case "doc_count":
			bucket.count = int(value.(float64))
		default:
			property, aggregator, err := parseAggBucketPropertyKey(key)
			if err != nil {
				return bucket, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket.property = property

			av, err := extractAggregatorAndValue(aggregator, value)
			if err != nil {
				return bucket, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket.aggregations = append(bucket.aggregations, av)
		}
	}

	return bucket, nil
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
		// _, ok := groups[bucket.groupedValue]
		// if !ok {
		aggs, err := bucket.numericalAggregations()
		if err != nil {
			return nil, err
		}

		groups[bucket.groupedValue] = aggregation.Group{
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
		// else {
		// 	fmt.Printf("\n\n\n---- we are appending ---\n\n\n\n")
		// 	groups[bucket.groupedValue].Properties[bucket.property] = append(
		// 		groups[bucket.groupedValue].Properties[bucket.property],
		// 		aggregation.Numerical{
		// 			Aggregator: bucket.aggregator,
		// 			Value:      bucket.value.(float64),
		// 		})
		// }
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

	return output
}

func roundDecimals(in float64) float64 {
	decimals := 5.0
	multiplier := math.Pow(10.0, decimals)

	return math.Round(in*multiplier) / multiplier
}
