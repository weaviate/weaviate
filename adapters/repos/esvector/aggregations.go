package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

var aggBucketKeyRegexp *regexp.Regexp

func init() {
	aggBucketKeyRegexp = regexp.MustCompile(`^agg\.[a-zA-z]+\.[a-zA-z]+$`)

}

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

	path := interfaceToStringSlice(params.GroupBy.Slice())
	return r.aggregationResponse(res, path)
}

//
// building the body
//

func aggBody(params traverser.AggregateParams) (map[string]interface{}, error) {
	inner, err := innerAggs(params.Properties)
	if err != nil {
		return nil, err
	}

	aggregations := map[string]interface{}{
		"outer": map[string]interface{}{
			"terms": map[string]interface{}{
				"field": params.GroupBy.Property,
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

//
// parsing the results
//

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

			var parsed interface{}

			if aggregator == string(traverser.ModeAggregator) {
				v, err := parseAggBucketPropertyValueAsMode(value)
				if err != nil {
					return bucket, fmt.Errorf("key '%s': %v", key, err)
				}

				parsed = v
			} else {
				v, err := parseAggBucketPropertyValue(value)
				if err != nil {
					return bucket, fmt.Errorf("key '%s' (mode): %v", key, err)
				}

				parsed = v
			}

			av := aggregatorAndValue{
				aggregator: aggregator,
				value:      parsed,
			}
			bucket.aggregations = append(bucket.aggregations, av)
		}
	}

	return bucket, nil
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
		return nil, fmt.Errorf("expected value to be a map, but was %T", input)
	}

	b, ok := asMap["buckets"]
	if !ok {
		return nil, fmt.Errorf("expected map to have key 'value', but got %v", asMap)
	}

	buckets := b.([]interface{})

	if len(buckets) != 1 {
		return nil, fmt.Errorf("expected inner buckets of mode query to have len 1, but got %d", len(buckets))
	}

	bucket := buckets[0].(map[string]interface{})

	v, ok := bucket["key"]
	if !ok {
		return nil, fmt.Errorf("expected inner mode bucket to have key 'key', but got %v", bucket)
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
		res[agg.aggregator] = agg.value.(float64)
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
