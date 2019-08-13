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
	property := properties[0]

	inner := map[string]interface{}{}

	aggregator := property.Aggregators[0]

	v, err := aggValue(property.Name, aggregator)
	if err != nil {
		return nil, fmt.Errorf("prop '%s': %v", property.Name, err)
	}

	inner[aggName(property.Name, aggregator)] = v

	return inner, nil
}

func aggName(prop schema.PropertyName, agg traverser.Aggregator) string {
	return fmt.Sprintf("agg.%s.%s", prop, agg)
}

var aggTranslation = map[traverser.Aggregator]string{
	traverser.MeanAggregator: "avg",
}

func lookupAgg(input traverser.Aggregator) (string, error) {
	res, ok := aggTranslation[input]
	if !ok {
		return "", fmt.Errorf("aggregator '%s' not supported", input)
	}

	return res, nil
}

func aggValue(prop schema.PropertyName, agg traverser.Aggregator) (map[string]interface{}, error) {
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
	aggregator   string
	value        interface{}
	count        int
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
			p, a, err := parseAggBucketPropertyKey(key)
			if err != nil {
				return bucket, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket.property = p
			bucket.aggregator = a

			v, err := parseAggBucketPropertyValue(value)
			if err != nil {
				return bucket, fmt.Errorf("key '%s': %v", key, err)
			}

			bucket.value = v
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
	groups := make([]aggregation.Group, len(b), len(b))
	for i, bucket := range b {
		groups[i] = aggregation.Group{
			GroupedBy: aggregation.GroupedBy{
				Path:  path,
				Value: bucket.groupedValue,
			},
			Properties: map[string][]aggregation.Property{
				bucket.property: []aggregation.Property{
					aggregation.Numerical{
						Aggregator: bucket.aggregator,
						Value:      bucket.value.(float64),
					},
				},
			},
		}

	}

	return groups, nil
}

func interfaceToStringSlice(input []interface{}) []string {
	output := make([]string, len(input), len(input))
	for i, value := range input {
		output[i] = value.(string)
	}

	return output
}
