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

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (r *Repo) GetUnclassified(ctx context.Context, kind kind.Kind,
	class string, properties []string) ([]search.Result, error) {

	mustNot := []map[string]interface{}{}
	for _, prop := range properties {
		mustNot = append(mustNot, map[string]interface{}{
			"exists": map[string]interface{}{
				"field": prop,
			},
		})

	}

	body := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": mustNot,
			},
		},
		"size": 9999,
		"aggregations": map[string]interface{}{
			"count": map[string]interface{}{
				"value_count": map[string]interface{}{
					"field": "_id",
				},
			},
		},
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("vector search: encode json: %v", err)
	}
	res, err := r.client.Search(
		r.client.Search.WithContext(ctx),
		r.client.Search.WithIndex(classIndexFromClassName(kind, class)),
		r.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	return r.unclassifiedSearchResponse(res, nil)
}

func (r *Repo) unclassifiedSearchResponse(res *esapi.Response, properties traverser.SelectProperties) ([]search.Result,
	error) {
	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	var sr searchResponse
	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("vector search: decode json: %v", err)
	}

	if err := checkClassificationCount(sr.Aggregations); err != nil {
		return nil, err
	}

	return sr.toResults(r, properties)
}

func checkClassificationCount(res map[string]interface{}) error {

	count, ok := res["count"]
	if !ok {
		return fmt.Errorf("get unclassified: expected 'count' aggregation, but got %v", res)
	}

	asMap, ok := count.(map[string]interface{})
	if !ok {
		return fmt.Errorf("get unclassified: expected 'count' to be map, got %T", count)
	}

	value, ok := asMap["value"]
	if !ok {
		return fmt.Errorf("get unclassified: expected 'count' to have key 'value', but got %v", count)
	}

	if int(value.(float64)) > 9999 {
		return fmt.Errorf("found more than 9999 unclassified items (%d), current supported maximum is 9999", int(value.(float64)))
	}

	return nil
}

func (r *Repo) AggregateNeighbors(ctx context.Context, vector []float32, kind kind.Kind, class string,
	properties []string, k int) ([]classification.NeighborRef, error) {

	propertyAggregations := map[string]interface{}{}
	for _, prop := range properties {
		propertyAggregations[prop] = map[string]interface{}{
			"terms": map[string]interface{}{
				"size":  1,
				"field": fmt.Sprintf("%s.beacon", prop),
			},
		}
	}

	aggregations := map[string]interface{}{
		"sample": map[string]interface{}{
			"sampler": map[string]interface{}{
				"shard_size": k,
			},
			"aggregations": propertyAggregations,
		},
	}

	query := map[string]interface{}{
		"function_score": map[string]interface{}{
			"boost_mode": "replace",
			"functions": []interface{}{
				map[string]interface{}{
					"script_score": map[string]interface{}{
						"script": map[string]interface{}{
							"inline": "binary_vector_score",
							"lang":   "knn",
							"params": map[string]interface{}{
								"cosine": false,
								"field":  keyVector,
								"vector": vector,
							},
						},
					},
				},
			},
		},
	}

	body := map[string]interface{}{
		"query":        query,
		"aggregations": aggregations,
		"size":         0,
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("vector search: encode json: %v", err)
	}

	res, err := r.client.Search(
		r.client.Search.WithContext(ctx),
		r.client.Search.WithIndex(classIndexFromClassName(kind, class)),
		r.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	return r.aggregateNeighborsResponse(res)
}

func (r *Repo) aggregateNeighborsResponse(res *esapi.Response) ([]classification.NeighborRef, error) {
	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("neighbor aggregation: %v", err)
	}

	var sr searchResponse

	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("neighbor aggregation: decode json: %v", err)
	}

	out, err := r.aggregationsToClassificationNeighborRefs(sr)
	if err != nil {
		return nil, fmt.Errorf("aggregate neighbors: %v", err)
	}
	return out, nil
}

func (r *Repo) aggregationsToClassificationNeighborRefs(input searchResponse) ([]classification.NeighborRef, error) {
	sample, ok := input.Aggregations["sample"]
	if !ok {
		return nil, fmt.Errorf("expected aggregation response to contain agg 'sample'")
	}

	asMap, ok := sample.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected 'sample' to be map, got %T", sample)
	}

	var out []classification.NeighborRef
	for key, value := range asMap {
		if key == "doc_count" {
			continue
		}

		inner, err := extractInnerNeighborAgg(key, value)
		if err != nil {
			return nil, fmt.Errorf("for prop %s: %v", key, err)
		}

		out = append(out, inner)
	}

	return out, nil
}

func extractInnerNeighborAgg(prop string, agg interface{}) (classification.NeighborRef, error) {
	var out classification.NeighborRef
	asMap, ok := agg.(map[string]interface{})
	if !ok {
		return out, fmt.Errorf("expected inner agg to be map, got %T", agg)
	}

	buckets, ok := asMap["buckets"]
	if !ok {
		return out, fmt.Errorf("expected key 'buckets', got %v", asMap)
	}

	bucketsSlice, ok := buckets.([]interface{})
	if !ok {
		return out, fmt.Errorf("expected buckets to be a slice, got %T", buckets)
	}

	if len(bucketsSlice) != 1 {
		return out, fmt.Errorf("expected buckets to have len=1, got %#v", bucketsSlice)
	}

	bucketMap, ok := bucketsSlice[0].(map[string]interface{})
	if !ok {
		return out, fmt.Errorf("expected key 'buckets' to be map, got %T", bucketsSlice[0])
	}

	beacon, ok := bucketMap["key"]
	if !ok {
		return out, fmt.Errorf("expected bucket to have key 'key', got %v", bucketMap)
	}

	count, ok := bucketMap["doc_count"]
	if !ok {
		return out, fmt.Errorf("expected bucket to have key 'doc_count', got %v", bucketMap)
	}

	out.Beacon = strfmt.URI(beacon.(string))
	out.Count = int(count.(float64))
	out.Property = prop

	return out, nil
}
