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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
)

func (r *Repo) GetUnclassified(ctx context.Context, kind kind.Kind,
	class string, properties []string, filter *filters.LocalFilter) ([]search.Result, error) {

	mustNot := []map[string]interface{}{}
	for _, prop := range properties {
		mustNot = append(mustNot, map[string]interface{}{
			"exists": map[string]interface{}{
				"field": prop,
			},
		})
	}

	var query map[string]interface{}

	if filter == nil {
		query = map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": mustNot,
			},
		}
	} else {
		subquery, err := queryFromFilter(filter)
		if err != nil {
			return nil, fmt.Errorf("build filter: %v", err)
		}
		query = map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					subquery,
					map[string]interface{}{
						"bool": map[string]interface{}{
							"must_not": mustNot,
						},
					},
				},
			},
		}
	}

	fmt.Printf("\n\n\n")
	j, _ := json.MarshalIndent(query, "", "  ")
	fmt.Printf("%s", string(j))
	fmt.Printf("\n\n\n")

	body := map[string]interface{}{
		"query": query,
		"size":  9999,
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

	return sr.toResults(r, properties, false)
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

	mustExist := []map[string]interface{}{}
	var propNames []string
	for _, prop := range properties {
		propNames = append(propNames, prop)
		mustExist = append(mustExist, map[string]interface{}{
			"exists": map[string]interface{}{
				"field": prop,
			},
		})
	}

	query := map[string]interface{}{
		"function_score": map[string]interface{}{
			"boost_mode": "replace",
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": mustExist,
				},
			},
			"functions": []interface{}{
				map[string]interface{}{
					"script_score": map[string]interface{}{
						"script": map[string]interface{}{
							"inline": "binary_vector_score",
							"lang":   "knn",
							"params": map[string]interface{}{
								"cosine": true,
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
		"query":   query,
		"size":    k,
		"_source": append(propNames, keyVector.String()),
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

	return r.aggregateNeighborsResponse(res, vector)
}

func (r *Repo) aggregateNeighborsResponse(res *esapi.Response,
	sourceVector []float32) ([]classification.NeighborRef, error) {
	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("neighbor aggregation: %v", err)
	}

	var sr searchResponse

	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("neighbor aggregation: decode json: %v", err)
	}

	out, err := r.aggregationsToClassificationNeighborRefs(sr, sourceVector)
	if err != nil {
		return nil, fmt.Errorf("aggregate neighbors: %v", err)
	}
	return out, nil
}

func (r *Repo) aggregationsToClassificationNeighborRefs(input searchResponse,
	sourceVector []float32) ([]classification.NeighborRef, error) {
	hits := input.Hits.Hits

	aggregations, err := extractRefNeighborsFromHits(hits, sourceVector)
	if err != nil {
		return nil, err
	}

	return aggregateRefNeighbors(aggregations)
}

func aggregateRefNeighbors(props map[string]map[string][]float32) ([]classification.NeighborRef, error) {
	var out []classification.NeighborRef
	for prop, beacons := range props {
		var winningBeacon string
		var winningCount int

		for beacon, distances := range beacons {
			if len(distances) > winningCount {
				winningBeacon = beacon
				winningCount = len(distances)
			}
		}

		winning, losing := extractWinningAndLoosingDistances(beacons, winningBeacon)

		out = append(out, classification.NeighborRef{
			Beacon:          strfmt.URI(winningBeacon),
			Count:           winningCount,
			Property:        prop,
			WinningDistance: winning,
			LosingDistance:  losing,
		})

	}

	return out, nil
}

func extractRefNeighborsFromHits(hits []hit,
	sourceVector []float32) (map[string]map[string][]float32, error) {
	// structure is [prop][beacon][[]distance]
	aggregations := map[string]map[string][]float32{}

	for _, hit := range hits {
		v, err := extractVectorFromHit(hit)
		if err != nil {
			return nil, err
		}

		dist, err := vectorizer.NormalizedDistance(sourceVector, v)
		if err != nil {
			return nil, err
		}

		for key, value := range hit.Source {
			if key == keyVector.String() {
				continue
			}

			// assume is a ref
			prop, ok := aggregations[key]
			if !ok {
				prop = map[string][]float32{}
			}

			beacon, err := extractBeaconFromProp(value)
			if err != nil {
				return nil, fmt.Errorf("prop %s: %v", key, err)
			}

			prop[beacon] = append(prop[beacon], dist)
			aggregations[key] = prop
		}
	}

	return aggregations, nil
}

func extractVectorFromHit(hit hit) ([]float32, error) {
	vector, ok := hit.Source[keyVector.String()]
	if !ok {
		return nil, fmt.Errorf("expected key %s, but got %v", keyVector, hit.Source)
	}

	return base64ToVector(vector.(string))
}

func extractBeaconFromProp(prop interface{}) (string, error) {
	propSlice, ok := prop.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected refs to be slice, got %T", prop)
	}

	if len(propSlice) != 1 {
		return "", fmt.Errorf("expected refs to have len 1, got %d", len(propSlice))
	}

	ref := propSlice[0]
	refMap, ok := ref.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected ref to be map, got %T", ref)
	}

	beacon, ok := refMap["beacon"]
	if !ok {
		return "", fmt.Errorf("expected ref (map) to have field 'beacon', got %v", refMap)
	}

	return beacon.(string), nil
}

func extractWinningAndLoosingDistances(beacons map[string][]float32, winner string) (float32, *float32) {

	var winningDistances []float32
	var losingDistances []float32

	for beacon, distances := range beacons {

		if beacon == winner {
			winningDistances = distances
		} else {
			losingDistances = append(losingDistances, distances...)
		}
	}

	var losingDistance *float32
	if len(losingDistances) > 0 {
		d := mean(losingDistances)
		losingDistance = &d
	}

	return mean(winningDistances), losingDistance
}

func mean(in []float32) float32 {
	sum := float32(0)
	for _, v := range in {
		sum += v
	}

	return sum / float32(len(in))
}
