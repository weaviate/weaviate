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
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// ThingSearch searches for all things with optional filters without vector scoring
func (r *Repo) ThingSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter) (search.Results, error) {
	return r.search(ctx, allThingIndices, nil, limit, filters, traverser.GetParams{})
}

// ActionSearch searches for all things with optional filters without vector scoring
func (r *Repo) ActionSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter) (search.Results, error) {
	return r.search(ctx, allActionIndices, nil, limit, filters, traverser.GetParams{})
}

// ThingByID extracts the one result matching the ID. Returns nil on no results
// (without errors), but errors if it finds more than 1 results
func (r *Repo) ThingByID(ctx context.Context, id strfmt.UUID,
	params traverser.SelectProperties) (*search.Result, error) {
	return r.searchByID(ctx, allThingIndices, id, params)
}

// ActionByID extracts the one result matching the ID. Returns nil on no results
// (without errors), but errors if it finds more than 1 results
func (r *Repo) ActionByID(ctx context.Context, id strfmt.UUID,
	params traverser.SelectProperties) (*search.Result, error) {
	return r.searchByID(ctx, allActionIndices, id, params)
}

func (r *Repo) byIndexAndID(ctx context.Context, index string, id strfmt.UUID,
	params traverser.SelectProperties) (*search.Result, error) {
	return r.searchByID(ctx, index, id, params)
}

func (r *Repo) searchByID(ctx context.Context, index string, id strfmt.UUID,
	properties traverser.SelectProperties) (*search.Result, error) {
	filters := &filters.LocalFilter{
		Root: &filters.Clause{
			On:       &filters.Path{Property: schema.PropertyName(keyID)},
			Value:    &filters.Value{Value: id},
			Operator: filters.OperatorEqual,
		},
	}
	res, err := r.search(ctx, index, nil, 2, filters, traverser.GetParams{Properties: properties})
	if err != nil {
		return nil, err
	}

	switch len(res) {
	case 0:
		return nil, nil
	case 1:
		return &res[0], nil
	default:
		return nil, fmt.Errorf("invalid number of results (%d) for id '%s'", len(res), id)
	}
}

// ClassSearch searches for classes with optional filters without vector scoring
func (r *Repo) ClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	index := classIndexFromClassName(params.Kind, params.ClassName)
	return r.search(ctx, index, nil, params.Pagination.Limit, params.Filters, params)
}

// VectorClassSearch limits the vector search to a specific class (and kind)
func (r *Repo) VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	index := classIndexFromClassName(params.Kind, params.ClassName)
	return r.search(ctx, index, params.SearchVector, params.Pagination.Limit, params.Filters, params)
}

// VectorSearch retrives the closest concepts by vector distance
func (r *Repo) VectorSearch(ctx context.Context, vector []float32,
	limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	return r.search(ctx, "*", vector, limit, filters, traverser.GetParams{})
}

func (r *Repo) search(ctx context.Context, index string,
	vector []float32, limit int,
	filters *filters.LocalFilter, params traverser.GetParams) ([]search.Result, error) {

	r.logger.
		WithField("action", "esvector_search").
		WithField("index", index).
		WithField("vector", vector).
		WithField("filters", filters).
		WithField("params", params).
		Debug("starting search to esvector")

	r.requestCounter.Inc()

	var buf bytes.Buffer

	query, err := queryFromFilter(filters)
	if err != nil {
		return nil, err
	}

	body := r.buildSearchBody(query, vector, limit)

	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("vector search: encode json: %v", err)
	}

	res, err := r.client.Search(
		r.client.Search.WithContext(ctx),
		r.client.Search.WithIndex(index),
		r.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	return r.searchResponse(res, params.Properties)
}

func (r *Repo) buildSearchBody(filterQuery map[string]interface{}, vector []float32, limit int) map[string]interface{} {
	var query map[string]interface{}

	if vector == nil {
		query = filterQuery
	} else {
		query = map[string]interface{}{
			"function_score": map[string]interface{}{
				"query":      filterQuery,
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
	}

	return map[string]interface{}{
		"query": query,
		"size":  limit,
	}
}

type searchResponse struct {
	Hits struct {
		Hits []hit `json:"hits"`
	} `json:"hits"`
	Aggreagtions aggregations `json:"aggregations"`
}

type aggregations map[string]singleAggregation

type singleAggregation struct {
	Buckets []map[string]interface{} `json:"buckets"`
}

type hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
	Score  float32                `json:"_score"`
	Index  string                 `json:"_index"`
}

func (r *Repo) searchResponse(res *esapi.Response, properties traverser.SelectProperties) ([]search.Result,
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

	return sr.toResults(r, properties)
}

func (sr searchResponse) toResults(r *Repo, properties traverser.SelectProperties) ([]search.Result, error) {
	hits := sr.Hits.Hits
	output := make([]search.Result, len(hits), len(hits))
	for i, hit := range hits {
		k, err := kind.Parse(hit.Source[keyKind.String()].(string))
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		vector, err := base64ToVector(hit.Source[keyVector.String()].(string))
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		cache := r.extractCache(hit.Source)
		schema, err := r.parseSchema(hit.Source, properties, cache, 0)
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		created := parseFloat64(hit.Source, keyCreated.String())
		updated := parseFloat64(hit.Source, keyUpdated.String())
		cacheHot := parseCacheHot(hit.Source)

		output[i] = search.Result{
			ClassName:   hit.Source[keyClassName.String()].(string),
			ID:          strfmt.UUID(hit.ID),
			Kind:        k,
			Score:       hit.Score,
			Vector:      vector,
			Schema:      schema,
			Created:     int64(created),
			Updated:     int64(updated),
			CacheHot:    cacheHot,
			CacheSchema: cache.schema,
		}
	}

	return output, nil
}

func parseFloat64(source map[string]interface{}, key string) float64 {
	untyped := source[key]
	if v, ok := untyped.(float64); ok {
		return v
	}

	return 0
}

func parseCacheHot(source map[string]interface{}) bool {
	untyped := source[keyCache.String()]
	m, ok := untyped.(map[string]interface{})
	if !ok {
		return false
	}

	hotUntyped, ok := m[keyCacheHot.String()]
	if !ok {
		return false
	}

	if v, ok := hotUntyped.(bool); ok {
		return v
	}

	return false
}
