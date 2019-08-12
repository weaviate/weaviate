//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
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
)

// ThingSearch searches for all things with optional filters without vector scoring
func (r *Repo) ThingSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter) (search.Results, error) {
	return r.search(ctx, allThingIndices, nil, limit, filters)
}

// ActionSearch searches for all things with optional filters without vector scoring
func (r *Repo) ActionSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter) (search.Results, error) {
	return r.search(ctx, allActionIndices, nil, limit, filters)
}

// ThingByID extracts the one result matching the ID. Returns nil on no results
// (without errors), but errors if it finds more than 1 results
func (r *Repo) ThingByID(ctx context.Context, id strfmt.UUID) (*search.Result, error) {
	return r.searchByID(ctx, allThingIndices, id)
}

// ActionByID extracts the one result matching the ID. Returns nil on no results
// (without errors), but errors if it finds more than 1 results
func (r *Repo) ActionByID(ctx context.Context, id strfmt.UUID) (*search.Result, error) {
	return r.searchByID(ctx, allActionIndices, id)
}

func (r *Repo) searchByID(ctx context.Context, index string, id strfmt.UUID) (*search.Result, error) {
	filters := &filters.LocalFilter{
		Root: &filters.Clause{
			On:       &filters.Path{Property: schema.PropertyName(keyID)},
			Value:    &filters.Value{Value: id},
			Operator: filters.OperatorEqual,
		},
	}
	res, err := r.search(ctx, index, nil, 2, filters)
	if err != nil {
		return nil, err
	}

	switch len(res) {
	case 0:
		return nil, nil
	case 1:
		return &res[0], nil
	default:
		return nil, fmt.Errorf("invalid number of results (%d) for if '%s'", len(res), id)
	}
}

// ClassSearch searches for classes with optional filters without vector scoring
func (r *Repo) ClassSearch(ctx context.Context, kind kind.Kind,
	className string, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	index := classIndexFromClassName(kind, className)
	return r.search(ctx, index, nil, limit, filters)
}

// VectorClassSearch limits the vector search to a specific class (and kind)
func (r *Repo) VectorClassSearch(ctx context.Context, kind kind.Kind,
	className string, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	index := classIndexFromClassName(kind, className)
	return r.search(ctx, index, vector, limit, filters)
}

// VectorSearch retrives the closest concepts by vector distance
func (r *Repo) VectorSearch(ctx context.Context, vector []float32,
	limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	return r.search(ctx, "*", vector, limit, filters)
}

func (r *Repo) search(ctx context.Context, index string,
	vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
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

	return r.searchResponse(res)
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
}

type hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
	Score  float32                `json:"_score"`
}

func (r *Repo) searchResponse(res *esapi.Response) ([]search.Result,
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

	return sr.toResults()
}

func (sr searchResponse) toResults() ([]search.Result, error) {
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

		schema, err := parseSchema(hit.Source)
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		created := hit.Source[keyCreated.String()].(float64)
		updated := hit.Source[keyUpdated.String()].(float64)

		output[i] = search.Result{
			ClassName: hit.Source[keyClassName.String()].(string),
			ID:        strfmt.UUID(hit.ID),
			Kind:      k,
			Score:     hit.Score,
			Vector:    vector,
			Schema:    schema,
			Created:   int64(created),
			Updated:   int64(updated),
		}
	}

	return output, nil
}
