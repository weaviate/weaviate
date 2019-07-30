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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

const (
	vectorProp = "embedding_vector"
)

// Repo stores and retrieves vector info in elasticsearch
type Repo struct {
	client *elasticsearch.Client
	logger logrus.FieldLogger
}

// NewRepo from existing es client
func NewRepo(client *elasticsearch.Client, logger logrus.FieldLogger) *Repo {
	return &Repo{client, logger}
}

type conceptBucket struct {
	Kind            string `json:"kind"`
	ID              string `json:"id"`
	ClassName       string `json:"class_name"`
	EmbeddingVector string `json:"embedding_vector"`
}

// VectorClassSearch limits the vector search to a specific class (and kind)
func (r *Repo) VectorClassSearch(ctx context.Context, kind kind.Kind,
	className string, vector []float32, limit int,
	filters *filters.LocalFilter) ([]traverser.VectorSearchResult, error) {
	index := classIndexFromClassName(kind, className)
	return r.VectorSearch(ctx, index, vector, limit)
}

// VectorSearch retrives the closest concepts by vector distance
func (r *Repo) VectorSearch(ctx context.Context, index string,
	vector []float32, limit int) ([]traverser.VectorSearchResult, error) {
	var buf bytes.Buffer
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{
					"match_all": map[string]interface{}{},
				},
				"boost_mode": "replace",
				"script_score": map[string]interface{}{
					"script": map[string]interface{}{
						"inline": "binary_vector_score",
						"lang":   "knn",
						"params": map[string]interface{}{
							"cosine": false,
							"field":  "embedding_vector",
							"vector": vector,
						},
					},
				},
			},
		},
		// hard code limit to 100 for now
		"size": limit,
	}

	err := json.NewEncoder(&buf).Encode(body)
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

type searchResponse struct {
	Hits struct {
		Hits []hit `json:"hits"`
	} `json:"hits"`
}

type hit struct {
	ID     string        `json:"_id"`
	Source conceptBucket `json:"_source"`
	Score  float32       `json:"_score"`
}

func (r *Repo) searchResponse(res *esapi.Response) ([]traverser.VectorSearchResult,
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

	return sr.toVectorSearchResult()
}

func (sr searchResponse) toVectorSearchResult() ([]traverser.VectorSearchResult, error) {
	hits := sr.Hits.Hits
	output := make([]traverser.VectorSearchResult, len(hits), len(hits))
	for i, hit := range hits {
		k, err := kind.Parse(hit.Source.Kind)
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		vector, err := base64ToVector(hit.Source.EmbeddingVector)
		if err != nil {
			return nil, fmt.Errorf("vector search: result %d: %v", i, err)
		}

		output[i] = traverser.VectorSearchResult{
			ClassName: hit.Source.ClassName,
			ID:        strfmt.UUID(hit.Source.ID),
			Kind:      k,
			Score:     hit.Score,
			Vector:    vector,
		}
	}

	return output, nil
}

// PutThing idempotently adds a Thing with its vector representation
func (r *Repo) PutThing(ctx context.Context,
	concept *models.Thing, vector []float32) error {
	err := r.putConcept(ctx, kind.Thing, concept.ID.String(),
		concept.Class, concept.Schema, vector)
	if err != nil {
		return fmt.Errorf("put thing: %v", err)
	}

	return nil
}

// PutAction idempotently adds a Action with its vector representation
func (r *Repo) PutAction(ctx context.Context,
	concept *models.Action, vector []float32) error {
	err := r.putConcept(ctx, kind.Action, concept.ID.String(),
		concept.Class, concept.Schema, vector)
	if err != nil {
		return fmt.Errorf("put action: %v", err)
	}

	return nil
}

func (r *Repo) putConcept(ctx context.Context,
	k kind.Kind, id, className string, props models.PropertySchema,
	vector []float32) error {

	var buf bytes.Buffer
	bucket := map[string]interface{}{
		"kind":             k.Name(),
		"id":               id,
		"class_name":       className,
		"embedding_vector": vectorToBase64(vector),
	}

	bucket = extendBucketWithProps(bucket, props)

	err := json.NewEncoder(&buf).Encode(bucket)
	if err != nil {
		return fmt.Errorf("index request: encode json: %v", err)
	}

	req := esapi.IndexRequest{
		Index:      classIndexFromClassName(k, className),
		DocumentID: id,
		Body:       &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		r.logger.WithField("action", "vector_index_put_concept").
			WithError(err).
			WithField("request", req).
			WithField("res", res).
			WithField("body_before_marshal", bucket).
			WithField("body", buf.String()).
			Errorf("put concept failed")

		return fmt.Errorf("index request: %v", err)
	}

	return nil
}

func (r *Repo) DeleteThing(ctx context.Context, className string, id strfmt.UUID) error {
	req := esapi.DeleteRequest{
		Index:      classIndexFromClassName(kind.Thing, className),
		DocumentID: id.String(),
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("delete thing: %v", err)
	}

	return nil
}

func (r *Repo) DeleteAction(ctx context.Context, className string, id strfmt.UUID) error {
	req := esapi.DeleteRequest{
		Index:      classIndexFromClassName(kind.Action, className),
		DocumentID: id.String(),
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("delete action: %v", err)
	}

	return nil
}

func extendBucketWithProps(bucket map[string]interface{}, props models.PropertySchema) map[string]interface{} {
	if props == nil {
		return bucket
	}

	propsMap := props.(map[string]interface{})
	for key, value := range propsMap {
		if gc, ok := value.(*models.GeoCoordinates); ok {
			value = map[string]interface{}{
				"lat": gc.Latitude,
				"lon": gc.Longitude,
			}
		}

		bucket[key] = value
	}

	return bucket
}

func vectorToBase64(array []float32) string {
	bytes := make([]byte, 0, 4*len(array))
	for _, a := range array {
		bits := math.Float32bits(a)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, bits)
		bytes = append(bytes, b...)
	}

	encoded := base64.StdEncoding.EncodeToString(bytes)
	return encoded
}

func base64ToVector(base64Str string) ([]float32, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, err
	}

	length := len(decoded)
	array := make([]float32, 0, length/4)

	for i := 0; i < len(decoded); i += 4 {
		bits := binary.BigEndian.Uint32(decoded[i : i+4])
		f := math.Float32frombits(bits)
		array = append(array, f)
	}
	return array, nil
}

func errorResToErr(res *esapi.Response, logger logrus.FieldLogger) error {
	if !res.IsError() {
		return nil
	}

	var e map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return fmt.Errorf("request is error: status: %s", res.Status())
	}

	logger.WithField("error", e).Error("error response from es")

	return fmt.Errorf("request is error: status: %v, type: %v, reason: %v",
		res.Status(),
		e["error"].(map[string]interface{})["type"],
		e["error"].(map[string]interface{})["reason"],
	)
}
