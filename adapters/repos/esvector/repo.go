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
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

const (
	doctype    = "doc"
	vectorProp = "embedding_vector"
)

// Repo stores and retrieves vector info in elasticsearch
type Repo struct {
	client *elasticsearch.Client
}

// NewRepo from existing es client
func NewRepo(client *elasticsearch.Client) *Repo {

	return &Repo{client}
}

// PutIndex idempotently creates an index
func (r *Repo) PutIndex(ctx context.Context, index string) error {

	ok, err := r.indexExists(ctx, index)
	if err != nil {
		return fmt.Errorf("create index: %v", err)
	}

	if ok {
		return nil
	}

	body := map[string]interface{}{
		"settings": map[string]interface{}{
			"index.mapping.single_type": true,
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return fmt.Errorf("create index: %v", err)
	}

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("create index: %v", err)
	}

	if err := errorResToErr(res); err != nil {
		return fmt.Errorf("create index: %v", err)
	}

	return nil
}

type conceptBucket struct {
	Kind            kind.Kind `json:"kind"`
	ID              string    `json:"id"`
	ClassName       string    `json:"className"`
	EmbeddingVector string    `json:"embeddingVector"`
}

// VectorSearch retrives the closest concepts by vector distance
func (r *Repo) VectorSearch(ctx context.Context, vector []float32) ([]kinds.VectorSearchResult, error) {

	return nil, nil
}

// PutThing idempotently adds a Thing with its vector representation
func (r *Repo) PutThing(ctx context.Context, concept *models.Thing,
	vector []float32) error {
	err := r.putConcept(ctx, kind.Thing, concept.ID.String(), concept.Class, vector)
	if err != nil {
		return fmt.Errorf("put thing: %v", err)
	}

	return nil
}

// PutAction idempotently adds a Action with its vector representation
func (r *Repo) PutAction(ctx context.Context, concept *models.Action,
	vector []float32) error {
	err := r.putConcept(ctx, kind.Action, concept.ID.String(), concept.Class, vector)
	if err != nil {
		return fmt.Errorf("put action: %v", err)
	}

	return nil
}

func (r *Repo) putConcept(ctx context.Context, k kind.Kind, id, class string,
	vector []float32) error {

	var buf bytes.Buffer
	bucket := conceptBucket{
		Kind:            k,
		ID:              id,
		ClassName:       class,
		EmbeddingVector: vectorToBase64(vector),
	}

	err := json.NewEncoder(&buf).Encode(bucket)
	if err != nil {
		return fmt.Errorf("index request: encode json: %v", err)
	}

	req := esapi.IndexRequest{
		// for now hard-code concepts index. In the future, this should
		// depend on the class name. Until then, there's a single index
		// for everything
		Index:        "concepts",
		DocumentType: doctype,
		DocumentID:   id,
		Body:         &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res); err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	return nil
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

func (r *Repo) indexExists(ctx context.Context, index string) (bool, error) {
	req := esapi.IndicesExistsRequest{
		Index: []string{index},
	}
	req.Do(ctx, r.client)

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return false, fmt.Errorf("check index exists: %v", err)
	}

	s := res.Status()
	switch s {
	case "200 OK":
		return true, nil
	case "404 Not Found":
		return false, nil
	default:
		return false, fmt.Errorf("check index exists: status: %s", s)
	}
}

// SetMappings for overall concept doctype of weaviate index
func (r *Repo) SetMappings(ctx context.Context, index string) error {
	var buf bytes.Buffer
	body := map[string]interface{}{
		"properties": map[string]interface{}{
			vectorProp: map[string]interface{}{
				"type":       "binary",
				"doc_values": true,
			},
		},
	}

	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return err
	}

	req := esapi.IndicesPutMappingRequest{
		Index:        []string{index},
		DocumentType: doctype,
		Body:         &buf,
	}
	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("set mappings: %v", err)
	}

	if err := errorResToErr(res); err != nil {
		return fmt.Errorf("set mappings: %v", err)
	}

	return nil
}

func errorResToErr(res *esapi.Response) error {
	if !res.IsError() {
		return nil
	}

	var e map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return fmt.Errorf("request is error: status: %s", res.Status())
	}

	return fmt.Errorf("request is error: status: %v, type: %v, reason: %v",
		res.Status(),
		e["error"].(map[string]interface{})["type"],
		e["error"].(map[string]interface{})["reason"],
	)
}
