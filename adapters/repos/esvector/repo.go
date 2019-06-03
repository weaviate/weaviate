package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
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
