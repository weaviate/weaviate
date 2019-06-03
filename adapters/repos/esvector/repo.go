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
	index      = "weaviate"
	doctype    = "concept"
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

// SetMappings for overall concept doctype of weaviate index
func (r *Repo) SetMappings(ctx context.Context) error {
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
	req := esapi.IndicesPutMappingRequest{
		Index:        []string{index},
		DocumentType: doctype,
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
		return fmt.Errorf("request is error: error parsing the response body: %s", err)
	}

	return fmt.Errorf("request is error: status: %v, type: %v, reason: %v",
		res.Status(),
		e["error"].(map[string]interface{})["type"],
		e["error"].(map[string]interface{})["reason"],
	)
}
