//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queryschema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/weaviate/weaviate/entities/models"
)

const (
	// v1/schema/**
	DefaultSchemaPrefix = "v1/schema"
)

var ErrTenantNotFound = errors.New("tenant not found")

type SchemaInfo struct {
	addr         string
	schemaPrefix string
	client       *http.Client
}

func NewSchemaInfo(addr, schemaPrefix string) *SchemaInfo {
	c := http.DefaultClient
	c.Timeout = 2 * time.Second

	return &SchemaInfo{
		addr:         addr,
		schemaPrefix: schemaPrefix,
		client:       c,
	}
}

func (t *SchemaInfo) TenantStatus(ctx context.Context, collection, tenant string) (string, []string, int64, error) {
	respPayload := Response{}

	path := t.schemaPrefix + "/" + collection + "/tenants/" + tenant
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return "", nil, 0, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", nil, 0, err
	}

	var rerr error
	if resp.StatusCode/100 != 2 {
		if len(respPayload.Error) == 0 {
			return "", nil, 0, errors.New("status code is non-200 but error is not set")
		}
		for _, e := range respPayload.Error {
			rerr = errors.Join(rerr, errors.New(e.Message))
		}
	}
	if rerr != nil {
		return "", nil, 0, rerr
	}
	return respPayload.Status, respPayload.BelongsToNodes, respPayload.DataVersion, nil
}

// Collection returns details about single collection from the schema.
// https://weaviate.io/developers/weaviate/api/rest#tag/schema/GET/schema/{className}
func (t *SchemaInfo) Collection(ctx context.Context, collection string) (*models.Class, error) {
	var classResp models.Class

	path := path.Join(t.schemaPrefix, collection)
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		var (
			rerr  error
			eresp Response
		)
		if err := json.NewDecoder(resp.Body).Decode(&eresp); err != nil {
			return nil, err
		}
		if len(eresp.Error) == 0 {
			return nil, errors.New("status code is non-200 but error is not set")
		}
		for _, e := range eresp.Error {
			rerr = errors.Join(rerr, errors.New(e.Message))
		}
		return nil, rerr
	}

	if err := json.NewDecoder(resp.Body).Decode(&classResp); err != nil {
		return nil, err
	}

	return &classResp, nil
}

type Response struct {
	Error          []ErrorResponse `json:"error,omitempty"`
	BelongsToNodes []string        `json:"belongsToNodes"`
	Status         string          `json:"activityStatus"`
	Name           string          `json:"name"`
	DataVersion    int64           `json:"dataVersion"`
}

type ErrorResponse struct {
	Message string `json:"message"`
}
