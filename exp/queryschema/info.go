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
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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

func (t *SchemaInfo) Schema(ctx context.Context) (*schema.Schema, error) {
	// TODO(kavi): Make it cleaner
	path := t.schemaPrefix
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var respPayload models.Schema

	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return nil, err
	}

	return &schema.Schema{Objects: &respPayload}, nil
}

func (t *SchemaInfo) TenantStatus(ctx context.Context, collection, tenant string) (string, uint64, error) {
	respPayload := []Response{}

	path := t.schemaPrefix + "/" + collection + "/tenants"
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", 0, err
	}

	var rerr error
	for _, v := range respPayload {
		for _, e := range v.Error {
			rerr = errors.Join(rerr, errors.New(e.Message))
		}
		if strings.EqualFold(v.Name, tenant) {
			return v.Status, 0, nil
		}

	}

	if rerr != nil {
		return "", 0, rerr
	}

	return "", 0, ErrTenantNotFound
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
	Error  []ErrorResponse `json:"error,omitempty"`
	Status string          `json:"activityStatus"`
	Name   string          `json:"name"`
}

type ErrorResponse struct {
	Message string `json:"message"`
}
