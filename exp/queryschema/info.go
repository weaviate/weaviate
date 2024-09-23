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

func (t *SchemaInfo) TenantStatus(ctx context.Context, collection, tenant string) (string, error) {
	respPayload := []Response{}

	path := t.schemaPrefix + "/" + collection + "/tenants"
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", err
	}

	var rerr error
	for _, v := range respPayload {
		for _, e := range v.Error {
			rerr = errors.Join(rerr, errors.New(e.Message))
		}
		if strings.EqualFold(v.Name, tenant) {
			return v.Status, nil
		}

	}

	if rerr != nil {
		return "", rerr
	}

	return "", ErrTenantNotFound
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
