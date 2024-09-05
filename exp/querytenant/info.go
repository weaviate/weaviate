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

package querytenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var ErrTenantNotFound = errors.New("tenant not found")

type TenantInfo struct {
	addr   string
	path   string
	client *http.Client
}

func NewTenantInfo(addr, path string) *TenantInfo {
	c := http.DefaultClient
	c.Timeout = 2 * time.Second

	return &TenantInfo{
		addr:   addr,
		path:   path,
		client: c,
	}
}

func (t *TenantInfo) TenantStatus(ctx context.Context, collection, tenant string) (string, error) {
	respPayload := []struct {
		Error  []ErrorResponse `json:"error,omitempty"`
		Status string          `json:"activityStatus"`
		Name   string          `json:"name"`
	}{}

	path := fmt.Sprintf(t.path, collection)
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

type ErrorResponse struct {
	Message string `json:"message"`
}
