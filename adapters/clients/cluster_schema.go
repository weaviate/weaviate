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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/weaviate/weaviate/usecases/cluster"
)

type ClusterSchema struct {
	client *http.Client
}

func NewClusterSchema(httpClient *http.Client) *ClusterSchema {
	return &ClusterSchema{client: httpClient}
}

func (c *ClusterSchema) OpenTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/schema/transactions/"
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: host, Path: path}

	pl := txPayload{
		Type:          tx.Type,
		ID:            tx.ID,
		Payload:       tx.Payload,
		DeadlineMilli: tx.Deadline.UnixMilli(),
	}

	jsonBytes, err := json.Marshal(pl)
	if err != nil {
		return fmt.Errorf("marshal transaction payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("open http request: %w", err)
	}

	req.Header.Set("content-type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send http request: %w", err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusCreated {
		if res.StatusCode == http.StatusConflict {
			return cluster.ErrConcurrentTransaction
		}

		return fmt.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	// optional for backward-compatibility before v1.17 where only
	// write-transactions where supported. They had no return value other than
	// the status code. With the introduction of read-transactions it is now
	// possible to return the requested value
	if len(body) == 0 {
		return nil
	}

	var txRes txResponsePayload
	err = json.Unmarshal(body, &txRes)
	if err != nil {
		return fmt.Errorf("unexpected error unmarshalling tx response: %w", err)
	}

	if tx.ID != txRes.ID {
		return fmt.Errorf("unexpected mismatch between outgoing and incoming tx ids:"+
			"%s vs %s", tx.ID, txRes.ID)
	}

	tx.Payload = txRes.Payload

	return nil
}

func (c *ClusterSchema) AbortTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/schema/transactions/" + tx.ID
	method := http.MethodDelete
	url := url.URL{Scheme: "http", Host: host, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return fmt.Errorf("open http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send http request: %w", err)
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unexpected status code %d: %s", res.StatusCode, errBody)
	}

	return nil
}

func (c *ClusterSchema) CommitTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/schema/transactions/" + tx.ID + "/commit"
	method := http.MethodPut
	url := url.URL{Scheme: "http", Host: host, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return fmt.Errorf("open http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send http request: %w", err)
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unexpected status code %d: %s", res.StatusCode, errBody)
	}

	return nil
}

type txPayload struct {
	Type          cluster.TransactionType `json:"type"`
	ID            string                  `json:"id"`
	Payload       interface{}             `json:"payload"`
	DeadlineMilli int64                   `json:"deadlineMilli"`
}

type txResponsePayload struct {
	Type    cluster.TransactionType `json:"type"`
	ID      string                  `json:"id"`
	Payload json.RawMessage         `json:"payload"`
}
