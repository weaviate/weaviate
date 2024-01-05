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
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type ClusterClassifications struct {
	client *http.Client
}

func NewClusterClassifications(httpClient *http.Client) *ClusterClassifications {
	return &ClusterClassifications{client: httpClient}
}

func (c *ClusterClassifications) OpenTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/classifications/transactions/"
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: host, Path: path}

	pl := txPayload{
		Type:    tx.Type,
		ID:      tx.ID,
		Payload: tx.Payload,
	}

	jsonBytes, err := json.Marshal(pl)
	if err != nil {
		return errors.Wrap(err, "marshal transaction payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	req.Header.Set("content-type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		if res.StatusCode == http.StatusConflict {
			return cluster.ErrConcurrentTransaction
		}

		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func (c *ClusterClassifications) AbortTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/classifications/transactions/" + tx.ID
	method := http.MethodDelete
	url := url.URL{Scheme: "http", Host: host, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		return errors.Errorf("unexpected status code %d", res.StatusCode)
	}

	return nil
}

func (c *ClusterClassifications) CommitTransaction(ctx context.Context, host string,
	tx *cluster.Transaction,
) error {
	path := "/classifications/transactions/" + tx.ID + "/commit"
	method := http.MethodPut
	url := url.URL{Scheme: "http", Host: host, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		return errors.Errorf("unexpected status code %d", res.StatusCode)
	}

	return nil
}
