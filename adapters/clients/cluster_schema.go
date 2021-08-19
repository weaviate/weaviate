package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/cluster"
)

type ClusterSchema struct {
	client *http.Client
}

func NewClusterSchema(httpClient *http.Client) *ClusterSchema {
	return &ClusterSchema{client: httpClient}
}

func (c *ClusterSchema) OpenTransaction(ctx context.Context, host string,
	tx *cluster.Transaction) error {
	path := "/schema/transactions/"
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

		body, _ := ioutil.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func (c *ClusterSchema) AbortTransaction(ctx context.Context, host string,
	tx *cluster.Transaction) error {
	path := "/schema/transactions/" + tx.ID
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

func (c *ClusterSchema) CommitTransaction(ctx context.Context, host string,
	tx *cluster.Transaction) error {
	path := "/schema/transactions/" + tx.ID + "/commit"
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

type txPayload struct {
	Type    cluster.TransactionType `json:"type"`
	ID      string                  `json:"id"`
	Payload interface{}             `json:"payload"`
}
