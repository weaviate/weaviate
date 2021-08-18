package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/cluster"
)

type ClusterScheme struct {
	client *http.Client
}

func NewClusterScheme(httpClient *http.Client) *ClusterScheme {
	return &ClusterScheme{client: httpClient}
}

func (c *ClusterScheme) OpenTransaction(ctx context.Context, host string,
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

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return errors.Errorf("unexpected status code %d", res.StatusCode)
	}

	return nil
}

func (c *ClusterScheme) AbortTransaction(ctx context.Context, host string,
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

func (c *ClusterScheme) CommitTransaction(ctx context.Context, host string,
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
