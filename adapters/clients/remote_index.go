package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIndex struct {
	client *http.Client
}

func NewRemoteIndex(httpClient *http.Client) *RemoteIndex {
	return &RemoteIndex{client: httpClient}
}

func (c *RemoteIndex) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object) error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := obj.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	req.Header.Set("content-type", "application/vnd.weaviate.storobj+octet-stream")

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func (c *RemoteIndex) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	selectPropsBytes, err := json.Marshal(selectProps)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	additionalBytes, err := json.Marshal(additional)
	if err != nil {
		return nil, errors.Wrap(err, "marshal additional props")
	}

	selectPropsEncoded := base64.StdEncoding.EncodeToString(selectPropsBytes)
	additionalEncoded := base64.StdEncoding.EncodeToString(additionalBytes)

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("additional", additionalEncoded)
	q.Set("selectProperties", selectPropsEncoded)
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	var obj storobj.Object
	err = json.NewDecoder(res.Body).Decode(&obj)
	if err != nil {
		return nil, errors.Wrap(err, "decode body")
	}

	return &obj, nil
}

type searchParametersPayload struct {
	SearchVector []float32             `json:"searchVector"`
	Limit        int                   `json:"limit"`
	Filters      *filters.LocalFilter  `json:"filters"`
	Additional   additional.Properties `json:"additional"`
}

type searchResultsPayload struct {
	Results   []*storobj.Object `json:"results"`
	Distances []float32         `json:"distances"`
}

func (c *RemoteIndex) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	params := searchParametersPayload{
		SearchVector: vector,
		Limit:        limit,
		Filters:      filters,
		Additional:   additional,
	}

	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal request payload")
	}

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/_search", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(paramsBytes))
	if err != nil {
		return nil, nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	var results searchResultsPayload
	err = json.NewDecoder(res.Body).Decode(&results)
	if err != nil {
		return nil, nil, errors.Wrap(err, "decode body")
	}

	return results.Results, results.Distances, nil
}
