//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	httpUtils "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/utils"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

type httpV1Client struct {
	httpClients map[string]*http.Client
	logger      logrus.FieldLogger
	rwLock      sync.RWMutex
}

type predictV1Request struct {
	Instances []string `json:"instances"`
}

type predictV1Response struct {
	Predictions [][]float32 `json:"predictions"`
}

// type httpConnectionArgs struct{}

// func toHttpConnectionArgs(args map[string]interface{}) (*httpConnectionArgs, error) {
// 	return &httpConnectionArgs{}, nil
// }

func NewHTTPV1Client(logger logrus.FieldLogger) *httpV1Client {
	return &httpV1Client{
		httpClients: map[string]*http.Client{},
		logger:      logger,
	}
}

// Vectorize implements Client.
func (c *httpV1Client) Vectorize(ctx context.Context, input string, config ent.ModuleConfig) (*ent.VectorizationResult, error) {
	var client *http.Client
	serviceUrl := config.Url
	cachedClient, ok := c.connection(serviceUrl)

	if !ok {
		connArgs, err := httpUtils.ToHttpConnectionArgs(config.ConnectionArgs)
		if err != nil {
			return nil, errors.Wrap(err, "invalid http connection args")
		}
		client, err = c.createConnection(ctx, serviceUrl, *connArgs)

		if err != nil {
			return nil, errors.Wrap(err, "couldn't create a http client")
		}
	} else {
		client = cachedClient
	}

	return c.vectorize(ctx, client, input, serviceUrl, config.Model, config.Version, config.EmbeddingDims)
}

func (c *httpV1Client) connection(target string) (*http.Client, bool) {
	c.rwLock.RLock()
	conn, ok := c.httpClients[target]
	defer c.rwLock.RUnlock()
	return conn, ok
}

func (c *httpV1Client) setConnection(target string, conn *http.Client) {
	c.rwLock.Lock()
	c.httpClients[target] = conn
	defer c.rwLock.Unlock()
}

func (c *httpV1Client) vectorize(ctx context.Context, client *http.Client, input string, url string, modelName string, modelVersion string, embeddingDims int32) (*ent.VectorizationResult, error) {
	fullUrl := appendHTTPV1InferEndpoint(url, modelName, modelVersion)
	requestBody := createV1RequestBody(input)
	marshalled, err := json.Marshal(requestBody)
	if err != nil {
		return nil, errors.Wrap(err, "json marshalling error")
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fullUrl, bytes.NewBuffer(marshalled))
	if err != nil {
		return nil, errors.Wrap(err, "vectorize failed")
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, errors.Wrap(err, "vectorize failed")
	}
	defer response.Body.Close()

	unmarshalled, err := parseV1ResponseBody(response.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "error while decoding http response")
	}

	if err := validateV1Response(unmarshalled, embeddingDims); err != nil {
		return nil, errors.Wrap(err, "invalid http response")
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: int(embeddingDims),
		Vector:     unmarshalled.Predictions[0],
	}, nil
}

func appendHTTPV1InferEndpoint(u string, modelName string, modelVersion string) string {
	modelEscaped := url.PathEscape(modelName)

	if versionEscaped := url.PathEscape(modelVersion); versionEscaped != "" {
		return fmt.Sprintf("%v/v1/models/%v/versions/%v:predict", u, modelEscaped, versionEscaped)
	}

	return fmt.Sprintf("%v/v1/models/%v:predict", u, modelEscaped)
}

func createV1RequestBody(input string) predictV1Request {
	return predictV1Request{
		Instances: []string{input},
	}
}

func parseV1ResponseBody(r io.Reader) (*predictV1Response, error) {
	result := predictV1Response{}
	err := json.NewDecoder(r).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func validateV1Response(r *predictV1Response, embeddingDims int32) error {
	if len(r.Predictions) != 1 {
		return errors.New("response contains more than one vector")
	}

	if dims := len(r.Predictions[0]); dims != int(embeddingDims) {
		return fmt.Errorf("expected embedding dim %v was %v", embeddingDims, dims)
	}

	return nil
}

func (c *httpV1Client) createConnection(ctx context.Context, url string, config httpUtils.HttpConnectionArgs) (*http.Client, error) {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
	}
	c.setConnection(url, client)

	return client, nil
}
