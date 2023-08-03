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

package httpv2

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
	utils "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/utils"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

type DataType = string

const (
	TYPE_BYTES = DataType("BYTES")
)

type HttpV2Client struct {
	httpClients map[string]*http.Client
	logger      logrus.FieldLogger
	rwLock      sync.RWMutex
}

type inputTensor struct {
	Name     string     `json:"name"`
	Shape    []int32    `json:"shape"`
	Data     [][]string `json:"data"`
	Datatype DataType   `json:"datatype"`
}

type outputTensor struct {
	Name     string    `json:"name"`
	Shape    []int32   `json:"shape"`
	Data     []float32 `json:"data"`
	Datatype DataType  `json:"datatype"`
}

type requestedOutput struct {
	Name string `json:"name"`
}

type predictV2Request struct {
	Inputs  []inputTensor     `json:"inputs"`
	Outputs []requestedOutput `json:"outputs"`
}

type predictV2Response struct {
	ModelName    string         `json:"model_name"`
	ModelVersion string         `json:"model_version"`
	Outputs      []outputTensor `json:"outputs"`
}

func NewHTTPV2Client(logger logrus.FieldLogger) *HttpV2Client {
	return &HttpV2Client{
		httpClients: map[string]*http.Client{},
		logger:      logger,
		rwLock:      sync.RWMutex{},
	}
}

// Vectorize implements Client.
func (c *HttpV2Client) Vectorize(ctx context.Context, input string, config ent.ModuleConfig) (*ent.VectorizationResult, error) {
	var client *http.Client
	serviceUrl := config.Url
	cachedClient, ok := c.connection(serviceUrl)

	if !ok {
		connArgs, err := utils.ToHttpConnectionArgs(config.ConnectionArgs)
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

	return c.vectorize(ctx, client, input, config)
}

func (c *HttpV2Client) vectorize(ctx context.Context, client *http.Client,
	input string, settings ent.ModuleConfig,
) (*ent.VectorizationResult, error) {
	fullUrl := appendHTTPV2InferEndpoint(settings.Url, settings.Model, settings.Version)
	requestBody := createV2RequestBody(settings.Input, input, settings.Output)
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

	unmarshalled, err := parseV2ResponseBody(response.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "error while decoding http response")
	}

	if err := validateV2Response(unmarshalled, settings.EmbeddingDims); err != nil {
		return nil, errors.Wrap(err, "invalid http response")
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: int(settings.EmbeddingDims),
		Vector:     unmarshalled.Outputs[0].Data,
	}, nil
}

func (c *HttpV2Client) connection(target string) (*http.Client, bool) {
	c.rwLock.RLock()
	conn, ok := c.httpClients[target]
	defer c.rwLock.RUnlock()
	return conn, ok
}

func (c *HttpV2Client) setConnection(target string, conn *http.Client) {
	c.rwLock.Lock()
	c.httpClients[target] = conn
	defer c.rwLock.Unlock()
}

func appendHTTPV2InferEndpoint(u string, modelName string, modelVersion string) string {
	nameEscaped := url.PathEscape(modelName)

	if versionEscaped := url.PathEscape(modelVersion); versionEscaped != "" {
		return fmt.Sprintf("%v/v2/models/%v/versions/%v/infer", u, nameEscaped, versionEscaped)
	}
	return fmt.Sprintf("%v/v2/models/%v/infer", u, nameEscaped)
}

func createV2RequestBody(inputName string, inputValue string, outputName string) predictV2Request {
	return predictV2Request{
		Inputs: []inputTensor{
			{
				Name:     inputName,
				Shape:    []int32{1, 1},
				Data:     [][]string{{inputValue}},
				Datatype: TYPE_BYTES,
			},
		},
		Outputs: []requestedOutput{
			{
				Name: outputName,
			},
		},
	}
}

func parseV2ResponseBody(r io.Reader) (*predictV2Response, error) {
	result := predictV2Response{}
	err := json.NewDecoder(r).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func validateV2Response(r *predictV2Response, embeddingDims int32) error {
	if len(r.Outputs) != 1 {
		return errors.New("response contains more than one output")
	}

	output := r.Outputs[0]

	if len(output.Shape) != 2 {
		return fmt.Errorf("unexpected shape of output tensor %v", output.Shape)
	}

	if dims := output.Shape[1]; dims != embeddingDims {
		return fmt.Errorf("expected embedding dim %v was %v", embeddingDims, dims)
	}

	return nil
}

func (c *HttpV2Client) createConnection(ctx context.Context, url string, config utils.HttpConnectionArgs) (*http.Client, error) {
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
