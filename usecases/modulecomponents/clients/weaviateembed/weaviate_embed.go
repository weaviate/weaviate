//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package weaviateembed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const (
	DefaultBaseURL = "https://api.embedding.weaviate.io"
	URLPath        = "/v1/embeddings/embed"
)

type Client[T dto.Embedding] struct {
	httpClient *http.Client
	defaultRPM int
	defaultTPM int
}

type embeddingsRequest struct {
	Input         []string `json:"input"`
	IsSearchQuery bool     `json:"is_search_query,omitempty"`
	Model         string   `json:"model"`
	Dimensions    *int64   `json:"dimensions,omitempty"`
}

type embeddingsResponseError struct {
	Detail string `json:"detail"`
}

type embeddingsResponse[T dto.Embedding] struct {
	Embeddings []T      `json:"embeddings,omitempty"`
	Metadata   metadata `json:"metadata,omitempty"`
}

type metadata struct {
	Model                 string                  `json:"model,omitempty"`
	TimeTakenInference    float32                 `json:"time_taken_inference,omitempty"`
	NumEmbeddingsInferred int                     `json:"num_embeddings_inferred,omitempty"`
	Usage                 *modulecomponents.Usage `json:"usage,omitempty"`
}

func New[T dto.Embedding](timeout time.Duration, defaultRPM, defaultTPM int) *Client[T] {
	return &Client[T]{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		defaultRPM: defaultRPM,
		defaultTPM: defaultTPM,
	}
}

func (c *Client[T]) Vectorize(ctx context.Context, input []string, query bool, dimensions *int64, model, baseURL string) (*embeddingsResponse[T], error) {
	request := embeddingsRequest{Input: input, IsSearchQuery: query, Model: model, Dimensions: dimensions}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	url, err := c.getWeaviateEmbedURL(ctx, baseURL)
	if err != nil {
		return nil, errors.Wrap(err, "join API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	token, err := getToken(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "authentication token")
	}

	clusterURL, err := getClusterURL(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "cluster URL")
	}

	req.Header.Set("Authorization", token)
	req.Header.Add("X-Weaviate-Cluster-Url", clusterURL)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}

	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if res.StatusCode > 200 {
		errorMessage := getErrorMessage(res.StatusCode, string(bodyBytes), "Weaviate embed API error: %d %s")
		return nil, errors.New(errorMessage)
	}

	var resBody embeddingsResponse[T]
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if len(resBody.Embeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &resBody, nil
}

func (c *Client[T]) getWeaviateEmbedURL(ctx context.Context, baseURL string) (string, error) {
	var host string
	if baseURL != "" {
		host = baseURL
	} else {
		host = DefaultBaseURL
	}
	return url.JoinPath(host, URLPath)
}

func getToken(ctx context.Context) (string, error) {
	if token := modulecomponents.GetValueFromContext(ctx, "Authorization"); token != "" {
		return token, nil
	}
	return "", errors.New("no authentication token found in request header: Authorization")
}

func getClusterURL(ctx context.Context) (string, error) {
	if clusterURL := modulecomponents.GetValueFromContext(ctx, "X-Weaviate-Cluster-Url"); clusterURL != "" {
		return clusterURL, nil
	}
	return "", errors.New("no cluster URL found in request header: X-Weaviate-Cluster-Url")
}

func getErrorMessage(statusCode int, resBodyError string, errorTemplate string) string {
	var errResp embeddingsResponseError
	if err := json.Unmarshal([]byte(resBodyError), &errResp); err != nil {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError)
	}
	return fmt.Sprintf(errorTemplate, statusCode, errResp.Detail)
}

func (c *Client[T]) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := getToken(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client[T]) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Weaviate", c.defaultRPM, c.defaultTPM)

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests--
			}
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		limits.RemainingTokens = tpm
		limits.LimitTokens = tpm
		limits.ResetTokens = time.Now().Add(time.Duration(1) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}
