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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-weaviate/ent"
)

const (
	DefaultRPM = 10000
	DefaultTPM = 10_000_000
)

type embeddingsRequest struct {
	Texts         []string `json:"texts"`
	IsSearchQuery bool     `json:"is_search_query,omitempty"`
	Dimensions    *int64   `json:"dimensions,omitempty"`
}

type embeddingsResponse struct {
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Metadata   metadata    `json:"metadata,omitempty"`
}

type embeddingsResponseError struct {
	Detail string `json:"detail"`
}

type metadata struct {
	Model                 string                  `json:"model,omitempty"`
	TimeTakenInference    float32                 `json:"time_taken_inference,omitempty"`
	NumEmbeddingsInferred int                     `json:"num_embeddings_inferred,omitempty"`
	Usage                 *modulecomponents.Usage `json:"usage,omitempty"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *weaviateEmbedUrlBuilder
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: newWeaviateEmbedUrlBuilder(),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	return v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, false, config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg)
	res, _, _, err := v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, true, config)
	return res, err
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	icheck := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:      icheck.Model(),
		BaseURL:    icheck.BaseURL(),
		Truncate:   icheck.Truncate(),
		Dimensions: icheck.Dimensions(),
	}
}

func (v *vectorizer) vectorize(ctx context.Context, input []string,
	model, truncate, baseURL string, isSearchQuery bool, config ent.VectorizationConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, isSearchQuery, config.Dimensions))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "marshal body")
	}

	url := v.getWeaviateEmbedURL(ctx, baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "create POST request")
	}
	token, err := v.getToken(ctx)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "authentication token")
	}
	clusterURL, err := v.getClusterURL(ctx)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "cluster URL")
	}

	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Request-Source", "unspecified:weaviate")
	req.Header.Add("X-Weaviate-Embedding-Model", model)
	req.Header.Add("X-Weaviate-Cluster-Url", clusterURL)

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "read response body")
	}

	if res.StatusCode > 200 {
		errorMessage := getErrorMessage(res.StatusCode, string(bodyBytes), "Weaviate embed API error: %d %s")
		return nil, nil, 0, errors.New(errorMessage)
	}

	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, nil, 0, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if len(resBody.Embeddings) == 0 {
		return nil, nil, 0, errors.Errorf("empty embeddings response")
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(resBody.Embeddings[0]),
		Vector:     resBody.Embeddings,
	}, nil, modulecomponents.GetTotalTokens(resBody.Metadata.Usage), nil
}

func (v *vectorizer) getWeaviateEmbedURL(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Weaviate-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return v.urlBuilder.url(passedBaseURL)
}

func (v *vectorizer) getEmbeddingsRequest(texts []string, isSearchQuery bool, dimensions *int64) embeddingsRequest {
	return embeddingsRequest{Texts: texts, IsSearchQuery: isSearchQuery, Dimensions: dimensions}
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := v.getToken(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Weaviate", DefaultRPM, DefaultTPM)

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

func getErrorMessage(statusCode int, resBodyError string, errorTemplate string) string {
	var errResp embeddingsResponseError
	if err := json.Unmarshal([]byte(resBodyError), &errResp); err != nil {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError)
	}
	return fmt.Sprintf(errorTemplate, statusCode, errResp.Detail)
}

func (v *vectorizer) getToken(ctx context.Context) (string, error) {
	if token := modulecomponents.GetValueFromContext(ctx, "Authorization"); token != "" {
		return token, nil
	}
	if v.apiKey != "" {
		return fmt.Sprintf("Bearer %s", v.apiKey), nil
	}
	return "", errors.New("neither authentication token found in request header: Authorization " +
		"nor api key in environment variable under WEAVIATE_APIKEY")
}

func (v *vectorizer) getClusterURL(ctx context.Context) (string, error) {
	if clusterURL := modulecomponents.GetValueFromContext(ctx, "X-Weaviate-Cluster-Url"); clusterURL != "" {
		return clusterURL, nil
	}
	return "", errors.New("no cluster URL found " +
		"in request header: X-Weaviate-Cluster-Url")
}
