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
	"github.com/weaviate/weaviate/modules/text2vec-voyageai/ent"
)

type embeddingsRequest struct {
	Input      []string  `json:"input"`
	Model      string    `json:"model"`
	Truncation bool      `json:"truncation,omitempty"`
	InputType  inputType `json:"input_type,omitempty"`
}

type embeddingsDataResponse struct {
	Embeddings []float32 `json:"embedding"`
}

type embeddingsResponse struct {
	Data   []embeddingsDataResponse `json:"data,omitempty"`
	Model  string                   `json:"model,omitempty"`
	Detail string                   `json:"detail,omitempty"`
	Usage  *modulecomponents.Usage  `json:"usage,omitempty"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *voyageaiUrlBuilder
	logger     logrus.FieldLogger
}

type inputType string

const (
	searchDocument inputType = "document"
	searchQuery    inputType = "query"
)

type voyageRLModel struct {
	TokenLimit   int
	RequestLimit int
}

type voyageRL struct {
	rateLimitPerModel map[string]voyageRLModel
}

func newRL() voyageRL {
	// from https://docs.voyageai.com/docs/rate-limits
	return voyageRL{
		rateLimitPerModel: map[string]voyageRLModel{
			"voyage-3":      {TokenLimit: 2_000_000, RequestLimit: 1000},
			"voyage-3-lite": {TokenLimit: 4_000_000, RequestLimit: 1000},
			"default":       {TokenLimit: 1_000_000, RequestLimit: 1000},
		},
	}
}

func (r voyageRL) getLimitForModel(model string) voyageRLModel {
	rl, ok := r.rateLimitPerModel[model]
	if !ok {
		return r.rateLimitPerModel["default"]
	}
	return rl
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: newVoyageAIUrlBuilder(),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	res, usage, err := v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, searchDocument)
	return res, nil, usage, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg)
	res, _, err := v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, searchQuery)
	return res, err
}

func (v *vectorizer) vectorize(ctx context.Context, input []string,
	model string, truncate bool, baseURL string, inputType inputType,
) (*modulecomponents.VectorizationResult[[]float32], int, error) {
	body, err := json.Marshal(embeddingsRequest{
		Input:      input,
		Model:      model,
		Truncation: truncate,
		InputType:  inputType,
	})
	if err != nil {
		return nil, 0, errors.Wrapf(err, "marshal body")
	}

	url := v.getVoyageAIUrl(ctx, baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, 0, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "VoyageAI API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, 0, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 0, errors.Wrap(err, "read response body")
	}
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, 0, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		if resBody.Detail != "" {
			errorMessage := getErrorMessage(res.StatusCode, resBody.Detail, "connection to VoyageAI failed with status: %d error: %v")
			return nil, 0, errors.New(errorMessage)
		}
		errorMessage := getErrorMessage(res.StatusCode, "", "connection to VoyageAI failed with status: %d")
		return nil, 0, errors.New(errorMessage)
	}

	if len(resBody.Data) == 0 || len(resBody.Data[0].Embeddings) == 0 {
		return nil, 0, errors.Errorf("empty embeddings response")
	}

	vectors := make([][]float32, len(resBody.Data))
	for i, data := range resBody.Data {
		vectors[i] = data.Embeddings
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(resBody.Data[0].Embeddings),
		Vector:     vectors,
	}, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (v *vectorizer) getVoyageAIUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Voyageai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return v.urlBuilder.url(passedBaseURL)
}

func getErrorMessage(statusCode int, resBodyError string, errorTemplate string) string {
	if resBodyError != "" {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError)
	}
	return fmt.Sprintf(errorTemplate, statusCode)
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Voyageai-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-VoyageAI-Api-Key " +
		"nor in environment variable under VOYAGEAI_APIKEY")
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	config := v.getVectorizationConfig(cfg)
	rl := newRL()
	modelRL := rl.getLimitForModel(config.Model)
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Voyageai", modelRL.RequestLimit, modelRL.TokenLimit)
	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests -= 1
			}
			limits.RemainingTokens -= tokensUsed
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		limits.RemainingTokens = tpm
		limits.LimitTokens = tpm
		limits.ResetTokens = time.Now().Add(time.Duration(61) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:    settings.Model(),
		Truncate: settings.Truncate(),
		BaseURL:  settings.BaseURL(),
	}
}
