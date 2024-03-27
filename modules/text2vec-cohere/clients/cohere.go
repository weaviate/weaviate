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
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-cohere/ent"
)

const (
	DefaultRPM = 10000    // from https://docs.cohere.com/docs/going-live#production-key-specifications default for production keys
	DefaultTPM = 10000000 // no token limit used by cohere
)

type embeddingsRequest struct {
	Texts     []string  `json:"texts"`
	Model     string    `json:"model,omitempty"`
	Truncate  string    `json:"truncate,omitempty"`
	InputType inputType `json:"input_type,omitempty"`
}

type billedUnits struct {
	InputTokens    int `json:"input_tokens,omitempty"`
	OutputTokens   int `json:"output_tokens,omitempty"`
	SearchUnits    int `json:"search_units,omitempty"`
	Classificatons int `json:"classifications,omitempty"`
}

type meta struct {
	BilledUnits billedUnits `json:"billed_units,omitempty"`
	Warnings    []string    `json:"warnings,omitempty"`
}

type embeddingsResponse struct {
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Message    string      `json:"message,omitempty"`
	Meta       meta        `json:"meta,omitempty"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *cohereUrlBuilder
	logger     logrus.FieldLogger
}

type inputType string

const (
	searchDocument inputType = "search_document"
	searchQuery    inputType = "search_query"
)

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: newCohereUrlBuilder(),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	config := v.getVectorizationConfig(cfg)
	res, err := v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, searchDocument)
	return res, nil, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	config := v.getVectorizationConfig(cfg)
	res, err := v.vectorize(ctx, input, config.Model, config.Truncate, config.BaseURL, searchQuery)
	return res, nil, err
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	icheck := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:    icheck.Model(),
		BaseURL:  icheck.BaseURL(),
		Truncate: icheck.Truncate(),
	}
}

func (v *vectorizer) vectorize(ctx context.Context, input []string,
	model, truncate, baseURL string, inputType inputType,
) (*modulecomponents.VectorizationResult, error) {
	body, err := json.Marshal(embeddingsRequest{
		Texts:     input,
		Model:     model,
		Truncate:  truncate,
		InputType: inputType,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	url := v.getCohereUrl(ctx, baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Cohere API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Request-Source", "unspecified:weaviate")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode >= 500 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Message, "connection to Cohere failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	} else if res.StatusCode > 200 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Message, "failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	}

	if len(resBody.Embeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &modulecomponents.VectorizationResult{
		Text:       input,
		Dimensions: len(resBody.Embeddings[0]),
		Vector:     resBody.Embeddings,
	}, nil
}

func (v *vectorizer) getCohereUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := v.getValueFromContext(ctx, "X-Cohere-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return v.urlBuilder.url(passedBaseURL)
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context) *modulecomponents.RateLimits {
	rpm := DefaultRPM

	if rpmS := v.getValueFromContext(ctx, "X-Cohere-Ratelimit-RequestPM-Embedding"); rpmS != "" {
		s, err := strconv.Atoi(rpmS)
		if err == nil {
			rpm = s
		}
	}

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			limits.RemainingRequests -= 1
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		// high dummy values
		limits.RemainingTokens = DefaultTPM
		limits.LimitTokens = DefaultTPM
		limits.ResetTokens = time.Now().Add(time.Duration(1) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction() // set initial values

	return initialRL
}

func getErrorMessage(statusCode int, resBodyError string, errorTemplate string) string {
	if resBodyError != "" {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError)
	}
	return fmt.Sprintf(errorTemplate, statusCode)
}

func (v *vectorizer) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}
	return ""
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if apiKey := v.getValueFromContext(ctx, "X-Cohere-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Cohere-Api-Key " +
		"nor in environment variable under COHERE_APIKEY")
}
