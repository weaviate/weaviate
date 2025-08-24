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
	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
)

const (
	DefaultOrigin = "https://router.huggingface.co/hf-inference/models"
	DefaultPath   = "pipeline/feature-extraction"
)

// there are no explicit rate limits: https://huggingface.co/docs/inference-providers/providers/hf-inference#feature-extraction
// so we set values that work and leave it up to the users to increase these values
const (
	DefaultRPM = 100      //
	DefaultTPM = 10000000 // no token limit
)

type embeddingsRequest struct {
	Inputs  []string `json:"inputs"`
	Options *options `json:"options,omitempty"`
}

type options struct {
	WaitForModel bool `json:"wait_for_model,omitempty"`
	UseGPU       bool `json:"use_gpu,omitempty"`
	UseCache     bool `json:"use_cache,omitempty"`
}

type embedding [][]float32

type embeddingBert [][][][]float32

type embeddingObject struct {
	Embeddings embedding `json:"embeddings"`
}

type huggingFaceApiError struct {
	Error         string   `json:"error"`
	EstimatedTime *float32 `json:"estimated_time,omitempty"`
	Warnings      []string `json:"warnings"`
}

type vectorizer struct {
	apiKey                string
	httpClient            *http.Client
	bertEmbeddingsDecoder *bertEmbeddingsDecoder
	logger                logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		bertEmbeddingsDecoder: newBertEmbeddingsDecoder(),
		logger:                logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	res, err := v.vectorize(ctx, v.getURL(config), input, v.getOptions(config))
	return res, nil, 0, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg)
	return v.vectorize(ctx, v.getURL(config), input, v.getOptions(config))
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	icheck := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		EndpointURL:  icheck.EndpointURL(),
		Model:        icheck.PassageModel(),
		WaitForModel: icheck.OptionWaitForModel(),
		UseGPU:       icheck.OptionUseGPU(),
		UseCache:     icheck.OptionUseCache(),
	}
}

func (v *vectorizer) vectorize(ctx context.Context, url string,
	input []string, options options,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	body, err := json.Marshal(embeddingsRequest{
		Inputs:  input,
		Options: &options,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	if apiKey, err := v.getApiKey(ctx); apiKey != "" && err == nil {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if err := checkResponse(res, bodyBytes); err != nil {
		return nil, err
	}

	vector, errs, err := v.decodeVector(bodyBytes)
	if err != nil {
		return nil, errors.Wrap(err, "cannot decode vector")
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(vector[0]),
		Vector:     vector,
		Errors:     errs,
	}, nil
}

func checkResponse(res *http.Response, bodyBytes []byte) error {
	if res.StatusCode < 400 {
		return nil
	}

	var resBody huggingFaceApiError
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return fmt.Errorf("unmarshal error response body: %v", string(bodyBytes))
	}

	message := fmt.Sprintf("failed with status: %d", res.StatusCode)
	if resBody.Error != "" {
		message = fmt.Sprintf("%s error: %v", message, resBody.Error)
		if resBody.EstimatedTime != nil {
			message = fmt.Sprintf("%s estimated time: %v", message, *resBody.EstimatedTime)
		}
		if len(resBody.Warnings) > 0 {
			message = fmt.Sprintf("%s warnings: %v", message, resBody.Warnings)
		}
	}

	if res.StatusCode == http.StatusInternalServerError {
		message = fmt.Sprintf("connection to HuggingFace %v", message)
	}

	return errors.New(message)
}

func (v *vectorizer) decodeVector(bodyBytes []byte) ([][]float32, []error, error) {
	var emb embedding
	if err := json.Unmarshal(bodyBytes, &emb); err != nil {
		var embObject embeddingObject
		if err := json.Unmarshal(bodyBytes, &embObject); err != nil {
			var embBert embeddingBert
			if err := json.Unmarshal(bodyBytes, &embBert); err != nil {
				return nil, nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
			}

			if len(embBert) == 1 && len(embBert[0]) > 0 {
				vectors := make([][]float32, len(embBert[0]))
				errs := make([]error, len(embBert[0]))
				for i, embBer := range embBert[0] {
					vectors[i], errs[i] = v.bertEmbeddingsDecoder.calculateVector(embBer)
				}
				return vectors, errs, nil
			}

			return nil, nil, errors.New("unprocessable response body")
		}
		if len(embObject.Embeddings) > 0 {
			return embObject.Embeddings, nil, nil
		}

		return nil, nil, errors.New("unprocessable response body")
	}

	if len(emb) > 0 {
		return emb, nil, nil
	}

	return nil, nil, errors.New("unprocessable response body")
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, _ := modulecomponents.GetRateLimitFromContext(ctx, "Cohere", DefaultRPM, 0)

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests -= 1
			}
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
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Huggingface-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Huggingface-Api-Key " +
		"nor in environment variable under HUGGINGFACE_APIKEY")
}

func (v *vectorizer) getOptions(config ent.VectorizationConfig) options {
	return options{
		WaitForModel: config.WaitForModel,
		UseGPU:       config.UseGPU,
		UseCache:     config.UseCache,
	}
}

func (v *vectorizer) getURL(config ent.VectorizationConfig) string {
	if config.EndpointURL != "" {
		return config.EndpointURL
	}

	return fmt.Sprintf("%s/%s/%s", DefaultOrigin, config.Model, DefaultPath)
}

func (v *vectorizer) HasTokenLimit() bool { return false }

func (v *vectorizer) ReturnsRateLimit() bool { return false }
