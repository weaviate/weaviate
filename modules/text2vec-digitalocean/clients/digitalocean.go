//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"net/url"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/ent"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	moduleLabel = "digitalocean"
	dummyLimit  = 10000000
)

type embeddingsRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
	Dimensions     *int64   `json:"dimensions,omitempty"`
}

type embeddingsResponse struct {
	Object string                  `json:"object"`
	Data   []embeddingData         `json:"data,omitempty"`
	Model  string                  `json:"model,omitempty"`
	Usage  *modulecomponents.Usage `json:"usage,omitempty"`
	Error  *digitalOceanError      `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type digitalOceanError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

type Client struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: modulecomponents.NewBaseHttpClient(timeout),
		logger:     logger,
	}
}

func (c *Client) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	// Error metrics are emitted inside vectorize() with rich labels
	// (endpoint, status, error). Avoid double-counting here.
	return c.vectorize(ctx, input, cfg)
}

func (c *Client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	res, _, _, err := c.vectorize(ctx, input, cfg)
	return res, err
}

func (c *Client) vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	settings := ent.NewClassSettings(cfg)
	endpoint, err := buildURL(ctx, settings.BaseURL())
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "build /v1/embeddings URL")
	}

	metrics := monitoring.GetMetrics()
	startTime := time.Now()
	metrics.ModuleExternalRequests.WithLabelValues("text2vec", moduleLabel).Inc()
	defer func() {
		metrics.ModuleExternalRequestDuration.WithLabelValues(moduleLabel, endpoint).Observe(time.Since(startTime).Seconds())
	}()

	body, err := json.Marshal(embeddingsRequest{
		Model:          settings.Model(),
		Input:          input,
		EncodingFormat: "float",
		Dimensions:     settings.Dimensions(),
	})
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "create POST request")
	}

	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "API key")
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Set("Content-Type", "application/json")
	if weaviateUUID := settings.WeaviateUUID(); weaviateUUID != "" {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/%s %s", weaviateUUID, build.Version))
	} else {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/unknown %s", build.Version))
	}

	metrics.ModuleExternalRequestSingleCount.WithLabelValues("text2vec", endpoint).Inc()
	metrics.ModuleExternalRequestSize.WithLabelValues("text2vec", endpoint).Observe(float64(len(body)))

	res, err := c.httpClient.Do(req)
	if res != nil {
		metrics.ModuleExternalResponseStatus.WithLabelValues("text2vec", endpoint, strconv.Itoa(res.StatusCode)).Inc()
	}
	if err != nil {
		metrics.ModuleCallError.WithLabelValues(moduleLabel, endpoint, err.Error()).Inc()
		return nil, nil, 0, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	requestID := res.Header.Get("x-request-id")
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "read response body")
	}
	metrics.ModuleExternalResponseSize.WithLabelValues("text2vec", endpoint).Observe(float64(len(bodyBytes)))

	var parsed embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to parse vectorization response (status %d): %w", res.StatusCode, err)
	}

	if res.StatusCode != http.StatusOK || parsed.Error != nil {
		return nil, nil, 0, formatError(res.StatusCode, requestID, parsed.Error)
	}

	if len(parsed.Data) == 0 {
		return nil, nil, 0, errors.New("no data returned from DigitalOcean Serverless Inference API")
	}

	rateLimit := rateLimitsFromHeader(res.Header)

	// DigitalOcean's response does not echo the input text back; data[i].Object
	// is the literal string "embedding". Map embeddings back to the original
	// inputs using the Index field so that callers can pair vectors with the
	// text they correspond to even if the API ever returns out-of-order data.
	//
	// We also defend against partial responses: every input slot must be
	// populated exactly once with a non-empty embedding, otherwise we'd
	// silently hand callers nil vectors and corrupt downstream results.
	embeddings := make([][]float32, len(input))
	for _, d := range parsed.Data {
		if d.Index < 0 || d.Index >= len(input) {
			return nil, nil, 0, fmt.Errorf("DigitalOcean response data index %d out of range for %d inputs", d.Index, len(input))
		}
		if embeddings[d.Index] != nil {
			return nil, nil, 0, fmt.Errorf("DigitalOcean response contained duplicate index %d", d.Index)
		}
		if len(d.Embedding) == 0 {
			return nil, nil, 0, fmt.Errorf("DigitalOcean response returned empty embedding for index %d", d.Index)
		}
		embeddings[d.Index] = d.Embedding
	}
	for i, e := range embeddings {
		if e == nil {
			return nil, nil, 0, fmt.Errorf("DigitalOcean response missing embedding for input index %d (got %d of %d)", i, len(parsed.Data), len(input))
		}
	}
	if parsed.Usage != nil {
		vrt := metrics.VectorizerRequestTokens
		vrt.WithLabelValues("input", endpoint).Observe(float64(parsed.Usage.PromptTokens))
		vrt.WithLabelValues("output", endpoint).Observe(float64(parsed.Usage.CompletionTokens))
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(parsed.Data[0].Embedding),
		Vector:     embeddings,
	}, rateLimit, modulecomponents.GetTotalTokens(parsed.Usage), nil
}

func buildURL(ctx context.Context, baseURL string) (string, error) {
	if header := modulecomponents.GetValueFromContext(ctx, "X-Digitalocean-Baseurl"); header != "" {
		if err := modulecomponents.ValidateBaseURL(header); err != nil {
			return "", err
		}
		baseURL = header
	}
	return url.JoinPath(baseURL, "/v1/embeddings")
}

func formatError(statusCode int, requestID string, body *digitalOceanError) error {
	endpoint := "DigitalOcean Serverless Inference API"
	msg := fmt.Sprintf("connection to: %s failed with status: %d", endpoint, statusCode)
	if requestID != "" {
		msg = fmt.Sprintf("%s request-id: %s", msg, requestID)
	}
	if body != nil && body.Message != "" {
		msg = fmt.Sprintf("%s error: %s", msg, body.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("text2vec", endpoint, msg, strconv.Itoa(statusCode)).Inc()
	return errors.New(msg)
}

// rateLimitsFromHeader parses DigitalOcean's ratelimit-* headers, which carry
// per-request totals. Tokens-per-minute is not exposed, so we set a high dummy
// limit to keep the batcher from throttling on it.
func rateLimitsFromHeader(header http.Header) *modulecomponents.RateLimits {
	limit := getHeaderInt(header, "ratelimit-limit")
	remaining := getHeaderInt(header, "ratelimit-remaining")

	var resetTime time.Time
	if resetEpoch, err := strconv.ParseInt(header.Get("ratelimit-reset"), 10, 64); err == nil && resetEpoch > 0 {
		resetTime = time.Unix(resetEpoch, 0)
	} else {
		resetTime = time.Now().Add(time.Minute)
	}

	if limit <= 0 {
		limit = dummyLimit
	}
	if remaining < 0 {
		remaining = limit
	}

	return &modulecomponents.RateLimits{
		LimitRequests:     limit,
		LimitTokens:       dummyLimit,
		RemainingRequests: remaining,
		RemainingTokens:   dummyLimit,
		ResetRequests:     resetTime,
		ResetTokens:       time.Now().Add(time.Minute),
	}
}

func getHeaderInt(header http.Header, key string) int {
	value := header.Get(key)
	if value == "" {
		return -1
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return i
}

func (c *Client) GetApiKeyHash(ctx context.Context, _ moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client) GetVectorizerRateLimit(ctx context.Context, _ moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Digitalocean", 0, 0)
	if rpm <= 0 {
		rpm = dummyLimit
	}
	if tpm <= 0 {
		tpm = dummyLimit
	}
	return &modulecomponents.RateLimits{
		LimitRequests:     rpm,
		LimitTokens:       tpm,
		RemainingRequests: rpm,
		RemainingTokens:   tpm,
		ResetRequests:     time.Now().Add(61 * time.Second),
		ResetTokens:       time.Now().Add(61 * time.Second),
	}
}

func (c *Client) getApiKey(ctx context.Context) (string, error) {
	if header := modulecomponents.GetValueFromContext(ctx, "X-Digitalocean-Api-Key"); header != "" {
		return header, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: X-Digitalocean-Api-Key nor in environment variable under DIGITALOCEAN_APIKEY")
}
