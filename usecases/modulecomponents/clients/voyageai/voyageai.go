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

package voyageai

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type multimodalType string

const (
	text        multimodalType = "text"
	imageBase64 multimodalType = "image_base64"
)

type multimodalInput struct {
	Content []multimodalContent `json:"content,omitempty"`
}

type multimodalContent struct {
	MultimodalType multimodalType `json:"type,omitempty"`
	Text           string         `json:"text,omitempty"`
	ImageBase64    string         `json:"image_base64,omitempty"`
}

type embeddingsRequest struct {
	Input      []string          `json:"input,omitempty"`
	Inputs     []multimodalInput `json:"inputs,omitempty"`
	Model      string            `json:"model"`
	Truncation bool              `json:"truncation,omitempty"`
	InputType  InputType         `json:"input_type,omitempty"`
}

type embeddingsDataResponse struct {
	Embedding []float32 `json:"embedding"`
}

type embeddingsResponse struct {
	Data   []embeddingsDataResponse `json:"data,omitempty"`
	Model  string                   `json:"model,omitempty"`
	Detail string                   `json:"detail,omitempty"`
	Usage  *modulecomponents.Usage  `json:"usage,omitempty"`
}

type UrlBuilder interface {
	URL(baseURL string) string
}

type Client struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder UrlBuilder
	logger     logrus.FieldLogger
}

type InputType string

const (
	Document InputType = "document"
	Query    InputType = "query"
)

type Settings struct {
	BaseURL   string
	Model     string
	Truncate  bool
	InputType InputType
}

type VoyageRLModel struct {
	TokenLimit   int
	RequestLimit int
}

func New(apiKey string, timeout time.Duration, urlBuilder UrlBuilder, logger logrus.FieldLogger) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: urlBuilder,
		logger:     logger,
	}
}

func (c *Client) Vectorize(ctx context.Context, input []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	resBody, err := c.vectorize(ctx, settings.BaseURL, embeddingsRequest{
		Input:      input,
		Model:      settings.Model,
		Truncation: settings.Truncate,
		InputType:  Document,
	})
	if err != nil {
		return nil, nil, 0, err
	}
	res, usage, err := c.getVectorizationResult(input, resBody)
	return res, nil, usage, err
}

func (c *Client) VectorizeQuery(ctx context.Context, input []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	resBody, err := c.vectorize(ctx, settings.BaseURL, embeddingsRequest{
		Input:      input,
		Model:      settings.Model,
		Truncation: settings.Truncate,
		InputType:  Query,
	})
	if err != nil {
		return nil, err
	}
	res, _, err := c.getVectorizationResult(input, resBody)
	return res, err
}

func (c *Client) VectorizeMultiModal(ctx context.Context, texts, images []string,
	settings Settings,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	request := c.getMultiModalEmbeddingsRequest(texts, images, settings)
	resBody, err := c.vectorize(ctx, settings.BaseURL, request)
	if err != nil {
		return nil, err
	}

	var textVectors, imageVectors [][]float32
	for i := range resBody.Data {
		if i < len(texts) {
			textVectors = append(textVectors, resBody.Data[i].Embedding)
		} else {
			imageVectors = append(imageVectors, resBody.Data[i].Embedding)
		}
	}

	res := &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}
	return res, nil
}

func (c *Client) getMultiModalEmbeddingsRequest(texts, images []string, settings Settings,
) embeddingsRequest {
	inputs := make([]multimodalInput, len(texts)+len(images))
	for i := range texts {
		inputs[i] = multimodalInput{Content: []multimodalContent{{Text: texts[i], MultimodalType: text}}}
	}
	offset := len(texts)
	for i := range images {
		if !strings.HasPrefix(images[i], "data:") {
			inputs[offset+i] = multimodalInput{Content: []multimodalContent{{ImageBase64: fmt.Sprintf("data:image/png;base64,%s", images[i]), MultimodalType: imageBase64}}}
		} else {
			inputs[offset+i] = multimodalInput{Content: []multimodalContent{{ImageBase64: images[i], MultimodalType: imageBase64}}}
		}
	}
	return embeddingsRequest{
		Inputs:     inputs,
		Model:      settings.Model,
		Truncation: settings.Truncate,
		InputType:  settings.InputType,
	}
}

func (c *Client) vectorize(ctx context.Context, baseURL string, request interface{},
) (*embeddingsResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	url := c.getVoyageAIUrl(ctx, baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "VoyageAI API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
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
		return nil, errors.Wrapf(err, "unmarshal response body. got: %v", string(bodyBytes))
	}

	if res.StatusCode != 200 {
		return nil, errors.New(c.getErrorMessage(res.StatusCode, resBody.Detail))
	}

	if len(resBody.Data) == 0 || len(resBody.Data[0].Embedding) == 0 {
		return nil, errors.New("empty embeddings response")
	}

	return &resBody, nil
}

func (c *Client) getVectorizationResult(input []string, resBody *embeddingsResponse,
) (*modulecomponents.VectorizationResult[[]float32], int, error) {
	vectors := make([][]float32, len(resBody.Data))
	for i, data := range resBody.Data {
		vectors[i] = data.Embedding
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     vectors,
	}, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (c *Client) getVoyageAIUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Voyageai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return c.urlBuilder.URL(passedBaseURL)
}

func (c *Client) getErrorMessage(statusCode int, resBodyError string) string {
	if resBodyError != "" {
		return fmt.Sprintf("connection to VoyageAI failed with status: %d error: %v", statusCode, resBodyError)
	}
	return fmt.Sprintf("connection to VoyageAI failed with status: %d", statusCode)
}

func (c *Client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Voyageai-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-VoyageAI-Api-Key " +
		"nor in environment variable under VOYAGEAI_APIKEY")
}

func (c *Client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client) GetVectorizerRateLimit(ctx context.Context, modelRL VoyageRLModel) *modulecomponents.RateLimits {
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
