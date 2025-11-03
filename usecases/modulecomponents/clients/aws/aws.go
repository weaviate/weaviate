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

package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type bedrockEmbeddingsRequest struct {
	InputText       *string                         `json:"inputText,omitempty"`
	InputImage      *string                         `json:"inputImage,omitempty"`
	EmbeddingConfig *bedrockEmbeddingsRequestConfig `json:"embeddingConfig,omitempty"`
}

type bedrockEmbeddingsRequestConfig struct {
	OutputEmbeddingLength *int64 `json:"outputEmbeddingLength,omitempty"`
}

type bedrockAmazonNovaEmbeddingsRequest struct {
	TaskType              TaskType                                `json:"taskType"`
	SingleEmbeddingParams bedrockAmazonNovaSingleEmbeddingsParams `json:"singleEmbeddingParams"`
}

type bedrockAmazonNovaSingleEmbeddingsParams struct {
	EmbeddingPurpose   EmbeddingPurpose        `json:"embeddingPurpose,omitempty"`
	EmbeddingDimension *int64                  `json:"embeddingDimension,omitempty"`
	Text               *bedrockAmazonNovaText  `json:"text,omitempty"`
	Image              *bedrockAmazonNovaImage `json:"image,omitempty"`
}

type bedrockAmazonNovaText struct {
	TruncationMode *string `json:"truncationMode,omitempty"`
	Value          string  `json:"value,omitempty"`
}

type bedrockAmazonNovaImage struct {
	Format *string                      `json:"format,omitempty"`
	Source bedrockAmazonNovaImageSource `json:"source"`
}

type bedrockAmazonNovaImageSource struct {
	Bytes string `json:"bytes,omitempty"`
}

type bedrockCohereEmbeddingRequest struct {
	Texts     []string `json:"texts,omitempty"`
	Images    []string `json:"images,omitempty"`
	InputType string   `json:"input_type"`
}

type sagemakerEmbeddingsRequest struct {
	Inputs []string `json:"inputs,omitempty"`
}

type bedrockEmbeddingResponse struct {
	InputTextTokenCount int         `json:"InputTextTokenCount,omitempty"`
	Embedding           []float32   `json:"embedding,omitempty"`
	Embeddings          [][]float32 `json:"embeddings,omitempty"`
	Message             *string     `json:"message,omitempty"`
}

type bedrockNovaEmbedding struct {
	EmbeddingType string    `json:"embeddingType,omitempty"`
	Embedding     []float32 `json:"embedding,omitempty"`
}

// UnmarshalJSON implements custom unmarshaling for bedrockEmbeddingResponse.
func (b *bedrockEmbeddingResponse) UnmarshalJSON(data []byte) error {
	type tempResponse struct {
		InputTextTokenCount int             `json:"inputTextTokenCount,omitempty"`
		Embedding           []float32       `json:"embedding,omitempty"`
		Embeddings          json.RawMessage `json:"embeddings,omitempty"`
		Message             *string         `json:"message,omitempty"`
	}

	var t tempResponse
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	b.InputTextTokenCount = t.InputTextTokenCount
	b.Embedding = t.Embedding
	b.Message = t.Message

	// Handle embeddings (if present): try [][]float32 first, then fall back to []NovaEmbeddings.
	if len(t.Embeddings) > 0 {
		var embeddings [][]float32
		if err := json.Unmarshal(t.Embeddings, &embeddings); err == nil {
			b.Embeddings = embeddings
			return nil
		}

		var novaEmbeddings []bedrockNovaEmbedding
		if err := json.Unmarshal(t.Embeddings, &novaEmbeddings); err != nil {
			return err
		}

		b.Embeddings = make([][]float32, len(novaEmbeddings))
		for i, e := range novaEmbeddings {
			b.Embeddings[i] = e.Embedding
		}
	}

	return nil
}

func (b *bedrockEmbeddingResponse) getEmbeddings() [][]float32 {
	if len(b.Embedding) > 0 {
		return [][]float32{b.Embedding}
	}
	if len(b.Embeddings) == 1 {
		return b.Embeddings
	}
	return nil
}

type OperationType string

const (
	Document OperationType = "document"
	Query    OperationType = "query"
)

type Service string

const (
	Bedrock   Service = "bedrock"
	Sagemaker Service = "sagemaker"
)

type TaskType string

const (
	SingleEmbedding TaskType = "SINGLE_EMBEDDING"
)

type EmbeddingPurpose string

const (
	GenericIndex     EmbeddingPurpose = "GENERIC_INDEX"
	GenericRetrieval EmbeddingPurpose = "GENERIC_RETRIEVAL"
)

type Settings struct {
	Model            string
	Region           string
	Endpoint         string
	Service          Service
	Dimensions       *int64
	OperationType    OperationType
	EmbeddingPurpose EmbeddingPurpose
}

type Client struct {
	awsAccessKey    string
	awsSecret       string
	awsSessionToken string
	httpClient      *http.Client
	logger          logrus.FieldLogger
}

func New(awsAccessKey, awsSecret, awsSessionToken string,
	timeout time.Duration,
	logger logrus.FieldLogger,
) *Client {
	return &Client{
		awsAccessKey:    awsAccessKey,
		awsSecret:       awsSecret,
		awsSessionToken: awsSessionToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *Client) Vectorize(ctx context.Context,
	texts []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return c.vectorizeText(ctx, texts, settings)
}

func (c *Client) VectorizeMultiModal(ctx context.Context,
	texts, images []string, settings Settings,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return c.vectorizeMultiModal(ctx, texts, images, settings)
}

func (c *Client) vectorizeText(ctx context.Context, texts []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	textEmbeddings, _, err := c.vectorize(ctx, texts, nil, settings)
	if err != nil {
		return nil, err
	}
	dimensions := 0
	if len(textEmbeddings) > 0 {
		dimensions = len(textEmbeddings[0])
	}
	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       texts,
		Dimensions: dimensions,
		Vector:     textEmbeddings,
	}, nil
}

func (c *Client) vectorizeMultiModal(ctx context.Context, texts, images []string, settings Settings,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	var textEmbeddings, imageEmbeddings [][]float32
	var err error
	if len(texts) > 0 {
		textEmbeddings, _, err = c.vectorize(ctx, texts, nil, settings)
		if err != nil {
			return nil, err
		}
	}
	if len(images) > 0 {
		_, imageEmbeddings, err = c.vectorize(ctx, nil, images, settings)
		if err != nil {
			return nil, err
		}
	}
	return &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors:  textEmbeddings,
		ImageVectors: imageEmbeddings,
	}, nil
}

func (c *Client) vectorize(ctx context.Context, texts, images []string, settings Settings,
) ([][]float32, [][]float32, error) {
	accessKey, err := c.getAwsAccessKey(ctx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "AWS Access Key")
	}
	secretKey, err := c.getAwsAccessSecret(ctx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "AWS Secret Key")
	}
	awsSessionToken, err := c.getAwsSessionToken(ctx)
	if err != nil {
		return nil, nil, err
	}
	maxRetries := 5

	if settings.Service == Bedrock {
		return c.sendBedrockRequest(ctx, texts, images, accessKey, secretKey, awsSessionToken, maxRetries, settings)
	} else {
		return c.sendSagemakerRequest(ctx, texts, accessKey, secretKey, awsSessionToken, maxRetries, settings)
	}
}

func (c *Client) sendBedrockRequest(ctx context.Context,
	texts, images []string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	settings Settings,
) ([][]float32, [][]float32, error) {
	model := settings.Model
	if strings.Contains(model, "amazon.") {
		var textEmbeddings, imageEmbeddings [][]float32
		for i := range texts {
			embeddings, err := c.invokeAmazonModel(ctx, texts[i], "", awsKey, awsSecret, awsSessionToken, maxRetries, settings)
			if err != nil {
				return nil, nil, fmt.Errorf("amazon: %w", err)
			}
			textEmbeddings = append(textEmbeddings, embeddings)
		}
		for i := range images {
			embeddings, err := c.invokeAmazonModel(ctx, "", images[i], awsKey, awsSecret, awsSessionToken, maxRetries, settings)
			if err != nil {
				return nil, nil, fmt.Errorf("amazon: %w", err)
			}
			imageEmbeddings = append(imageEmbeddings, embeddings)
		}
		return textEmbeddings, imageEmbeddings, nil
	} else if strings.Contains(model, "cohere.") {
		var textEmbeddings, imageEmbeddings [][]float32
		var err error
		if len(texts) > 0 {
			textEmbeddings, err = c.invokeCohereModel(ctx, texts, nil, awsKey, awsSecret, awsSessionToken, maxRetries, settings)
			if err != nil {
				return nil, nil, fmt.Errorf("cohere: %w", err)
			}
		}
		if len(images) > 0 {
			imageEmbeddings, err = c.invokeCohereModel(ctx, nil, images, awsKey, awsSecret, awsSessionToken, maxRetries, settings)
			if err != nil {
				return nil, nil, fmt.Errorf("cohere: %w", err)
			}
		}
		return textEmbeddings, imageEmbeddings, nil
	} else {
		return nil, nil, fmt.Errorf("unknown model provider: %s", model)
	}
}

func (c *Client) invokeAmazonModel(ctx context.Context,
	text, image string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	settings Settings,
) ([]float32, error) {
	var req any
	if strings.Contains(settings.Model, "amazon.nova") {
		req = c.createAmazonNovaBody(text, image, settings)
	} else {
		req = c.createAmazonTitanBody(text, image, settings)
	}
	result, err := c.invokeModel(ctx, req, awsKey, awsSecret, awsSessionToken, maxRetries, settings)
	if err != nil {
		return nil, fmt.Errorf("invoke model: %w", err)
	}
	embeddings, err := c.parseBedrockAmazonResponse(result.Body)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return embeddings, nil
}

func (c *Client) invokeCohereModel(ctx context.Context,
	texts, images []string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	settings Settings,
) ([][]float32, error) {
	req := c.createCohereBody(texts, images, settings)
	result, err := c.invokeModel(ctx, req, awsKey, awsSecret, awsSessionToken, maxRetries, settings)
	if err != nil {
		return nil, fmt.Errorf("invoke model: %w", err)
	}
	return c.parseBedrockCohereResponse(result.Body)
}

func (c *Client) createAmazonTitanBody(text, image string, settings Settings) bedrockEmbeddingsRequest {
	var embeddingConfig *bedrockEmbeddingsRequestConfig
	if settings.Dimensions != nil {
		embeddingConfig = &bedrockEmbeddingsRequestConfig{OutputEmbeddingLength: settings.Dimensions}
	}
	return bedrockEmbeddingsRequest{
		InputText:       c.ptrString(text),
		InputImage:      c.ptrString(image),
		EmbeddingConfig: embeddingConfig,
	}
}

func (c *Client) createAmazonNovaBody(text, image string, settings Settings) bedrockAmazonNovaEmbeddingsRequest {
	var textRequest *bedrockAmazonNovaText
	if text != "" {
		textRequest = &bedrockAmazonNovaText{
			TruncationMode: c.ptrString("END"),
			Value:          text,
		}
	}
	var imageRequest *bedrockAmazonNovaImage
	if image != "" {
		imageRequest = &bedrockAmazonNovaImage{
			Format: c.ptrString("jpeg"),
			Source: bedrockAmazonNovaImageSource{Bytes: image},
		}
	}
	return bedrockAmazonNovaEmbeddingsRequest{
		TaskType: SingleEmbedding,
		SingleEmbeddingParams: bedrockAmazonNovaSingleEmbeddingsParams{
			EmbeddingPurpose:   settings.EmbeddingPurpose,
			EmbeddingDimension: settings.Dimensions,
			Text:               textRequest,
			Image:              imageRequest,
		},
	}
}

func (c *Client) createCohereBody(texts, images []string, settings Settings) bedrockCohereEmbeddingRequest {
	inputType := "search_document"
	if settings.OperationType == Query {
		inputType = "search_query"
	}
	return bedrockCohereEmbeddingRequest{
		Texts:     texts,
		Images:    images,
		InputType: inputType,
	}
}

func (c *Client) ptrString(in string) *string {
	if in != "" {
		return &in
	}
	return nil
}

func (c *Client) invokeModel(ctx context.Context,
	req any,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	settings Settings,
) (*bedrockruntime.InvokeModelOutput, error) {
	model := settings.Model
	region := settings.Region

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request for model %s: %w", model, err)
	}

	sdkConfig, err := c.getConfig(ctx, awsKey, awsSecret, awsSessionToken, region, maxRetries)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	client := bedrockruntime.NewFromConfig(sdkConfig)
	result, err := client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(model),
		ContentType: aws.String("application/json"),
		Body:        body,
	})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such host") {
			return nil, fmt.Errorf("Bedrock service is not available in the selected region. " +
				"Please double-check the service availability for your region at " +
				"https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/")
		} else if strings.Contains(errMsg, "Could not resolve the foundation model") {
			return nil, fmt.Errorf("could not resolve the foundation model from model identifier: \"%v\". "+
				"Please verify that the requested model exists and is accessible within the specified region", model)
		} else {
			return nil, fmt.Errorf("couldn't invoke %s model: %w", model, err)
		}
	}
	return result, nil
}

func (c *Client) parseBedrockCohereResponse(bodyBytes []byte) ([][]float32, error) {
	var resBody bedrockEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	if len(resBody.Embeddings) == 0 {
		return nil, fmt.Errorf("could not obtain vector from AWS Bedrock")
	}
	return resBody.Embeddings, nil
}

func (c *Client) parseBedrockAmazonResponse(bodyBytes []byte) ([]float32, error) {
	var resBody bedrockEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	if len(resBody.getEmbeddings()) == 0 {
		return nil, fmt.Errorf("could not obtain vector from AWS Bedrock")
	}
	return resBody.getEmbeddings()[0], nil
}

func (c *Client) sendSagemakerRequest(ctx context.Context,
	texts []string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	settings Settings,
) ([][]float32, [][]float32, error) {
	region := settings.Region
	endpoint := settings.Endpoint

	body, err := json.Marshal(sagemakerEmbeddingsRequest{
		Inputs: texts,
	})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "marshal body")
	}

	sdkConfig, err := c.getConfig(ctx, awsKey, awsSecret, awsSessionToken, region, maxRetries)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	svc := sagemakerruntime.NewFromConfig(sdkConfig)
	result, err := svc.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{
		EndpointName: aws.String(endpoint),
		ContentType:  aws.String("application/json"),
		Body:         body,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("invoke request: %w", err)
	}

	return c.parseSagemakerResponse(result.Body)
}

func (c *Client) parseSagemakerResponse(bodyBytes []byte) ([][]float32, [][]float32, error) {
	var smEmbeddings [][]float32
	if err := json.Unmarshal(bodyBytes, &smEmbeddings); err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	if len(smEmbeddings) == 0 {
		return nil, nil, errors.Errorf("empty embeddings response")
	}
	return smEmbeddings, nil, nil
}

func (c *Client) getConfig(ctx context.Context,
	awsKey, awsSecret, awsSessionToken string,
	region string,
	maxRetries int,
) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(awsKey, awsSecret, awsSessionToken),
		),
		config.WithRetryMaxAttempts(maxRetries),
	)
}

func (c *Client) getAwsAccessKey(ctx context.Context) (string, error) {
	if awsAccessKey := modulecomponents.GetValueFromContext(ctx, "X-Aws-Access-Key"); awsAccessKey != "" {
		return awsAccessKey, nil
	}
	if c.awsAccessKey != "" {
		return c.awsAccessKey, nil
	}
	return "", errors.New("no access key found " +
		"neither in request header: X-AWS-Access-Key " +
		"nor in environment variable under AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY")
}

func (c *Client) getAwsAccessSecret(ctx context.Context) (string, error) {
	if awsSecret := modulecomponents.GetValueFromContext(ctx, "X-Aws-Secret-Key"); awsSecret != "" {
		return awsSecret, nil
	}
	if c.awsSecret != "" {
		return c.awsSecret, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-AWS-Secret-Key " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY")
}

func (c *Client) getAwsSessionToken(ctx context.Context) (string, error) {
	if awsSessionToken := modulecomponents.GetValueFromContext(ctx, "X-Aws-Session-Token"); awsSessionToken != "" {
		return awsSessionToken, nil
	}
	if c.awsSessionToken != "" {
		return c.awsSessionToken, nil
	}
	return "", nil
}
