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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type operationType string

var (
	vectorizeObject operationType = "vectorize_object"
	vectorizeQuery  operationType = "vectorize_query"
)

func buildBedrockUrl(service, region, model string) string {
	serviceName := service
	if strings.HasPrefix(model, "cohere") {
		serviceName = fmt.Sprintf("%s-runtime", serviceName)
	}
	urlTemplate := "https://%s.%s.amazonaws.com/model/%s/invoke"
	return fmt.Sprintf(urlTemplate, serviceName, region, model)
}

func buildSagemakerUrl(service, region, endpoint string) string {
	urlTemplate := "https://runtime.%s.%s.amazonaws.com/endpoints/%s/invocations"
	return fmt.Sprintf(urlTemplate, service, region, endpoint)
}

type awsClient struct {
	awsAccessKey        string
	awsSecret           string
	awsSessionToken     string
	buildBedrockUrlFn   func(service, region, model string) string
	buildSagemakerUrlFn func(service, region, endpoint string) string
	httpClient          *http.Client
	logger              logrus.FieldLogger
}

func New(awsAccessKey, awsSecret, awsSessionToken string, timeout time.Duration, logger logrus.FieldLogger) *awsClient {
	return &awsClient{
		awsAccessKey:    awsAccessKey,
		awsSecret:       awsSecret,
		awsSessionToken: awsSessionToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildBedrockUrlFn:   buildBedrockUrl,
		buildSagemakerUrlFn: buildSagemakerUrl,
		logger:              logger,
	}
}

func (v *awsClient) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, vectorizeObject, config)
}

func (v *awsClient) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, vectorizeQuery, config)
}

func (v *awsClient) vectorize(ctx context.Context, input []string, operation operationType, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	accessKey, err := v.getAwsAccessKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Access Key")
	}
	secretKey, err := v.getAwsAccessSecret(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Secret Key")
	}
	awsSessionToken, err := v.getAwsSessionToken(ctx)
	if err != nil {
		return nil, err
	}
	maxRetries := 5

	service := v.getService(config)
	if v.isBedrock(service) {
		return v.sendBedrockRequest(ctx, input, operation, maxRetries, accessKey, secretKey, awsSessionToken, config)
	} else {
		return v.sendSagemakerRequest(ctx, input, maxRetries, accessKey, secretKey, awsSessionToken, config)
	}
}

func (v *awsClient) getConfig(ctx context.Context,
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

func (v *awsClient) sendBedrockRequest(ctx context.Context,
	input []string,
	operation operationType,
	maxRetries int,
	awsKey, awsSecret, awsSessionToken string,
	cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	model := cfg.Model
	region := cfg.Region

	req, err := createRequestBody(model, input, operation)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for model %s: %w", model, err)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request for model %s: %w", model, err)
	}

	sdkConfig, err := v.getConfig(ctx, awsKey, awsSecret, awsSessionToken, region, maxRetries)
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

	return v.parseBedrockResponse(result.Body, input)
}

func (v *awsClient) parseBedrockResponse(bodyBytes []byte, input []string) (*ent.VectorizationResult, error) {
	var resBody bedrockEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	if len(resBody.Embedding) == 0 && len(resBody.Embeddings) == 0 {
		return nil, fmt.Errorf("could not obtain vector from AWS Bedrock")
	}

	embedding := resBody.Embedding
	if len(resBody.Embeddings) > 0 {
		embedding = resBody.Embeddings[0]
	}

	return &ent.VectorizationResult{
		Text:       input[0],
		Dimensions: len(embedding),
		Vector:     embedding,
	}, nil
}

func (v *awsClient) sendSagemakerRequest(ctx context.Context,
	input []string,
	maxRetries int,
	awsKey, awsSecret, awsSessionToken string,
	cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	region := v.getRegion(cfg)
	endpoint := v.getEndpoint(cfg)

	body, err := json.Marshal(sagemakerEmbeddingsRequest{
		Inputs: input,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	sdkConfig, err := v.getConfig(ctx, awsKey, awsSecret, awsSessionToken, region, maxRetries)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	svc := sagemakerruntime.NewFromConfig(sdkConfig)
	result, err := svc.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{
		EndpointName: aws.String(endpoint),
		ContentType:  aws.String("application/json"),
		Body:         body,
	})
	if err != nil {
		return nil, fmt.Errorf("invoke request: %w", err)
	}

	return v.parseSagemakerResponse(result.Body, input)
}

func (v *awsClient) parseSagemakerResponse(bodyBytes []byte, input []string) (*ent.VectorizationResult, error) {
	var smEmbeddings [][]float32
	if err := json.Unmarshal(bodyBytes, &smEmbeddings); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if len(smEmbeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &ent.VectorizationResult{
		Text:       input[0],
		Dimensions: len(smEmbeddings[0]),
		Vector:     smEmbeddings[0],
	}, nil
}

func (v *awsClient) isBedrock(service string) bool {
	return service == "bedrock"
}

func (v *awsClient) getAwsAccessKey(ctx context.Context) (string, error) {
	if awsAccessKey := modulecomponents.GetValueFromContext(ctx, "X-Aws-Access-Key"); awsAccessKey != "" {
		return awsAccessKey, nil
	}
	if v.awsAccessKey != "" {
		return v.awsAccessKey, nil
	}
	return "", errors.New("no access key found " +
		"neither in request header: X-AWS-Access-Key " +
		"nor in environment variable under AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY")
}

func (v *awsClient) getAwsAccessSecret(ctx context.Context) (string, error) {
	if awsSecret := modulecomponents.GetValueFromContext(ctx, "X-Aws-Secret-Key"); awsSecret != "" {
		return awsSecret, nil
	}
	if v.awsSecret != "" {
		return v.awsSecret, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-AWS-Secret-Key " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY")
}

func (v *awsClient) getAwsSessionToken(ctx context.Context) (string, error) {
	if awsSessionToken := modulecomponents.GetValueFromContext(ctx, "X-Aws-Session-Token"); awsSessionToken != "" {
		return awsSessionToken, nil
	}
	if v.awsSessionToken != "" {
		return v.awsSessionToken, nil
	}
	return "", nil
}

func (v *awsClient) getRegion(config ent.VectorizationConfig) string {
	return config.Region
}

func (v *awsClient) getService(config ent.VectorizationConfig) string {
	return config.Service
}

func (v *awsClient) getEndpoint(config ent.VectorizationConfig) string {
	return config.Endpoint
}

type bedrockEmbeddingsRequest struct {
	InputText string `json:"inputText,omitempty"`
}

type bedrockCohereEmbeddingRequest struct {
	Texts     []string `json:"texts"`
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
type sagemakerEmbeddingResponse struct {
	Embedding          [][]float32 `json:"embedding,omitempty"`
	ErrorCode          *string     `json:"ErrorCode,omitempty"`
	LogStreamArn       *string     `json:"LogStreamArn,omitempty"`
	OriginalMessage    *string     `json:"OriginalMessage,omitempty"`
	Message            *string     `json:"Message,omitempty"`
	OriginalStatusCode *int        `json:"OriginalStatusCode,omitempty"`
}

func extractHostAndPath(endpointUrl string) (string, string, error) {
	u, err := url.Parse(endpointUrl)
	if err != nil {
		return "", "", err
	}

	if u.Host == "" || u.Path == "" {
		return "", "", fmt.Errorf("invalid endpoint URL: %s", endpointUrl)
	}

	return u.Host, u.Path, nil
}

func createRequestBody(model string, texts []string, operation operationType) (interface{}, error) {
	modelParts := strings.Split(model, ".")
	if len(modelParts) == 0 {
		return nil, fmt.Errorf("invalid model: %s", model)
	}

	modelProvider := modelParts[0]

	switch modelProvider {
	case "amazon":
		return bedrockEmbeddingsRequest{
			InputText: texts[0],
		}, nil
	case "cohere":
		inputType := "search_document"
		if operation == vectorizeQuery {
			inputType = "search_query"
		}
		return bedrockCohereEmbeddingRequest{
			Texts:     texts,
			InputType: inputType,
		}, nil
	default:
		return nil, fmt.Errorf("unknown model provider: %s", modelProvider)
	}
}
