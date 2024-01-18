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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
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

type aws struct {
	awsAccessKey        string
	awsSecret           string
	buildBedrockUrlFn   func(service, region, model string) string
	buildSagemakerUrlFn func(service, region, endpoint string) string
	httpClient          *http.Client
	logger              logrus.FieldLogger
}

func New(awsAccessKey string, awsSecret string, timeout time.Duration, logger logrus.FieldLogger) *aws {
	return &aws{
		awsAccessKey: awsAccessKey,
		awsSecret:    awsSecret,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildBedrockUrlFn:   buildBedrockUrl,
		buildSagemakerUrlFn: buildSagemakerUrl,
		logger:              logger,
	}
}

func (v *aws) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, vectorizeObject, config)
}

func (v *aws) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, vectorizeQuery, config)
}

func (v *aws) vectorize(ctx context.Context, input []string, operation operationType, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	service := v.getService(config)
	region := v.getRegion(config)
	model := v.getModel(config)
	endpoint := v.getEndpoint(config)
	targetModel := v.getTargetModel(config)
	targetVariant := v.getTargetVariant(config)

	var body []byte
	var endpointUrl string
	var host string
	var path string
	var err error

	headers := map[string]string{
		"accept":       "*/*",
		"content-type": contentType,
	}

	if v.isBedrock(service) {
		endpointUrl = v.buildBedrockUrlFn(service, region, model)
		host, path, _ = extractHostAndPath(endpointUrl)

		req, err := createRequestBody(model, input, operation)
		if err != nil {
			return nil, err
		}

		body, err = json.Marshal(req)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal body")
		}
	} else if v.isSagemaker(service) {
		endpointUrl = v.buildSagemakerUrlFn(service, region, endpoint)
		host = "runtime." + service + "." + region + ".amazonaws.com"
		path = "/endpoints/" + endpoint + "/invocations"
		if targetModel != "" {
			headers["x-amzn-sagemaker-target-model"] = targetModel
		}
		if targetVariant != "" {
			headers["x-amzn-sagemaker-target-variant"] = targetVariant
		}
		body, err = json.Marshal(sagemakerEmbeddingsRequest{
			TextInputs: input,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "marshal body")
		}
	} else {
		return nil, errors.Wrapf(err, "service error")
	}

	accessKey, err := v.getAwsAccessKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Access Key")
	}
	secretKey, err := v.getAwsAccessSecret(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Secret Key")
	}

	headers["host"] = host
	amzDate, headers, authorizationHeader := getAuthHeader(accessKey, secretKey, host, service, region, path, body, headers)
	headers["Authorization"] = authorizationHeader
	headers["x-amz-date"] = amzDate

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointUrl, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := v.makeRequest(req, 30, 5)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}
	if v.isBedrock(service) {
		return v.parseBedrockResponse(bodyBytes, res, input)
	} else {
		return v.parseSagemakerResponse(bodyBytes, res, input)
	}
}

func (v *aws) makeRequest(req *http.Request, delayInSeconds int, maxRetries int) (*http.Response, error) {
	var res *http.Response
	var err error

	// Generate a UUID for this request
	requestID := uuid.New().String()

	for i := 0; i < maxRetries; i++ {
		res, err = v.httpClient.Do(req)
		if err != nil {
			return nil, errors.Wrap(err, "send POST request")
		}

		// If the status code is not 429 or 400, break the loop
		if res.StatusCode != http.StatusTooManyRequests && res.StatusCode != http.StatusBadRequest {
			break
		}

		v.logger.Debugf("Request ID %s to %s returned 429, retrying in %d seconds", requestID, req.URL, delayInSeconds)

		// Sleep for a while and then continue to the next iteration
		time.Sleep(time.Duration(delayInSeconds) * time.Second)

		// Double the delay for the next iteration
		delayInSeconds *= 2

	}

	return res, err
}

func (v *aws) parseBedrockResponse(bodyBytes []byte, res *http.Response, input []string) (*ent.VectorizationResult, error) {
	var resBodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBodyMap); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	// if resBodyMap has inputTextTokenCount, it's a resonse from an Amazon model
	// otherwise, it is a response from a Cohere model
	var resBody bedrockEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Message != nil {
		if resBody.Message != nil {
			return nil, fmt.Errorf("connection to AWS Bedrock failed with status: %v error: %s",
				res.StatusCode, *resBody.Message)
		}
		return nil, fmt.Errorf("connection to AWS Bedrock failed with status: %d", res.StatusCode)
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

func (v *aws) parseSagemakerResponse(bodyBytes []byte, res *http.Response, input []string) (*ent.VectorizationResult, error) {
	var resBody sagemakerEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Message != nil {
		if resBody.Message != nil {
			return nil, fmt.Errorf("connection to AWS failed with status: %v error: %s",
				res.StatusCode, *resBody.Message)
		}
		return nil, fmt.Errorf("connection to AWS failed with status: %d", res.StatusCode)
	}

	if len(resBody.Embedding) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &ent.VectorizationResult{
		Text:       input[0],
		Dimensions: len(resBody.Embedding[0]),
		Vector:     resBody.Embedding[0],
	}, nil
}

func (v *aws) isSagemaker(service string) bool {
	return service == "sagemaker"
}

func (v *aws) isBedrock(service string) bool {
	return service == "bedrock"
}

func (v *aws) getAwsAccessKey(ctx context.Context) (string, error) {
	awsAccessKey := ctx.Value("X-Aws-Access-Key")
	if awsAccessKeyHeader, ok := awsAccessKey.([]string); ok &&
		len(awsAccessKeyHeader) > 0 && len(awsAccessKeyHeader[0]) > 0 {
		return awsAccessKeyHeader[0], nil
	}
	if len(v.awsAccessKey) > 0 {
		return v.awsAccessKey, nil
	}
	return "", errors.New("no access key found " +
		"neither in request header: X-AWS-Access-Key " +
		"nor in environment variable under AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY")
}

func (v *aws) getAwsAccessSecret(ctx context.Context) (string, error) {
	awsSecretKey := ctx.Value("X-Aws-Secret-Key")
	if awsAccessSecretHeader, ok := awsSecretKey.([]string); ok &&
		len(awsAccessSecretHeader) > 0 && len(awsAccessSecretHeader[0]) > 0 {
		return awsAccessSecretHeader[0], nil
	}
	if len(v.awsSecret) > 0 {
		return v.awsSecret, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-AWS-Secret-Key " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY")
}

func (v *aws) getModel(config ent.VectorizationConfig) string {
	return config.Model
}

func (v *aws) getRegion(config ent.VectorizationConfig) string {
	return config.Region
}

func (v *aws) getService(config ent.VectorizationConfig) string {
	return config.Service
}

func (v *aws) getEndpoint(config ent.VectorizationConfig) string {
	return config.Endpoint
}

func (v *aws) getTargetModel(config ent.VectorizationConfig) string {
	return config.TargetModel
}

func (v *aws) getTargetVariant(config ent.VectorizationConfig) string {
	return config.TargetVariant
}

type bedrockEmbeddingsRequest struct {
	InputText string `json:"inputText,omitempty"`
}

type bedrockCohereEmbeddingRequest struct {
	Texts     []string `json:"texts"`
	InputType string   `json:"input_type"`
}

type sagemakerEmbeddingsRequest struct {
	TextInputs []string `json:"text_inputs,omitempty"`
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
