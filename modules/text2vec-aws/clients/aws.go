//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
)

func buildBedrockUrl(service, region, model string) string {
	if strings.Split(model, ".")[0] == "cohere" {
		service += "-runtime"
	}
	urlTemplate := "https://%s.%s.amazonaws.com/model/%s/invoke"
	return fmt.Sprintf(urlTemplate, service, region, model)
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

func New(awsAccessKey string, awsSecret string, logger logrus.FieldLogger) *aws {
	return &aws{
		awsAccessKey: awsAccessKey,
		awsSecret:    awsSecret,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		buildBedrockUrlFn:   buildBedrockUrl,
		buildSagemakerUrlFn: buildSagemakerUrl,
		logger:              logger,
	}
}

func (v *aws) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *aws) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *aws) vectorize(ctx context.Context, input []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
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
		
		req, err := createRequestBody(model, input)
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

	res, err := v.httpClient.Do(req)
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

func (v *aws) parseBedrockResponse(bodyBytes []byte, res *http.Response, input []string) (*ent.VectorizationResult, error) {
	var resBodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBodyMap); err != nil {
        return nil, errors.Wrap(err, "unmarshal response body")
    }
	
	// if resBodyMap has inputTextTokenCount, it's a resonse from an Amazon model
	// otherwise, it is a response from a Cohere model
	var resBody bedrockEmbeddingResponse
	if _, ok := resBodyMap["inputTextTokenCount"]; ok {
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return nil, errors.Wrap(err, "unmarshal response body")
		}
	} else {
		// Cohere's response does not give a token count, so we set it to 0
		resBody.InputTextTokenCount = 0
		
		embeddingsInterface, _ := resBodyMap["embeddings"].([]interface{})
		firstEmbeddingInterface, _ := embeddingsInterface[0].([]interface{})

		firstEmbeddingFloat := make([]float32, len(firstEmbeddingInterface))
		for i, v := range firstEmbeddingInterface {
			if val, ok := v.(float64); ok {
				firstEmbeddingFloat[i] = float32(val)
			} else {
				return nil, fmt.Errorf("expected the elements of the first 'embeddings' to be float64")
			}
		}
		
		resBody.Embedding = firstEmbeddingFloat

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
		Dimensions: len(resBody.Embedding),
		Vector:     resBody.Embedding,
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
	if len(v.awsAccessKey) > 0 {
		return v.awsAccessKey, nil
	}
	awsAccessKey := ctx.Value("X-Aws-Access-Key")
	if awsAccessKeyHeader, ok := awsAccessKey.([]string); ok &&
		len(awsAccessKeyHeader) > 0 && len(awsAccessKeyHeader[0]) > 0 {
		return awsAccessKeyHeader[0], nil
	}
	return "", errors.New("no access key found " +
		"neither in request header: X-Aws-Access-Key " +
		"nor in environment variable under AWS_ACCESS_KEY_ID")
}

func (v *aws) getAwsAccessSecret(ctx context.Context) (string, error) {
	if len(v.awsSecret) > 0 {
		return v.awsSecret, nil
	}
	awsSecretKey := ctx.Value("X-Aws-Access-Secret")
	if awsAccessSecretHeader, ok := awsSecretKey.([]string); ok &&
		len(awsAccessSecretHeader) > 0 && len(awsAccessSecretHeader[0]) > 0 {
		return awsAccessSecretHeader[0], nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-Aws-Access-Secret " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY")
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
	InputTextTokenCount int       `json:"InputTextTokenCount,omitempty"`
	Embedding           []float32 `json:"embedding,omitempty"`
	Message             *string   `json:"message,omitempty"`
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

func createRequestBody(model string, texts []string) (interface{}, error) {
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
        return bedrockCohereEmbeddingRequest{
            Texts:     texts,
            InputType: "search_document",
        }, nil
    default:
        return nil, fmt.Errorf("unknown model provider: %s", modelProvider)
    }
}