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
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-aws/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildBedrockUrl(service, region, model string) string {
	urlTemplate := "https://%s.%s.amazonaws.com/model/%s/invoke"
	return fmt.Sprintf(urlTemplate, fmt.Sprintf("%s-runtime", service), region, model)
}

func buildSagemakerUrl(service, region, endpoint string) string {
	urlTemplate := "https://runtime.%s.%s.amazonaws.com/endpoints/%s/invocations"
	return fmt.Sprintf(urlTemplate, service, region, endpoint)
}

type aws struct {
	awsAccessKey        string
	awsSecretKey        string
	buildBedrockUrlFn   func(service, region, model string) string
	buildSagemakerUrlFn func(service, region, endpoint string) string
	httpClient          *http.Client
	logger              logrus.FieldLogger
}

func New(awsAccessKey string, awsSecretKey string, timeout time.Duration, logger logrus.FieldLogger) *aws {
	return &aws{
		awsAccessKey: awsAccessKey,
		awsSecretKey: awsSecretKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildBedrockUrlFn:   buildBedrockUrl,
		buildSagemakerUrlFn: buildSagemakerUrl,
		logger:              logger,
	}
}

func (v *aws) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *aws) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *aws) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)
	service := settings.Service()
	region := settings.Region()
	model := settings.Model()
	endpoint := settings.Endpoint()
	targetModel := settings.TargetModel()
	targetVariant := settings.TargetVariant()

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
		host = service + "-runtime" + "." + region + ".amazonaws.com"
		path = "/model/" + model + "/invoke"

		if v.isAmazonModel(model) {
			body, err = json.Marshal(bedrockAmazonGenerateRequest{
				InputText: prompt,
			})
		} else if v.isAnthropicModel(model) {
			var builder strings.Builder
			builder.WriteString("\n\nHuman: ")
			builder.WriteString(prompt)
			builder.WriteString("\n\nAssistant:")
			body, err = json.Marshal(bedrockAnthropicGenerateRequest{
				Prompt:            builder.String(),
				MaxTokensToSample: *settings.MaxTokenCount(),
				Temperature:       *settings.Temperature(),
				TopK:              *settings.TopK(),
				TopP:              settings.TopP(),
				StopSequences:     settings.StopSequences(),
				AnthropicVersion:  "bedrock-2023-05-31",
			})
		} else if v.isAI21Model(model) {
			body, err = json.Marshal(bedrockAI21GenerateRequest{
				Prompt:        prompt,
				MaxTokens:     *settings.MaxTokenCount(),
				Temperature:   *settings.Temperature(),
				TopP:          settings.TopP(),
				StopSequences: settings.StopSequences(),
			})
		} else if v.isCohereModel(model) {
			body, err = json.Marshal(bedrockCohereRequest{
				Prompt:      prompt,
				Temperature: *settings.Temperature(),
				MaxTokens:   *settings.MaxTokenCount(),
				// ReturnLikeliHood: "GENERATION", // contray to docs, this is invalid
			})
		}

		headers["x-amzn-bedrock-save"] = "false"
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
		body, err = json.Marshal(sagemakerGenerateRequest{
			Prompt: prompt,
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
		return v.parseBedrockResponse(bodyBytes, res)
	} else if v.isSagemaker(service) {
		return v.parseSagemakerResponse(bodyBytes, res)
	} else {
		return &generativemodels.GenerateResponse{
			Result: nil,
		}, nil
	}
}

func (v *aws) parseBedrockResponse(bodyBytes []byte, res *http.Response) (*generativemodels.GenerateResponse, error) {
	var resBodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBodyMap); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	var resBody bedrockGenerateResponse
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

	if len(resBody.Results) == 0 && len(resBody.Generations) == 0 {
		return nil, fmt.Errorf("received empty response from AWS Bedrock")
	}

	var content string
	if len(resBody.Results) > 0 && len(resBody.Results[0].CompletionReason) > 0 {
		content = resBody.Results[0].OutputText
	} else if len(resBody.Generations) > 0 {
		content = resBody.Generations[0].Text
	}

	if content != "" {
		return &generativemodels.GenerateResponse{
			Result: &content,
		}, nil
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *aws) parseSagemakerResponse(bodyBytes []byte, res *http.Response) (*generativemodels.GenerateResponse, error) {
	var resBody sagemakerGenerateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Message != nil {
		if resBody.Message != nil {
			return nil, fmt.Errorf("connection to AWS Sagemaker failed with status: %v error: %s",
				res.StatusCode, *resBody.Message)
		}
		return nil, fmt.Errorf("connection to AWS Sagemaker failed with status: %d", res.StatusCode)
	}

	if len(resBody.Generations) == 0 {
		return nil, fmt.Errorf("received empty response from AWS Sagemaker")
	}

	if len(resBody.Generations) > 0 && len(resBody.Generations[0].Id) > 0 {
		content := resBody.Generations[0].Text
		if content != "" {
			return &generativemodels.GenerateResponse{
				Result: &content,
			}, nil
		}
	}
	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *aws) isSagemaker(service string) bool {
	return service == "sagemaker"
}

func (v *aws) isBedrock(service string) bool {
	return service == "bedrock"
}

func (v *aws) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *aws) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
	all := compile.FindAll([]byte(prompt), -1)
	for _, match := range all {
		originalProperty := string(match)
		replacedProperty := compile.FindStringSubmatch(originalProperty)[1]
		replacedProperty = strings.TrimSpace(replacedProperty)
		value := textProperties[replacedProperty]
		if value == "" {
			return "", errors.Errorf("Following property has empty value: '%v'. Make sure you spell the property name correctly, verify that the property exists and has a value", replacedProperty)
		}
		prompt = strings.ReplaceAll(prompt, originalProperty, value)
	}
	return prompt, nil
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
	awsAccessSecret := ctx.Value("X-Aws-Secret-Key")
	if awsAccessSecretHeader, ok := awsAccessSecret.([]string); ok &&
		len(awsAccessSecretHeader) > 0 && len(awsAccessSecretHeader[0]) > 0 {
		return awsAccessSecretHeader[0], nil
	}
	if len(v.awsSecretKey) > 0 {
		return v.awsSecretKey, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-Aws-Secret-Key " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY")
}

func (v *aws) isAmazonModel(model string) bool {
	return strings.Contains(model, "amazon")
}

func (v *aws) isAI21Model(model string) bool {
	return strings.Contains(model, "ai21")
}

func (v *aws) isAnthropicModel(model string) bool {
	return strings.Contains(model, "anthropic")
}

func (v *aws) isCohereModel(model string) bool {
	return strings.Contains(model, "cohere")
}

type bedrockAmazonGenerateRequest struct {
	InputText            string                `json:"inputText,omitempty"`
	TextGenerationConfig *textGenerationConfig `json:"textGenerationConfig,omitempty"`
}

type bedrockAnthropicGenerateRequest struct {
	Prompt            string   `json:"prompt,omitempty"`
	MaxTokensToSample int      `json:"max_tokens_to_sample,omitempty"`
	Temperature       float64  `json:"temperature,omitempty"`
	TopK              int      `json:"top_k,omitempty"`
	TopP              *float64 `json:"top_p,omitempty"`
	StopSequences     []string `json:"stop_sequences,omitempty"`
	AnthropicVersion  string   `json:"anthropic_version,omitempty"`
}

type bedrockAI21GenerateRequest struct {
	Prompt           string   `json:"prompt,omitempty"`
	MaxTokens        int      `json:"maxTokens,omitempty"`
	Temperature      float64  `json:"temperature,omitempty"`
	TopP             *float64 `json:"top_p,omitempty"`
	StopSequences    []string `json:"stop_sequences,omitempty"`
	CountPenalty     penalty  `json:"countPenalty,omitempty"`
	PresencePenalty  penalty  `json:"presencePenalty,omitempty"`
	FrequencyPenalty penalty  `json:"frequencyPenalty,omitempty"`
}
type bedrockCohereRequest struct {
	Prompt           string  `json:"prompt,omitempty"`
	MaxTokens        int     `json:"max_tokens,omitempty"`
	Temperature      float64 `json:"temperature,omitempty"`
	ReturnLikeliHood string  `json:"return_likelihood,omitempty"`
}

type penalty struct {
	Scale int `json:"scale,omitempty"`
}

type sagemakerGenerateRequest struct {
	Prompt string `json:"prompt,omitempty"`
}

type textGenerationConfig struct {
	MaxTokenCount int      `json:"maxTokenCount"`
	StopSequences []string `json:"stopSequences"`
	Temperature   float64  `json:"temperature"`
	TopP          int      `json:"topP"`
}

type bedrockGenerateResponse struct {
	InputTextTokenCount int                 `json:"InputTextTokenCount,omitempty"`
	Results             []Result            `json:"results,omitempty"`
	Generations         []BedrockGeneration `json:"generations,omitempty"`
	Message             *string             `json:"message,omitempty"`
}

type sagemakerGenerateResponse struct {
	Generations []Generation `json:"generations,omitempty"`
	Message     *string      `json:"message,omitempty"`
}

type Generation struct {
	Id   string `json:"id,omitempty"`
	Text string `json:"text,omitempty"`
}

type BedrockGeneration struct {
	Id           string `json:"id,omitempty"`
	Text         string `json:"text,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
}

type Result struct {
	TokenCount       int    `json:"tokenCount,omitempty"`
	OutputText       string `json:"outputText,omitempty"`
	CompletionReason string `json:"completionReason,omitempty"`
}
