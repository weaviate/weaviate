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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativeconfig "github.com/weaviate/weaviate/modules/generative-aws/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
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

type awsClient struct {
	awsAccessKey        string
	awsSecretKey        string
	awsSessionToken     string
	buildBedrockUrlFn   func(service, region, model string) string
	buildSagemakerUrlFn func(service, region, endpoint string) string
	httpClient          *http.Client
	logger              logrus.FieldLogger
}

func New(awsAccessKey, awsSecretKey, awsSessionToken string, timeout time.Duration, logger logrus.FieldLogger) *awsClient {
	return &awsClient{
		awsAccessKey:    awsAccessKey,
		awsSecretKey:    awsSecretKey,
		awsSessionToken: awsSessionToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildBedrockUrlFn:   buildBedrockUrl,
		buildSagemakerUrlFn: buildSagemakerUrl,
		logger:              logger,
	}
}

func (v *awsClient) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *awsClient) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *awsClient) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := generativeconfig.NewClassSettings(cfg)
	service := settings.Service()

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

	if v.isBedrock(service) {
		return v.sendBedrockRequest(ctx,
			prompt,
			accessKey, secretKey, awsSessionToken, maxRetries,
			cfg,
		)
	} else if v.isSagemaker(service) {
		var body []byte
		var endpointUrl string
		var host string
		var path string
		var err error

		region := settings.Region()
		endpoint := settings.Endpoint()
		targetModel := settings.TargetModel()
		targetVariant := settings.TargetVariant()

		endpointUrl = v.buildSagemakerUrlFn(service, region, endpoint)
		host = "runtime." + service + "." + region + ".amazonaws.com"
		path = "/endpoints/" + endpoint + "/invocations"

		headers := map[string]string{
			"accept":       "*/*",
			"content-type": contentType,
		}

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

		return v.parseSagemakerResponse(bodyBytes, res)
	} else {
		return &generativemodels.GenerateResponse{
			Result: nil,
		}, nil
	}
}

func (v *awsClient) sendBedrockRequest(
	ctx context.Context,
	prompt string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	cfg moduletools.ClassConfig,
) (*generativemodels.GenerateResponse, error) {
	settings := generativeconfig.NewClassSettings(cfg)
	model := settings.Model()
	region := settings.Region()
	req, err := v.createRequestBody(prompt, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for model %s: %w", model, err)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request for model %s: %w", model, err)
	}

	sdkConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(awsKey, awsSecret, awsSessionToken),
		),
		config.WithRetryMaxAttempts(maxRetries),
	)
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
			return nil, fmt.Errorf("Could not resolve the foundation model from model identifier: \"%v\". "+
				"Please verify that the requested model exists and is accessible within the specified region", model)
		} else {
			return nil, fmt.Errorf("Couldn't invoke %s model: %w", model, err)
		}
	}

	return v.parseBedrockResponse(result.Body, model)
}

func (v *awsClient) createRequestBody(prompt string, cfg moduletools.ClassConfig) (interface{}, error) {
	settings := generativeconfig.NewClassSettings(cfg)
	model := settings.Model()
	if v.isAmazonModel(model) {
		return bedrockAmazonGenerateRequest{
			InputText: prompt,
		}, nil
	} else if v.isAnthropicClaude3Model(model) {
		return bedrockAnthropicClaude3Request{
			AnthropicVersion: "bedrock-2023-05-31",
			MaxTokens:        *settings.MaxTokenCount(),
			Messages: []bedrockAnthropicClaude3Message{
				{
					Role: "user",
					Content: []bedrockAnthropicClaude3Content{
						{
							ContentType: "text",
							Text:        &prompt,
						},
					},
				},
			},
		}, nil
	} else if v.isAnthropicModel(model) {
		var builder strings.Builder
		builder.WriteString("\n\nHuman: ")
		builder.WriteString(prompt)
		builder.WriteString("\n\nAssistant:")
		return bedrockAnthropicGenerateRequest{
			Prompt:            builder.String(),
			MaxTokensToSample: *settings.MaxTokenCount(),
			Temperature:       *settings.Temperature(),
			StopSequences:     settings.StopSequences(),
			TopK:              settings.TopK(),
			TopP:              settings.TopP(),
			AnthropicVersion:  "bedrock-2023-05-31",
		}, nil
	} else if v.isAI21Model(model) {
		return bedrockAI21GenerateRequest{
			Prompt:        prompt,
			MaxTokens:     *settings.MaxTokenCount(),
			Temperature:   *settings.Temperature(),
			TopP:          settings.TopP(),
			StopSequences: settings.StopSequences(),
		}, nil
	} else if v.isCohereCommandRModel(model) {
		return bedrockCohereCommandRRequest{
			Message: prompt,
		}, nil
	} else if v.isCohereModel(model) {
		return bedrockCohereRequest{
			Prompt:      prompt,
			Temperature: *settings.Temperature(),
			MaxTokens:   *settings.MaxTokenCount(),
			// ReturnLikeliHood: "GENERATION", // contray to docs, this is invalid
		}, nil
	} else if v.isMistralAIModel(model) {
		return bedrockMistralAIRequest{
			Prompt:      fmt.Sprintf("<s>[INST] %s [/INST]", prompt),
			MaxTokens:   settings.MaxTokenCount(),
			Temperature: settings.Temperature(),
		}, nil
	} else if v.isMetaModel(model) {
		return bedrockMetaRequest{
			Prompt:      prompt,
			MaxGenLen:   settings.MaxTokenCount(),
			Temperature: settings.Temperature(),
		}, nil
	}
	return nil, fmt.Errorf("unspported model: %s", model)
}

func (v *awsClient) parseBedrockResponse(bodyBytes []byte, model string) (*generativemodels.GenerateResponse, error) {
	content, err := v.getBedrockResponseMessage(model, bodyBytes)
	if err != nil {
		return nil, err
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

func (v *awsClient) getBedrockResponseMessage(model string, bodyBytes []byte) (string, error) {
	var content string
	var resBodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBodyMap); err != nil {
		return "", errors.Wrap(err, "unmarshal response body")
	}

	if v.isCohereCommandRModel(model) {
		var resBody bedrockCohereCommandRResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		return resBody.Text, nil
	} else if v.isAnthropicClaude3Model(model) {
		var resBody bedrockAnthropicClaude3Response
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		if len(resBody.Content) > 0 && resBody.Content[0].Text != nil {
			return *resBody.Content[0].Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isAnthropicModel(model) {
		var resBody bedrockAnthropicClaudeResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		return resBody.Completion, nil
	} else if v.isAI21Model(model) {
		var resBody bedrockAI21Response
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		if len(resBody.Completions) > 0 {
			return resBody.Completions[0].Data.Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isMistralAIModel(model) {
		var resBody bedrockMistralAIResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		if len(resBody.Outputs) > 0 {
			return resBody.Outputs[0].Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isMetaModel(model) {
		var resBody bedrockMetaResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, "unmarshal response body")
		}
		return resBody.Generation, nil
	}

	var resBody bedrockGenerateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return "", errors.Wrap(err, "unmarshal response body")
	}

	if len(resBody.Results) == 0 && len(resBody.Generations) == 0 {
		return "", fmt.Errorf("received empty response from AWS Bedrock")
	}

	if len(resBody.Results) > 0 && len(resBody.Results[0].CompletionReason) > 0 {
		content = resBody.Results[0].OutputText
	} else if len(resBody.Generations) > 0 {
		content = resBody.Generations[0].Text
	}

	return content, nil
}

func (v *awsClient) parseSagemakerResponse(bodyBytes []byte, res *http.Response) (*generativemodels.GenerateResponse, error) {
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

func (v *awsClient) isSagemaker(service string) bool {
	return service == "sagemaker"
}

func (v *awsClient) isBedrock(service string) bool {
	return service == "bedrock"
}

func (v *awsClient) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *awsClient) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *awsClient) getAwsAccessKey(ctx context.Context) (string, error) {
	if awsAccessKey := v.getHeaderValue(ctx, "X-Aws-Access-Key"); awsAccessKey != "" {
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
	if awsSecret := v.getHeaderValue(ctx, "X-Aws-Secret-Key"); awsSecret != "" {
		return awsSecret, nil
	}
	if v.awsSecretKey != "" {
		return v.awsSecretKey, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-Aws-Secret-Key " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY")
}

func (v *awsClient) getAwsSessionToken(ctx context.Context) (string, error) {
	if awsSessionToken := v.getHeaderValue(ctx, "X-Aws-Session-Token"); awsSessionToken != "" {
		return awsSessionToken, nil
	}
	if v.awsSessionToken != "" {
		return v.awsSessionToken, nil
	}
	return "", nil
}

func (v *awsClient) getHeaderValue(ctx context.Context, header string) string {
	headerValue := ctx.Value(header)
	if value, ok := headerValue.([]string); ok &&
		len(value) > 0 && len(value[0]) > 0 {
		return value[0]
	}
	// try getting header from GRPC if not successful
	if value := modulecomponents.GetValueFromGRPC(ctx, header); len(value) > 0 && len(value[0]) > 0 {
		return value[0]
	}
	return ""
}

func (v *awsClient) isAmazonModel(model string) bool {
	return strings.HasPrefix(model, "amazon")
}

func (v *awsClient) isAI21Model(model string) bool {
	return strings.HasPrefix(model, "ai21")
}

func (v *awsClient) isAnthropicModel(model string) bool {
	return strings.HasPrefix(model, "anthropic")
}

func (v *awsClient) isAnthropicClaude3Model(model string) bool {
	return strings.HasPrefix(model, "anthropic.claude-3")
}

func (v *awsClient) isCohereModel(model string) bool {
	return strings.HasPrefix(model, "cohere")
}

func (v *awsClient) isCohereCommandRModel(model string) bool {
	return strings.HasPrefix(model, "cohere.command-r")
}

func (v *awsClient) isMistralAIModel(model string) bool {
	return strings.HasPrefix(model, "mistral")
}

func (v *awsClient) isMetaModel(model string) bool {
	return strings.HasPrefix(model, "meta")
}

type bedrockAmazonGenerateRequest struct {
	InputText            string                `json:"inputText,omitempty"`
	TextGenerationConfig *textGenerationConfig `json:"textGenerationConfig,omitempty"`
}

type bedrockAnthropicGenerateRequest struct {
	Prompt            string   `json:"prompt,omitempty"`
	MaxTokensToSample int      `json:"max_tokens_to_sample,omitempty"`
	Temperature       float64  `json:"temperature,omitempty"`
	StopSequences     []string `json:"stop_sequences,omitempty"`
	TopK              *int     `json:"top_k,omitempty"`
	TopP              *float64 `json:"top_p,omitempty"`
	AnthropicVersion  string   `json:"anthropic_version,omitempty"`
}

type bedrockAnthropicClaudeResponse struct {
	Completion string `json:"completion"`
}

type bedrockAnthropicClaude3Request struct {
	AnthropicVersion string                           `json:"anthropic_version,omitempty"`
	MaxTokens        int                              `json:"max_tokens,omitempty"`
	Messages         []bedrockAnthropicClaude3Message `json:"messages,omitempty"`
}

type bedrockAnthropicClaude3Message struct {
	Role    string                           `json:"role,omitempty"`
	Content []bedrockAnthropicClaude3Content `json:"content,omitempty"`
}

type bedrockAnthropicClaude3Content struct {
	// possible values are: image, text
	ContentType string                          `json:"type,omitempty"`
	Text        *string                         `json:"text,omitempty"`
	Source      *bedrockAnthropicClaudeV3Source `json:"source,omitempty"`
}

type bedrockAnthropicClaude3Response struct {
	ID          string                               `json:"id,omitempty"`
	ContentType string                               `json:"type,omitempty"`
	Role        string                               `json:"role,omitempty"`
	Model       string                               `json:"model,omitempty"`
	StopReason  string                               `json:"stop_reason,omitempty"`
	Usage       bedrockAnthropicClaude3UsageResponse `json:"usage,omitempty"`
	Content     []bedrockAnthropicClaude3Content     `json:"content,omitempty"`
}

type bedrockAnthropicClaude3UsageResponse struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
}

type bedrockAnthropicClaudeV3Source struct {
	// possible values are: base64
	ContentType string `json:"type,omitempty"`
	// possible values are: image/jpeg
	MediaType string `json:"media_type,omitempty"`
	// base64 encoded image
	Data string `json:"data,omitempty"`
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

type bedrockAI21Response struct {
	Completions []bedrockAI21Completion `json:"completions,omitempty"`
}

type bedrockAI21Completion struct {
	Data bedrockAI21Data `json:"data,omitempty"`
}

type bedrockAI21Data struct {
	Text string `json:"text,omitempty"`
}

type bedrockCohereRequest struct {
	Prompt           string  `json:"prompt,omitempty"`
	MaxTokens        int     `json:"max_tokens,omitempty"`
	Temperature      float64 `json:"temperature,omitempty"`
	ReturnLikeliHood string  `json:"return_likelihood,omitempty"`
}

type bedrockCohereCommandRRequest struct {
	Message string `json:"message,omitempty"`
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

type bedrockCohereCommandRResponse struct {
	ChatHistory  []bedrockCohereChatHistory `json:"chat_history,omitempty"`
	ResponseID   string                     `json:"response_id,omitempty"`
	GenerationID string                     `json:"generation_id,omitempty"`
	FinishReason string                     `json:"finish_reason,omitempty"`
	Text         string                     `json:"text,omitempty"`
}

type bedrockCohereChatHistory struct {
	Message string `json:"message,omitempty"`
	Role    string `json:"role,omitempty"`
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

type bedrockMistralAIRequest struct {
	Prompt      string   `json:"prompt,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"topP,omitempty"`
	TopK        *int     `json:"topK,omitempty"`
}

type bedrockMistralAIResponse struct {
	Outputs []bedrockMistralAIOutput `json:"outputs,omitempty"`
}

type bedrockMistralAIOutput struct {
	Text string `json:"text,omitempty"`
}

type bedrockMetaRequest struct {
	Prompt      string   `json:"prompt,omitempty"`
	MaxGenLen   *int     `json:"max_gen_len,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
}

type bedrockMetaResponse struct {
	Generation           string `json:"generation,omitempty"`
	PromptTokenCount     *int   `json:"prompt_token_count,omitempty"`
	GenerationTokenCount *int   `json:"generation_token_count,omitempty"`
	StopReason           string `json:"stop_reason,omitempty"`
}
