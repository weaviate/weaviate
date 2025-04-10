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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativeconfig "github.com/weaviate/weaviate/modules/generative-aws/config"
	awsparams "github.com/weaviate/weaviate/modules/generative-aws/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	generativecomponents "github.com/weaviate/weaviate/usecases/modulecomponents/generative"
)

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

func (v *awsClient) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generativecomponents.MakeSinglePrompt(generativecomponents.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, generativecomponents.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (v *awsClient) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generativecomponents.MakeTaskPrompt(generativecomponents.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, generativecomponents.Blobs(properties), options, debug)
}

func (v *awsClient) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options, imageProperties)
	service := params.Service
	debugInformation := v.getDebugInformation(debug, prompt)

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
			params,
			cfg,
			debugInformation,
		)
	} else if v.isSagemaker(service) {
		var body []byte
		var endpointUrl string
		var host string
		var path string
		var err error

		region := params.Region
		endpoint := params.Endpoint
		targetModel := params.TargetModel
		targetVariant := params.TargetVariant

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
		return &modulecapabilities.GenerateResponse{
			Result: nil,
			Debug:  debugInformation,
		}, nil
	}
}

func (v *awsClient) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *awsClient) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) awsparams.Params {
	settings := generativeconfig.NewClassSettings(cfg)

	service := settings.Service()
	var params awsparams.Params
	if p, ok := options.(awsparams.Params); ok {
		params = p
	}

	if params.Service == "" {
		params.Service = settings.Service()
	}
	if params.Region == "" {
		params.Region = settings.Region()
	}
	if params.Endpoint == "" {
		params.Endpoint = settings.Endpoint()
	}
	if params.TargetModel == "" {
		params.TargetModel = settings.TargetModel()
	}
	if params.TargetVariant == "" {
		params.TargetVariant = settings.TargetVariant()
	}
	if params.Model == "" {
		params.Model = settings.Model()
	}
	if params.Temperature == nil {
		temperature := settings.Temperature(service, params.Model)
		params.Temperature = temperature
	}

	params.Images = generativecomponents.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (v *awsClient) sendBedrockRequest(
	ctx context.Context,
	prompt string,
	awsKey, awsSecret, awsSessionToken string,
	maxRetries int,
	params awsparams.Params,
	cfg moduletools.ClassConfig,
	debugInformation *modulecapabilities.GenerateDebugInformation,
) (*modulecapabilities.GenerateResponse, error) {
	model := params.Model
	region := params.Region
	req, err := v.createRequestBody(prompt, params, cfg)
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
		Accept:      aws.String("application/json"),
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

	return v.parseBedrockResponse(result.Body, model, debugInformation)
}

func (v *awsClient) createRequestBody(prompt string, params awsparams.Params, cfg moduletools.ClassConfig) (interface{}, error) {
	settings := generativeconfig.NewClassSettings(cfg)
	model := params.Model
	service := settings.Service()
	if v.isAmazonTitanModel(model) {
		return bedrockAmazonGenerateRequest{
			InputText: prompt,
		}, nil
	} else if v.isAmazonNovaModel(model) {
		var content []bedrockAmazonNovaContent
		for i := range params.Images {
			content = append(content, bedrockAmazonNovaContent{
				Image: &bedrockAmazonNovaContentImage{
					Format: "jpg",
					Source: bedrockAmazonNovaContentImageSource{
						Bytes: params.Images[i],
					},
				},
			})
		}
		content = append(content, bedrockAmazonNovaContent{
			Text: &prompt,
		})
		return bedrockAmazonNovaRequest{
			Messages: []bedrockAmazonNovaMessage{
				{
					Role:    "user",
					Content: content,
				},
			},
			InferenceConfig: &bedrockAmazonNovaInferenceConfig{
				Temperature: params.Temperature,
			},
		}, nil
	} else if v.isAnthropicClaude3Model(model) {
		var content []bedrockAnthropicClaude3Content
		for i := range params.Images {
			imageName := fmt.Sprintf("Image %d:", i+1)
			content = append(content, bedrockAnthropicClaude3Content{
				Type: "text",
				Text: &imageName,
			})
			content = append(content, bedrockAnthropicClaude3Content{
				Type: "image",
				Source: &bedrockAnthropicClaudeV3Source{
					ContentType: "base64",
					MediaType:   "image/jpeg",
					Data:        params.Images[i],
				},
			})
		}
		content = append(content, bedrockAnthropicClaude3Content{
			Type: "text",
			Text: &prompt,
		})
		return bedrockAnthropicClaude3Request{
			AnthropicVersion: "bedrock-2023-05-31",
			MaxTokens:        settings.MaxTokenCount(service, model),
			Messages: []bedrockAnthropicClaude3Message{
				{
					Role:    "user",
					Content: content,
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
			Temperature:       params.Temperature,
			MaxTokensToSample: settings.MaxTokenCount(service, model),
			StopSequences:     settings.StopSequences(service, model),
			TopK:              settings.TopK(service, model),
			TopP:              settings.TopP(service, model),
			AnthropicVersion:  "bedrock-2023-05-31",
		}, nil
	} else if v.isAI21Model(model) {
		return bedrockAI21GenerateRequest{
			Prompt:        prompt,
			Temperature:   params.Temperature,
			MaxTokens:     settings.MaxTokenCount(service, model),
			TopP:          settings.TopP(service, model),
			StopSequences: settings.StopSequences(service, model),
		}, nil
	} else if v.isCohereCommandRModel(model) {
		return bedrockCohereCommandRRequest{
			Message: prompt,
		}, nil
	} else if v.isCohereModel(model) {
		return bedrockCohereRequest{
			Prompt:      prompt,
			Temperature: params.Temperature,
			MaxTokens:   settings.MaxTokenCount(service, model),
			// ReturnLikeliHood: "GENERATION", // contray to docs, this is invalid
		}, nil
	} else if v.isMistralAIModel(model) {
		return bedrockMistralAIRequest{
			Prompt:      fmt.Sprintf("<s>[INST] %s [/INST]", prompt),
			Temperature: params.Temperature,
			MaxTokens:   settings.MaxTokenCount(service, model),
		}, nil
	} else if v.isMetaModel(model) {
		return bedrockMetaRequest{
			Prompt:      prompt,
			Temperature: params.Temperature,
			MaxGenLen:   settings.MaxTokenCount(service, model),
		}, nil
	}
	return nil, fmt.Errorf("unspported model: %s", model)
}

func (v *awsClient) parseBedrockResponse(bodyBytes []byte,
	model string,
	debug *modulecapabilities.GenerateDebugInformation,
) (*modulecapabilities.GenerateResponse, error) {
	content, err := v.getBedrockResponseMessage(model, bodyBytes)
	if err != nil {
		return nil, err
	}

	if content != "" {
		return &modulecapabilities.GenerateResponse{
			Result: &content,
			Debug:  debug,
		}, nil
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debug,
	}, nil
}

func (v *awsClient) getBedrockResponseMessage(model string, bodyBytes []byte) (string, error) {
	var content string
	var resBodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBodyMap); err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if v.isCohereCommandRModel(model) {
		var resBody bedrockCohereCommandRResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		return resBody.Text, nil
	} else if v.isAnthropicClaude3Model(model) {
		var resBody bedrockAnthropicClaude3Response
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		if len(resBody.Content) > 0 && resBody.Content[0].Text != nil {
			return *resBody.Content[0].Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isAnthropicModel(model) {
		var resBody bedrockAnthropicClaudeResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		return resBody.Completion, nil
	} else if v.isAI21Model(model) {
		var resBody bedrockAI21Response
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		if len(resBody.Completions) > 0 {
			return resBody.Completions[0].Data.Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isMistralAIModel(model) {
		var resBody bedrockMistralAIResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		if len(resBody.Outputs) > 0 {
			return resBody.Outputs[0].Text, nil
		}
		return "", fmt.Errorf("no message from model: %s", model)
	} else if v.isMetaModel(model) {
		var resBody bedrockMetaResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		return resBody.Generation, nil
	} else if v.isAmazonNovaModel(model) {
		var resBody bedrockNovaResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
		}
		if len(resBody.Output.Message.Content) > 0 {
			return resBody.Output.Message.Content[0].Text, nil
		}
		return "", nil
	}

	var resBody bedrockGenerateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
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

func (v *awsClient) parseSagemakerResponse(bodyBytes []byte, res *http.Response) (*modulecapabilities.GenerateResponse, error) {
	var resBody sagemakerGenerateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
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
			return &modulecapabilities.GenerateResponse{
				Result: &content,
			}, nil
		}
	}
	return &modulecapabilities.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *awsClient) isSagemaker(service string) bool {
	return service == "sagemaker"
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
	if v.awsSecretKey != "" {
		return v.awsSecretKey, nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-Aws-Secret-Key " +
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

func (v *awsClient) isAmazonTitanModel(model string) bool {
	return strings.HasPrefix(model, "amazon.titan")
}

func (v *awsClient) isAmazonNovaModel(model string) bool {
	return strings.HasPrefix(model, "amazon.nova")
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

type bedrockAmazonNovaRequest struct {
	Messages        []bedrockAmazonNovaMessage        `json:"messages,omitempty"`
	InferenceConfig *bedrockAmazonNovaInferenceConfig `json:"inferenceConfig,omitempty"`
}

type bedrockAmazonNovaMessage struct {
	Role    string                     `json:"role,omitempty"`
	Content []bedrockAmazonNovaContent `json:"content,omitempty"`
}

type bedrockAmazonNovaContent struct {
	Text  *string                        `json:"text,omitempty"`
	Image *bedrockAmazonNovaContentImage `json:"image,omitempty"`
}

type bedrockAmazonNovaContentImage struct {
	Format string                              `json:"format,omitempty"`
	Source bedrockAmazonNovaContentImageSource `json:"source,omitempty"`
}

type bedrockAmazonNovaContentImageSource struct {
	Bytes *string `json:"bytes,omitempty"`
}

type bedrockAmazonNovaInferenceConfig struct {
	MaxNewTokens *int     `json:"max_new_tokens,omitempty"`
	TopP         *float64 `json:"top_p,omitempty"`
	TopK         *int     `json:"top_k,omitempty"`
	Temperature  *float64 `json:"temperature,omitempty"`
}

type bedrockAnthropicGenerateRequest struct {
	Prompt            string   `json:"prompt,omitempty"`
	MaxTokensToSample *int     `json:"max_tokens_to_sample,omitempty"`
	Temperature       *float64 `json:"temperature,omitempty"`
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
	MaxTokens        *int                             `json:"max_tokens,omitempty"`
	Messages         []bedrockAnthropicClaude3Message `json:"messages,omitempty"`
}

type bedrockAnthropicClaude3Message struct {
	Role    string                           `json:"role,omitempty"`
	Content []bedrockAnthropicClaude3Content `json:"content,omitempty"`
}

type bedrockAnthropicClaude3Content struct {
	// possible values are: image, text
	Type   string                          `json:"type,omitempty"`
	Text   *string                         `json:"text,omitempty"`
	Source *bedrockAnthropicClaudeV3Source `json:"source,omitempty"`
}

type bedrockAnthropicClaude3Response struct {
	ID         string                               `json:"id,omitempty"`
	Type       string                               `json:"type,omitempty"`
	Role       string                               `json:"role,omitempty"`
	Model      string                               `json:"model,omitempty"`
	StopReason string                               `json:"stop_reason,omitempty"`
	Usage      bedrockAnthropicClaude3UsageResponse `json:"usage,omitempty"`
	Content    []bedrockAnthropicClaude3Content     `json:"content,omitempty"`
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
	Data *string `json:"data,omitempty"`
}

type bedrockAI21GenerateRequest struct {
	Prompt           string   `json:"prompt,omitempty"`
	MaxTokens        *int     `json:"maxTokens,omitempty"`
	Temperature      *float64 `json:"temperature,omitempty"`
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
	Prompt           string   `json:"prompt,omitempty"`
	MaxTokens        *int     `json:"max_tokens,omitempty"`
	Temperature      *float64 `json:"temperature,omitempty"`
	ReturnLikeliHood string   `json:"return_likelihood,omitempty"`
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

type bedrockNovaResponse struct {
	Output bedrockNovaResponseOutput `json:"output,omitempty"`
}

type bedrockNovaResponseOutput struct {
	Message bedrockNovaResponseMessage `json:"message,omitempty"`
}

type bedrockNovaResponseMessage struct {
	Content []bedrockNovaResponseContent `json:"content,omitempty"`
}

type bedrockNovaResponseContent struct {
	Text string `json:"text,omitempty"`
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
