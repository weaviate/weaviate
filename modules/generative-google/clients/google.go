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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-google/config"
	googleparams "github.com/weaviate/weaviate/modules/generative-google/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"
)

type harmCategory string

var (
	// Category is unspecified.
	HarmCategoryUnspecified harmCategory = "HARM_CATEGORY_UNSPECIFIED"
	// Negative or harmful comments targeting identity and/or protected attribute.
	HarmCategoryDerogatory harmCategory = "HARM_CATEGORY_DEROGATORY"
	// Content that is rude, disrepspectful, or profane.
	HarmCategoryToxicity harmCategory = "HARM_CATEGORY_TOXICITY"
	// Describes scenarios depictng violence against an individual or group, or general descriptions of gore.
	HarmCategoryViolence harmCategory = "HARM_CATEGORY_VIOLENCE"
	// Contains references to sexual acts or other lewd content.
	HarmCategorySexual harmCategory = "HARM_CATEGORY_SEXUAL"
	// Promotes unchecked medical advice.
	HarmCategoryMedical harmCategory = "HARM_CATEGORY_MEDICAL"
	// Dangerous content that promotes, facilitates, or encourages harmful acts.
	HarmCategoryDangerous harmCategory = "HARM_CATEGORY_DANGEROUS"
	// Harassment content.
	HarmCategoryHarassment harmCategory = "HARM_CATEGORY_HARASSMENT"
	// Hate speech and content.
	HarmCategoryHate_speech harmCategory = "HARM_CATEGORY_HATE_SPEECH"
	// Sexually explicit content.
	HarmCategorySexually_explicit harmCategory = "HARM_CATEGORY_SEXUALLY_EXPLICIT"
	// Dangerous content.
	HarmCategoryDangerous_content harmCategory = "HARM_CATEGORY_DANGEROUS_CONTENT"
)

type harmBlockThreshold string

var (
	// Threshold is unspecified.
	HarmBlockThresholdUnspecified harmBlockThreshold = "HARM_BLOCK_THRESHOLD_UNSPECIFIED"
	// Content with NEGLIGIBLE will be allowed.
	BlockLowAndAbove harmBlockThreshold = "BLOCK_LOW_AND_ABOVE"
	// Content with NEGLIGIBLE and LOW will be allowed.
	BlockMediumAndAbove harmBlockThreshold = "BLOCK_MEDIUM_AND_ABOVE"
	// Content with NEGLIGIBLE, LOW, and MEDIUM will be allowed.
	BlockOnlyHigh harmBlockThreshold = "BLOCK_ONLY_HIGH"
	// All content will be allowed.
	BlockNone harmBlockThreshold = "BLOCK_NONE"
)

type harmProbability string

var (
	// Probability is unspecified.
	HARM_PROBABILITY_UNSPECIFIED harmProbability = "HARM_PROBABILITY_UNSPECIFIED"
	// Content has a negligible chance of being unsafe.
	NEGLIGIBLE harmProbability = "NEGLIGIBLE"
	// Content has a low chance of being unsafe.
	LOW harmProbability = "LOW"
	// Content has a medium chance of being unsafe.
	MEDIUM harmProbability = "MEDIUM"
	// Content has a high chance of being unsafe.
	HIGH harmProbability = "HIGH"
)

var (
	FINISH_REASON_UNSPECIFIED_EXPLANATION        = "The finish reason is unspecified."
	FINISH_REASON_STOP_EXPLANATION               = "Natural stop point of the model or provided stop sequence."
	FINISH_REASON_MAX_TOKENS_EXPLANATION         = "The maximum number of tokens as specified in the request was reached."
	FINISH_REASON_SAFETY_EXPLANATION             = "The token generation was stopped as the response was flagged for safety reasons. NOTE: When streaming the Candidate.content will be empty if content filters blocked the output."
	FINISH_REASON_RECITATION_EXPLANATION         = "The token generation was stopped as the response was flagged for unauthorized citations."
	FINISH_REASON_OTHER_EXPLANATION              = "All other reasons that stopped the token generation"
	FINISH_REASON_BLOCKLIST_EXPLANATION          = "The token generation was stopped as the response was flagged for the terms which are included from the terminology blocklist."
	FINISH_REASON_PROHIBITED_CONTENT_EXPLANATION = "The token generation was stopped as the response was flagged for the prohibited contents."
	FINISH_REASON_SPII_EXPLANATION               = "The token generation was stopped as the response was flagged for Sensitive Personally Identifiable Information (SPII) contents."

	FINISH_REASON_UNSPECIFIED        = "FINISH_REASON_UNSPECIFIED"
	FINISH_REASON_STOP               = "STOP"
	FINISH_REASON_MAX_TOKENS         = "MAX_TOKENS"
	FINISH_REASON_SAFETY             = "SAFETY"
	FINISH_REASON_RECITATION         = "RECITATION"
	FINISH_REASON_OTHER              = "OTHER"
	FINISH_REASON_BLOCKLIST          = "BLOCKLIST"
	FINISH_REASON_PROHIBITED_CONTENT = "PROHIBITED_CONTENT"
	FINISH_REASON_SPII               = "SPII"
)

func buildURL(useGenerativeAI bool, apiEndoint, projectID, modelID, region string) string {
	if useGenerativeAI {
		// Generative AI endpoints, for more context check out this link:
		// https://developers.generativeai.google/models/language#model_variations
		// https://developers.generativeai.google/api/rest/generativelanguage/models/generateMessage
		if strings.HasPrefix(modelID, "gemini") {
			return fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", modelID)
		}
		return "https://generativelanguage.googleapis.com/v1beta2/models/chat-bison-001:generateMessage"
	}
	if strings.HasPrefix(modelID, "gemini") {
		// Vertex AI support for Gemini models: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/gemini
		urlTemplate := "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent"
		return fmt.Sprintf(urlTemplate, region, projectID, region, modelID)
	}
	urlTemplate := "https://%s/v1/projects/%s/locations/us-central1/publishers/google/models/%s:predict"
	return fmt.Sprintf(urlTemplate, apiEndoint, projectID, modelID)
}

type google struct {
	apiKey        string
	useGoogleAuth bool
	googleApiKey  *apikey.GoogleApiKey
	buildUrlFn    func(useGenerativeAI bool, apiEndoint, projectID, modelID, region string) string
	httpClient    *http.Client
	logger        logrus.FieldLogger
}

func New(apiKey string, useGoogleAuth bool, timeout time.Duration, logger logrus.FieldLogger) *google {
	return &google{
		apiKey:        apiKey,
		useGoogleAuth: useGoogleAuth,
		googleApiKey:  apikey.NewGoogleApiKey(),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildURL,
		logger:     logger,
	}
}

func (v *google) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forPrompt, generative.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (v *google) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forTask, generative.Blobs(properties), options, debug)
}

func (v *google) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options, imageProperties)
	debugInformation := v.getDebugInformation(debug, prompt)

	useGenerativeAIEndpoint := v.useGenerativeAIEndpoint(params.ApiEndpoint)
	modelID := params.Model
	if params.EndpointID != "" {
		modelID = params.EndpointID
	}

	endpointURL := v.buildUrlFn(useGenerativeAIEndpoint, params.ApiEndpoint, params.ProjectID, modelID, params.Region)
	input := v.getPayload(useGenerativeAIEndpoint, prompt, params)

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx, useGenerativeAIEndpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "Google API Key")
	}
	req.Header.Add("Content-Type", "application/json")
	if useGenerativeAIEndpoint {
		req.Header.Add("x-goog-api-key", apiKey)
	} else {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
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

	if useGenerativeAIEndpoint {
		if strings.HasPrefix(modelID, "gemini") {
			return v.parseGenerateContentResponse(res.StatusCode, bodyBytes, debugInformation)
		}
		return v.parseGenerateMessageResponse(res.StatusCode, bodyBytes, debugInformation)
	}
	if strings.HasPrefix(modelID, "gemini") {
		return v.parseGenerateContentResponse(res.StatusCode, bodyBytes, debugInformation)
	}
	return v.parseResponse(res.StatusCode, bodyBytes, debugInformation)
}

func (v *google) parseGenerateMessageResponse(statusCode int, bodyBytes []byte, debug *modulecapabilities.GenerateDebugInformation) (*modulecapabilities.GenerateResponse, error) {
	var resBody generateMessageResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Candidates) > 0 {
		return v.getGenerateResponse(resBody.Candidates[0].Content, nil, debug)
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debug,
	}, nil
}

func (v *google) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) googleparams.Params {
	settings := config.NewClassSettings(cfg)

	var params googleparams.Params
	if p, ok := options.(googleparams.Params); ok {
		params = p
	}

	if params.ApiEndpoint == "" {
		params.ApiEndpoint = settings.ApiEndpoint()
	}
	if params.ProjectID == "" {
		params.ProjectID = settings.ProjectID()
	}
	if params.EndpointID == "" {
		params.EndpointID = settings.EndpointID()
	}
	if params.Region == "" {
		params.Region = settings.Region()
	}
	if params.Model == "" {
		model := settings.Model()
		if model == "" {
			model = settings.ModelID()
		}
		params.Model = model
	}
	if params.Temperature == nil {
		temperature := settings.Temperature()
		params.Temperature = &temperature
	}
	if params.TopP == nil {
		topP := settings.TopP()
		params.TopP = &topP
	}
	if params.TopK == nil {
		topK := settings.TopK()
		params.TopK = &topK
	}
	if params.MaxTokens == nil {
		maxTokens := settings.TokenLimit()
		params.MaxTokens = &maxTokens
	}

	params.Images = generative.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (v *google) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *google) parseGenerateContentResponse(statusCode int, bodyBytes []byte, debug *modulecapabilities.GenerateDebugInformation) (*modulecapabilities.GenerateResponse, error) {
	var resBody generateContentResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Candidates) > 0 {
		if len(resBody.Candidates[0].Content.Parts) > 0 {
			var params map[string]interface{}
			if resBody.UsageMetadata != nil {
				params = v.getResponseParams(map[string]interface{}{
					"usageMetadata": resBody.UsageMetadata,
				})
			}
			return v.getGenerateResponse(resBody.Candidates[0].Content.Parts[0].Text, params, debug)
		}
		return nil, fmt.Errorf("%s", v.decodeFinishReason(resBody.Candidates[0].FinishReason))
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debug,
	}, nil
}

func (v *google) parseResponse(statusCode int, bodyBytes []byte, debug *modulecapabilities.GenerateDebugInformation) (*modulecapabilities.GenerateResponse, error) {
	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Predictions) > 0 && len(resBody.Predictions[0].Candidates) > 0 {
		var params map[string]interface{}
		if resBody.Metadata != nil {
			params = v.getResponseParams(map[string]interface{}{
				"metadata": resBody.Metadata,
			})
		}
		return v.getGenerateResponse(resBody.Predictions[0].Candidates[0].Content, params, debug)
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debug,
	}, nil
}

func (v *google) getResponseParams(params map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{googleparams.Name: params}
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[googleparams.Name].(map[string]interface{}); ok {
		responseParams := &responseParams{}
		if metadata, ok := params["metadata"].(*metadata); ok {
			responseParams.Metadata = metadata
		}
		if usageMetadata, ok := params["usageMetadata"].(*usageMetadata); ok {
			responseParams.UsageMetadata = usageMetadata
		}
		return responseParams
	}
	return nil
}

func (v *google) getGenerateResponse(content *string, params map[string]interface{}, debug *modulecapabilities.GenerateDebugInformation) (*modulecapabilities.GenerateResponse, error) {
	if content != nil && *content != "" {
		trimmedResponse := strings.Trim(*content, "\n")
		return &modulecapabilities.GenerateResponse{
			Result: &trimmedResponse,
			Params: params,
			Debug:  debug,
		}, nil
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debug,
	}, nil
}

func (v *google) checkResponse(statusCode int, googleApiError *googleApiError) error {
	if statusCode != 200 || googleApiError != nil {
		if googleApiError != nil {
			return fmt.Errorf("connection to Google failed with status: %v error: %v",
				statusCode, googleApiError.Message)
		}
		return fmt.Errorf("connection to Google failed with status: %d", statusCode)
	}
	return nil
}

func (v *google) useGenerativeAIEndpoint(apiEndpoint string) bool {
	return apiEndpoint == "generativelanguage.googleapis.com"
}

func (v *google) getPayload(useGenerativeAI bool, prompt string, params googleparams.Params) any {
	if useGenerativeAI {
		if strings.HasPrefix(params.Model, "gemini") {
			return v.getGeminiPayload(prompt, params)
		}
		input := generateMessageRequest{
			Prompt: &generateMessagePrompt{
				Messages: []generateMessage{
					{
						Content: &prompt,
					},
				},
			},
			Temperature:    params.Temperature,
			TopP:           params.TopP,
			TopK:           params.TopK,
			CandidateCount: 1,
		}
		return input
	}
	if strings.HasPrefix(params.Model, "gemini") {
		return v.getGeminiPayload(prompt, params)
	}
	input := generateInput{
		Instances: []instance{
			{
				Messages: []message{
					{
						Content: prompt,
					},
				},
			},
		},
		Parameters: parameters{
			Temperature:      params.Temperature,
			MaxOutputTokens:  params.MaxTokens,
			TopP:             params.TopP,
			TopK:             params.TopK,
			StopSequences:    params.StopSequences,
			PresencePenalty:  params.PresencePenalty,
			FrequencyPenalty: params.FrequencyPenalty,
		},
	}
	return input
}

func (v *google) getGeminiPayload(prompt string, params googleparams.Params) any {
	parts := []part{
		{
			Text: &prompt,
		},
	}
	for _, image := range params.Images {
		parts = append(parts, part{
			InlineData: &inlineData{Data: image, MimeType: "image/png"},
		})
	}
	input := generateContentRequest{
		Contents: []content{
			{
				Role:  "user",
				Parts: parts,
			},
		},
		GenerationConfig: &generationConfig{
			Temperature:      params.Temperature,
			MaxOutputTokens:  params.MaxTokens,
			TopP:             params.TopP,
			TopK:             params.TopK,
			PresencePenalty:  params.PresencePenalty,
			FrequencyPenalty: params.FrequencyPenalty,
			StopSequences:    params.StopSequences,
			CandidateCount:   1,
		},
		SafetySettings: []safetySetting{
			{
				Category:  HarmCategoryHarassment,
				Threshold: BlockMediumAndAbove,
			},
			{
				Category:  HarmCategoryHate_speech,
				Threshold: BlockMediumAndAbove,
			},
			{
				Category:  HarmCategoryDangerous_content,
				Threshold: BlockMediumAndAbove,
			},
		},
	}
	return input
}

func (v *google) getApiKey(ctx context.Context, useGenerativeAIEndpoint bool) (string, error) {
	return v.googleApiKey.GetApiKey(ctx, v.apiKey, useGenerativeAIEndpoint, v.useGoogleAuth)
}

func (v *google) decodeFinishReason(reason string) string {
	switch reason {
	case FINISH_REASON_UNSPECIFIED:
		return FINISH_REASON_UNSPECIFIED_EXPLANATION
	case FINISH_REASON_STOP:
		return FINISH_REASON_STOP_EXPLANATION
	case FINISH_REASON_MAX_TOKENS:
		return FINISH_REASON_MAX_TOKENS_EXPLANATION
	case FINISH_REASON_SAFETY:
		return FINISH_REASON_SAFETY_EXPLANATION
	case FINISH_REASON_RECITATION:
		return FINISH_REASON_RECITATION_EXPLANATION
	case FINISH_REASON_OTHER:
		return FINISH_REASON_OTHER_EXPLANATION
	case FINISH_REASON_BLOCKLIST:
		return FINISH_REASON_BLOCKLIST_EXPLANATION
	case FINISH_REASON_PROHIBITED_CONTENT:
		return FINISH_REASON_PROHIBITED_CONTENT_EXPLANATION
	case FINISH_REASON_SPII:
		return FINISH_REASON_SPII_EXPLANATION
	default:
		return fmt.Sprintf("unregonized finis reason: %s", reason)
	}
}

type generateInput struct {
	Instances  []instance `json:"instances,omitempty"`
	Parameters parameters `json:"parameters"`
}

type instance struct {
	Context  string    `json:"context,omitempty"`
	Messages []message `json:"messages,omitempty"`
	Examples []example `json:"examples,omitempty"`
}

type message struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type example struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

type parameters struct {
	Temperature      *float64 `json:"temperature,omitempty"`
	MaxOutputTokens  *int     `json:"maxOutputTokens,omitempty"`
	TopP             *float64 `json:"topP,omitempty"`
	TopK             *int     `json:"topK,omitempty"`
	StopSequences    []string `json:"stopSequences,omitempty"`
	PresencePenalty  *float64 `json:"presencePenalty,omitempty"`
	FrequencyPenalty *float64 `json:"frequencyPenalty,omitempty"`
}

type generateResponse struct {
	Predictions      []prediction    `json:"predictions,omitempty"`
	Metadata         *metadata       `json:"metadata,omitempty"`
	Error            *googleApiError `json:"error,omitempty"`
	DeployedModelId  string          `json:"deployedModelId,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	ModelVersionId   string          `json:"modelVersionId,omitempty"`
}

type prediction struct {
	Candidates       []candidate         `json:"candidates,omitempty"`
	SafetyAttributes *[]safetyAttributes `json:"safetyAttributes,omitempty"`
}

type metadata struct {
	TokenMetadata *tokenMetadata `json:"tokenMetadata,omitempty"`
}

type tokenMetadata struct {
	InputTokenCount  *tokenCount `json:"inputTokenCount,omitempty"`
	OutputTokenCount *tokenCount `json:"outputTokenCount,omitempty"`
}

type tokenCount struct {
	TotalBillableCharacters int64 `json:"totalBillableCharacters,omitempty"`
	TotalTokens             int64 `json:"totalTokens,omitempty"`
}

type candidate struct {
	Author  *string `json:"author,omitempty"`
	Content *string `json:"content,omitempty"`
}

type safetyAttributes struct {
	Scores     []float64 `json:"scores,omitempty"`
	Blocked    *bool     `json:"blocked,omitempty"`
	Categories []string  `json:"categories,omitempty"`
}

type googleApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type generateMessageRequest struct {
	Prompt         *generateMessagePrompt `json:"prompt,omitempty"`
	Temperature    *float64               `json:"temperature,omitempty"`
	TopP           *float64               `json:"topP,omitempty"`
	TopK           *int                   `json:"topK,omitempty"`
	CandidateCount int                    `json:"candidateCount"` // default 1
}

type generateMessagePrompt struct {
	Context  string            `json:"prompt,omitempty"`
	Examples []generateExample `json:"examples,omitempty"`
	Messages []generateMessage `json:"messages,omitempty"`
}

type generateMessage struct {
	Author           string                    `json:"author,omitempty"`
	Content          *string                   `json:"content,omitempty"`
	CitationMetadata *generateCitationMetadata `json:"citationMetadata,omitempty"`
}

type generateCitationMetadata struct {
	CitationSources []generateCitationSource `json:"citationSources,omitempty"`
}

type generateCitationSource struct {
	StartIndex int    `json:"startIndex,omitempty"`
	EndIndex   int    `json:"endIndex,omitempty"`
	URI        string `json:"uri,omitempty"`
	License    string `json:"license,omitempty"`
}

type generateExample struct {
	Input  *generateMessage `json:"input,omitempty"`
	Output *generateMessage `json:"output,omitempty"`
}

type generateMessageResponse struct {
	Candidates []generateMessage `json:"candidates,omitempty"`
	Messages   []generateMessage `json:"messages,omitempty"`
	Filters    []contentFilter   `json:"filters,omitempty"`
	Error      *googleApiError   `json:"error,omitempty"`
}

type contentFilter struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type generateContentRequest struct {
	Contents         []content         `json:"contents,omitempty"`
	SafetySettings   []safetySetting   `json:"safetySettings,omitempty"`
	GenerationConfig *generationConfig `json:"generationConfig,omitempty"`
}

type content struct {
	Parts []part `json:"parts,omitempty"`
	Role  string `json:"role,omitempty"`
}

type part struct {
	Text       *string     `json:"text,omitempty"`
	InlineData *inlineData `json:"inline_data,omitempty"`
}

type inlineData struct {
	MimeType string  `json:"mime_type,omitempty"`
	Data     *string `json:"data,omitempty"`
}

type safetySetting struct {
	Category  harmCategory       `json:"category,omitempty"`
	Threshold harmBlockThreshold `json:"threshold,omitempty"`
}

type generationConfig struct {
	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"topP,omitempty"`
	TopK             *int     `json:"topK,omitempty"`
	MaxOutputTokens  *int     `json:"maxOutputTokens,omitempty"`
	PresencePenalty  *float64 `json:"presencePenalty,omitempty"`
	FrequencyPenalty *float64 `json:"frequencyPenalty,omitempty"`
	StopSequences    []string `json:"stopSequences,omitempty"`
	CandidateCount   int      `json:"candidateCount,omitempty"`
}

type generateContentResponse struct {
	Candidates     []generateContentCandidate `json:"candidates,omitempty"`
	UsageMetadata  *usageMetadata             `json:"usageMetadata,omitempty"`
	PromptFeedback *promptFeedback            `json:"promptFeedback,omitempty"`
	Error          *googleApiError            `json:"error,omitempty"`
}

type generateContentCandidate struct {
	Content       contentResponse `json:"content,omitempty"`
	FinishReason  string          `json:"finishReason,omitempty"`
	Index         int             `json:"index,omitempty"`
	SafetyRatings []safetyRating  `json:"safetyRatings,omitempty"`
}

type contentResponse struct {
	Parts []part `json:"parts,omitempty"`
	Role  string `json:"role,omitempty"`
}

type promptFeedback struct {
	SafetyRatings []safetyRating `json:"safetyRatings,omitempty"`
}

type safetyRating struct {
	Category    harmCategory    `json:"category,omitempty"`
	Probability harmProbability `json:"probability,omitempty"`
	Blocked     *bool           `json:"blocked,omitempty"`
}

type usageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}

type responseParams struct {
	Metadata      *metadata      `json:"metadata,omitempty"`
	UsageMetadata *usageMetadata `json:"usageMetadata,omitempty"`
}
