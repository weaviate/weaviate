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

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-palm/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
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

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildURL(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
	if useGenerativeAI {
		// Generative AI endpoints, for more context check out this link:
		// https://developers.generativeai.google/models/language#model_variations
		// https://developers.generativeai.google/api/rest/generativelanguage/models/generateMessage
		if strings.HasPrefix(modelID, "gemini") {
			return fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", modelID)
		}
		return "https://generativelanguage.googleapis.com/v1beta2/models/chat-bison-001:generateMessage"
	}
	urlTemplate := "https://%s/v1/projects/%s/locations/us-central1/publishers/google/models/%s:predict"
	return fmt.Sprintf(urlTemplate, apiEndoint, projectID, modelID)
}

type palm struct {
	apiKey     string
	buildUrlFn func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *palm {
	return &palm{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildURL,
		logger:     logger,
	}
}

func (v *palm) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *palm) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *palm) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	useGenerativeAIEndpoint := v.useGenerativeAIEndpoint(settings.ApiEndpoint())
	modelID := settings.ModelID()
	if settings.EndpointID() != "" {
		modelID = settings.EndpointID()
	}

	endpointURL := v.buildUrlFn(useGenerativeAIEndpoint, settings.ApiEndpoint(), settings.ProjectID(), modelID)
	input := v.getPayload(useGenerativeAIEndpoint, prompt, settings)

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "PaLM API Key")
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
			return v.parseGenerateContentResponse(res.StatusCode, bodyBytes)
		}
		return v.parseGenerateMessageResponse(res.StatusCode, bodyBytes)
	}
	return v.parseResponse(res.StatusCode, bodyBytes)
}

func (v *palm) parseGenerateMessageResponse(statusCode int, bodyBytes []byte) (*generativemodels.GenerateResponse, error) {
	var resBody generateMessageResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Candidates) > 0 {
		return v.getGenerateResponse(resBody.Candidates[0].Content)
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *palm) parseGenerateContentResponse(statusCode int, bodyBytes []byte) (*generativemodels.GenerateResponse, error) {
	var resBody generateContentResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Candidates) > 0 && len(resBody.Candidates[0].Content.Parts) > 0 {
		return v.getGenerateResponse(resBody.Candidates[0].Content.Parts[0].Text)
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *palm) parseResponse(statusCode int, bodyBytes []byte) (*generativemodels.GenerateResponse, error) {
	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Predictions) > 0 && len(resBody.Predictions[0].Candidates) > 0 {
		return v.getGenerateResponse(resBody.Predictions[0].Candidates[0].Content)
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *palm) getGenerateResponse(content string) (*generativemodels.GenerateResponse, error) {
	if content != "" {
		trimmedResponse := strings.Trim(content, "\n")
		return &generativemodels.GenerateResponse{
			Result: &trimmedResponse,
		}, nil
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *palm) checkResponse(statusCode int, palmApiError *palmApiError) error {
	if statusCode != 200 || palmApiError != nil {
		if palmApiError != nil {
			return fmt.Errorf("connection to Google PaLM failed with status: %v error: %v",
				statusCode, palmApiError.Message)
		}
		return fmt.Errorf("connection to Google PaLM failed with status: %d", statusCode)
	}
	return nil
}

func (v *palm) useGenerativeAIEndpoint(apiEndpoint string) bool {
	return apiEndpoint == "generativelanguage.googleapis.com"
}

func (v *palm) getPayload(useGenerativeAI bool, prompt string, settings config.ClassSettings) any {
	if useGenerativeAI {
		if strings.HasPrefix(settings.ModelID(), "gemini") {
			input := generateContentRequest{
				Contents: []content{
					{
						Role: "user",
						Parts: []part{
							{
								Text: prompt,
							},
						},
					},
				},
				GenerationConfig: &generationConfig{
					Temperature:    settings.Temperature(),
					TopP:           settings.TopP(),
					TopK:           settings.TopK(),
					CandidateCount: 1,
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
					{
						Category:  HarmCategoryDangerous_content,
						Threshold: BlockMediumAndAbove,
					},
				},
			}
			return input
		}
		input := generateMessageRequest{
			Prompt: &generateMessagePrompt{
				Messages: []generateMessage{
					{
						Content: prompt,
					},
				},
			},
			Temperature:    settings.Temperature(),
			TopP:           settings.TopP(),
			TopK:           settings.TopK(),
			CandidateCount: 1,
		}
		return input
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
			Temperature:     settings.Temperature(),
			MaxOutputTokens: settings.TokenLimit(),
			TopP:            settings.TopP(),
			TopK:            settings.TopK(),
		},
	}
	return input
}

func (v *palm) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *palm) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *palm) getApiKey(ctx context.Context) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, "X-Palm-Api-Key"); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Palm-Api-Key " +
		"nor in environment variable under PALM_APIKEY")
}

func (v *palm) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}
	return ""
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
	Temperature     float64 `json:"temperature"`
	MaxOutputTokens int     `json:"maxOutputTokens"`
	TopP            float64 `json:"topP"`
	TopK            int     `json:"topK"`
}

type generateResponse struct {
	Predictions      []prediction  `json:"predictions,omitempty"`
	Error            *palmApiError `json:"error,omitempty"`
	DeployedModelId  string        `json:"deployedModelId,omitempty"`
	Model            string        `json:"model,omitempty"`
	ModelDisplayName string        `json:"modelDisplayName,omitempty"`
	ModelVersionId   string        `json:"modelVersionId,omitempty"`
}

type prediction struct {
	Candidates       []candidate         `json:"candidates,omitempty"`
	SafetyAttributes *[]safetyAttributes `json:"safetyAttributes,omitempty"`
}

type candidate struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type safetyAttributes struct {
	Scores     []float64 `json:"scores,omitempty"`
	Blocked    *bool     `json:"blocked,omitempty"`
	Categories []string  `json:"categories,omitempty"`
}

type palmApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type generateMessageRequest struct {
	Prompt         *generateMessagePrompt `json:"prompt,omitempty"`
	Temperature    float64                `json:"temperature,omitempty"`
	CandidateCount int                    `json:"candidateCount,omitempty"` // default 1
	TopP           float64                `json:"topP"`
	TopK           int                    `json:"topK"`
}

type generateMessagePrompt struct {
	Context  string            `json:"prompt,omitempty"`
	Examples []generateExample `json:"examples,omitempty"`
	Messages []generateMessage `json:"messages,omitempty"`
}

type generateMessage struct {
	Author           string                    `json:"author,omitempty"`
	Content          string                    `json:"content,omitempty"`
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
	Error      *palmApiError     `json:"error,omitempty"`
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
	Text       string `json:"text,omitempty"`
	InlineData string `json:"inline_data,omitempty"`
}

type safetySetting struct {
	Category  harmCategory       `json:"category,omitempty"`
	Threshold harmBlockThreshold `json:"threshold,omitempty"`
}

type generationConfig struct {
	StopSequences   []string `json:"stopSequences,omitempty"`
	CandidateCount  int      `json:"candidateCount,omitempty"`
	MaxOutputTokens int      `json:"maxOutputTokens,omitempty"`
	Temperature     float64  `json:"temperature,omitempty"`
	TopP            float64  `json:"topP,omitempty"`
	TopK            int      `json:"topK,omitempty"`
}

type generateContentResponse struct {
	Candidates     []generateContentCandidate `json:"candidates,omitempty"`
	PromptFeedback *promptFeedback            `json:"promptFeedback,omitempty"`
	Error          *palmApiError              `json:"error,omitempty"`
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
