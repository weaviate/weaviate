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

func buildURL(service, region, model string) string {
	urlTemplate := "https://%s.%s.amazonaws.com/model/%s/invoke"
	return fmt.Sprintf(urlTemplate, service, region, model)
}

type aws struct {
	awsAccessKey string
	awsSecretKey string
	buildUrlFn   func(service, region, model string) string
	httpClient   *http.Client
	logger       logrus.FieldLogger
}

func New(awsAccessKey string, awsSecretKey string, logger logrus.FieldLogger) *aws {
	return &aws{
		awsAccessKey: awsAccessKey,
		awsSecretKey: awsSecretKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		buildUrlFn: buildURL,
		logger:     logger,
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

	body, err := json.Marshal(generateInput{
		InputText: prompt,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	service := settings.Service()
	region := settings.Region()
	model := settings.Model()
	host := service + "." + region + ".amazonaws.com"

	endpoint := v.buildUrlFn(service, region, model)

	accessKey, err := v.getAwsAccessKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Access Key")
	}
	secretKey, err := v.getAwsAccessSecret(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "AWS Secret Key")
	}

	amzDate, headers, authorizationHeader := getAuthHeader(accessKey, secretKey, model, region, service, host, body)

	headers["Authorization"] = authorizationHeader
	headers["x-amz-date"] = amzDate

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
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

	var resBody generateResponse
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

	if len(resBody.Results) > 0 && len(resBody.Results[0].CompletionReason) > 0 {
		content := resBody.Results[0].OutputText
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
	if len(v.awsSecretKey) > 0 {
		return v.awsSecretKey, nil
	}
	awsAccessSecret := ctx.Value("X-Aws-Access-Secret")
	if awsAccessSecretHeader, ok := awsAccessSecret.([]string); ok &&
		len(awsAccessSecretHeader) > 0 && len(awsAccessSecretHeader[0]) > 0 {
		return awsAccessSecretHeader[0], nil
	}
	return "", errors.New("no secret found " +
		"neither in request header: X-Aws-Access-Secret " +
		"nor in environment variable under AWS_SECRET_ACCESS_KEY")
}

type generateInput struct {
	InputText            string                `json:"inputText,omitempty"`
	TextGenerationConfig *textGenerationConfig `json:"textGenerationConfig,omitempty"`
}

type textGenerationConfig struct {
	MaxTokenCount int      `json:"maxTokenCount"`
	StopSequences []string `json:"stopSequences"`
	Temperature   float64  `json:"temperature"`
	TopP          int      `json:"topP"`
}

type generateResponse struct {
	InputTextTokenCount int      `json:"InputTextTokenCount,omitempty"`
	Results             []Result `json:"results,omitempty"`
	Message             *string  `json:"message,omitempty"`
}

type Result struct {
	TokenCount       int    `json:"tokenCount,omitempty"`
	OutputText       string `json:"outputText,omitempty"`
	CompletionReason string `json:"completionReason,omitempty"`
}
