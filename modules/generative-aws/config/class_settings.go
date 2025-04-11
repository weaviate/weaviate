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

package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	serviceProperty           = "service"
	regionProperty            = "region"
	modelProperty             = "model"
	endpointProperty          = "endpoint"
	targetModelProperty       = "targetModel"
	targetVariantProperty     = "targetVariant"
	maxTokenCountProperty     = "maxTokenCount"
	maxTokensToSampleProperty = "maxTokensToSample"
	stopSequencesProperty     = "stopSequences"
	temperatureProperty       = "temperature"
	topPProperty              = "topP"
	topKProperty              = "topK"
)

const (
	Bedrock   = "bedrock"
	Sagemaker = "sagemaker"
)

var (
	DefaultTitanMaxTokens     = 8192
	DefaultTitanStopSequences = []string{}
	DefaultTitanTemperature   = 0.0
	DefaultTitanTopP          = 1.0
	DefaultService            = Bedrock
)

var (
	DefaultAnthropicMaxTokensToSample = 300
	DefaultAnthropicStopSequences     = []string{"\\n\\nHuman:"}
	DefaultAnthropicTemperature       = 1.0
	DefaultAnthropicTopK              = 250
	DefaultAnthropicTopP              = 0.999
)

var DefaultAI21MaxTokens = 300

var (
	DefaultCohereMaxTokens   = 100
	DefaultCohereTemperature = 0.8
	DefaultAI21Temperature   = 0.7
	DefaultCohereTopP        = 1.0
)

var (
	DefaultMistralAIMaxTokens   = 200
	DefaultMistralAITemperature = 0.5
)

var (
	DefaultMetaMaxTokens   = 512
	DefaultMetaTemperature = 0.5
)

var availableAWSServices = []string{
	DefaultService,
	Sagemaker,
}

var availableBedrockModels = []string{
	"ai21.j2-ultra-v1",
	"ai21.j2-mid-v1",
	"amazon.titan-text-lite-v1",
	"amazon.titan-text-express-v1",
	"amazon.titan-text-premier-v1:0",
	"anthropic.claude-v2",
	"anthropic.claude-v2:1",
	"anthropic.claude-instant-v1",
	"anthropic.claude-3-sonnet-20240229-v1:0",
	"anthropic.claude-3-haiku-20240307-v1:0",
	"cohere.command-text-v14",
	"cohere.command-light-text-v14",
	"cohere.command-r-v1:0",
	"cohere.command-r-plus-v1:0",
	"meta.llama3-8b-instruct-v1:0",
	"meta.llama3-70b-instruct-v1:0",
	"meta.llama2-13b-chat-v1",
	"meta.llama2-70b-chat-v1",
	"mistral.mistral-7b-instruct-v0:2",
	"mistral.mixtral-8x7b-instruct-v0:1",
	"mistral.mistral-large-2402-v1:0",
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-aws")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	var errorMessages []string

	service := ic.Service()
	if service == "" || !ic.validatAvailableAWSSetting(service, availableAWSServices) {
		errorMessages = append(errorMessages, fmt.Sprintf("wrong %s, available services are: %v", serviceProperty, availableAWSServices))
	}

	if isBedrock(service) {
		model := ic.Model()
		if model != "" && !ic.validateAWSSetting(model, availableBedrockModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s: %s, available model names are: %v", modelProperty, model, availableBedrockModels))
		}

		maxTokenCount := ic.MaxTokenCount(Bedrock, model)
		if maxTokenCount != nil && (*maxTokenCount < 1 || *maxTokenCount > 8192) {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and 8096", maxTokenCountProperty))
		}
		temperature := ic.Temperature(Bedrock, model)
		if temperature != nil && (*temperature < 0 || *temperature > 1) {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", temperatureProperty))
		}
		topP := ic.TopP(Bedrock, model)
		if topP != nil && (*topP < 0 || *topP > 1) {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 0 and 1", topPProperty))
		}

		endpoint := ic.Endpoint()
		if endpoint != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s", endpoint, service))
		}
	}

	if isSagemaker(service) {
		endpoint := ic.Endpoint()
		if endpoint == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", endpointProperty))
		}
		model := ic.Model()
		if model != "" {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong configuration: %s, not applicable to %s. did you mean %s", modelProperty, service, targetModelProperty))
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validatAvailableAWSSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) validateAWSSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, name, defaultValue)
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, name, defaultValue)
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getListOfStringsProperty(name string, defaultValue []string) *[]string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
	if ok {
		asStringList, ok := model.([]string)
		if ok {
			return &asStringList
		}
		var empty []string
		return &empty
	}
	return &defaultValue
}

// AWS params
func (ic *classSettings) Service() string {
	return ic.getStringProperty(serviceProperty, DefaultService)
}

func (ic *classSettings) Region() string {
	return ic.getStringProperty(regionProperty, "")
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, "")
}

func (ic *classSettings) MaxTokenCount(service, model string) *int {
	if isBedrock(service) {
		if isAmazonModel(model) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultTitanMaxTokens)
		}
		if isAnthropicModel(model) {
			return ic.getIntProperty(maxTokensToSampleProperty, &DefaultAnthropicMaxTokensToSample)
		}
		if isAI21Model(model) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultAI21MaxTokens)
		}
		if isCohereModel(model) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultCohereMaxTokens)
		}
		if isMistralAIModel(model) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultMistralAIMaxTokens)
		}
		if isMetaModel(model) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultMetaMaxTokens)
		}
	}
	return ic.getIntProperty(maxTokenCountProperty, nil)
}

func (ic *classSettings) StopSequences(service, model string) []string {
	if isBedrock(service) {
		if isAmazonModel(model) {
			return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultTitanStopSequences)
		}
		if isAnthropicModel(model) {
			return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultAnthropicStopSequences)
		}
	}
	return *ic.getListOfStringsProperty(stopSequencesProperty, nil)
}

func (ic *classSettings) Temperature(service, model string) *float64 {
	if isBedrock(service) {
		if isAmazonModel(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultTitanTemperature)
		}
		if isAnthropicModel(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultAnthropicTemperature)
		}
		if isCohereModel(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultCohereTemperature)
		}
		if isAI21Model(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultAI21Temperature)
		}
		if isMistralAIModel(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultMistralAITemperature)
		}
		if isMetaModel(model) {
			return ic.getFloatProperty(temperatureProperty, &DefaultMetaTemperature)
		}
	}
	return ic.getFloatProperty(temperatureProperty, nil)
}

func (ic *classSettings) TopP(service, model string) *float64 {
	if isBedrock(service) {
		if isAmazonModel(model) {
			return ic.getFloatProperty(topPProperty, &DefaultTitanTopP)
		}
		if isAnthropicModel(model) {
			return ic.getFloatProperty(topPProperty, &DefaultAnthropicTopP)
		}
		if isCohereModel(model) {
			return ic.getFloatProperty(topPProperty, &DefaultCohereTopP)
		}
	}
	return ic.getFloatProperty(topPProperty, nil)
}

func (ic *classSettings) TopK(service, model string) *int {
	if isBedrock(service) {
		if isAnthropicModel(model) {
			return ic.getIntProperty(topKProperty, &DefaultAnthropicTopK)
		}
	}
	return ic.getIntProperty(topKProperty, nil)
}

func (ic *classSettings) Endpoint() string {
	return ic.getStringProperty(endpointProperty, "")
}

func (ic *classSettings) TargetModel() string {
	return ic.getStringProperty(targetModelProperty, "")
}

func (ic *classSettings) TargetVariant() string {
	return ic.getStringProperty(targetVariantProperty, "")
}

func isSagemaker(service string) bool {
	return service == Sagemaker
}

func isBedrock(service string) bool {
	return service == Bedrock
}

func isAmazonModel(model string) bool {
	return strings.HasPrefix(model, "amazon")
}

func isAI21Model(model string) bool {
	return strings.HasPrefix(model, "ai21")
}

func isAnthropicModel(model string) bool {
	return strings.HasPrefix(model, "anthropic")
}

func isCohereModel(model string) bool {
	return strings.HasPrefix(model, "cohere")
}

func isMistralAIModel(model string) bool {
	return strings.HasPrefix(model, "mistral")
}

func isMetaModel(model string) bool {
	return strings.HasPrefix(model, "meta")
}
