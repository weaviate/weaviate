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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
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

var (
	DefaultTitanMaxTokens     = 8192
	DefaultTitanStopSequences = []string{}
	DefaultTitanTemperature   = 0.0
	DefaultTitanTopP          = 1.0
	DefaultService            = "bedrock"
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

var availableAWSServices = []string{
	DefaultService,
}

var availableBedrockModels = []string{
	"cohere.command-text-v14",
	"cohere.command-light-text-v14",
}

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
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
	region := ic.Region()
	if region == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", regionProperty))
	}

	if isBedrock(service) {
		model := ic.Model()
		if model == "" && !ic.validateAWSSetting(model, availableBedrockModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s: %s, available model names are: %v", modelProperty, model, availableBedrockModels))
		}

		maxTokenCount := ic.MaxTokenCount()
		if *maxTokenCount < 1 || *maxTokenCount > 8192 {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and 8096", maxTokenCountProperty))
		}
		temperature := ic.Temperature()
		if *temperature < 0 || *temperature > 1 {
			errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", temperatureProperty))
		}
		topP := ic.TopP()
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
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
	if ok {
		asString, ok := value.(string)
		if ok {
			return asString
		}
	}
	return defaultValue
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
	if ok {
		asFloat, ok := val.(float64)
		if ok {
			return &asFloat
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			return &asFloat
		}
		asInt, ok := val.(int)
		if ok {
			asFloat := float64(asInt)
			return &asFloat
		}
	}

	return defaultValue
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
	if ok {
		asInt, ok := val.(int)
		if ok {
			return &asInt
		}
		asFloat, ok := val.(float64)
		if ok {
			asInt := int(asFloat)
			return &asInt
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			asInt := int(asFloat)
			return &asInt
		}
		wrongVal := -1
		return &wrongVal
	}

	if defaultValue != nil {
		return defaultValue
	}
	return nil
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

func (ic *classSettings) MaxTokenCount() *int {
	if isBedrock(ic.Service()) {
		if isAmazonModel(ic.Model()) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultTitanMaxTokens)
		}
		if isAnthropicModel(ic.Model()) {
			return ic.getIntProperty(maxTokensToSampleProperty, &DefaultAnthropicMaxTokensToSample)
		}
		if isAI21Model(ic.Model()) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultAI21MaxTokens)
		}
		if isCohereModel(ic.Model()) {
			return ic.getIntProperty(maxTokenCountProperty, &DefaultCohereMaxTokens)
		}
	}
	return ic.getIntProperty(maxTokenCountProperty, nil)
}

func (ic *classSettings) StopSequences() []string {
	if isBedrock(ic.Service()) {
		if isAmazonModel(ic.Model()) {
			return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultTitanStopSequences)
		}
		if isAnthropicModel(ic.Model()) {
			return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultAnthropicStopSequences)
		}
	}
	return *ic.getListOfStringsProperty(stopSequencesProperty, nil)
}

func (ic *classSettings) Temperature() *float64 {
	if isBedrock(ic.Service()) {
		if isAmazonModel(ic.Model()) {
			return ic.getFloatProperty(temperatureProperty, &DefaultTitanTemperature)
		}
		if isAnthropicModel(ic.Model()) {
			return ic.getFloatProperty(temperatureProperty, &DefaultAnthropicTemperature)
		}
		if isCohereModel(ic.Model()) {
			return ic.getFloatProperty(temperatureProperty, &DefaultCohereTemperature)
		}
		if isAI21Model(ic.Model()) {
			return ic.getFloatProperty(temperatureProperty, &DefaultAI21Temperature)
		}
	}
	return ic.getFloatProperty(temperatureProperty, nil)
}

func (ic *classSettings) TopP() *float64 {
	if isBedrock(ic.Service()) {
		if isAmazonModel(ic.Model()) {
			return ic.getFloatProperty(topPProperty, &DefaultTitanTopP)
		}
		if isAnthropicModel(ic.Model()) {
			return ic.getFloatProperty(topPProperty, &DefaultAnthropicTopP)
		}
		if isCohereModel(ic.Model()) {
			return ic.getFloatProperty(topPProperty, &DefaultCohereTopP)
		}
	}
	return ic.getFloatProperty(topPProperty, nil)
}

func (ic *classSettings) TopK() *int {
	if isBedrock(ic.Service()) {
		if isAnthropicModel(ic.Model()) {
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
	return service == "sagemaker"
}

func isBedrock(service string) bool {
	return service == "bedrock"
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
