//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
)

const (
	DefaultOpenAIModel            = "text-ada-001"
	DefaultOpenAITemperature      = 0.0
	DefaultOpenAIMaxTokens        = 16
	DefaultOpenAIFrequencyPenalty = 0.0
	DefaultOpenAIPresencePenalty  = 0.0
	DefaultOpenAITopP             = 1.0
)

var maxTokensForModel = map[string]int64{
	"text-ada-001":     2048,
	"text-babbage-001": 2048,
	"text-curie-001":   2048,
	"text-davinci-002": 4000,
}
var availableOpenAIModels = []string{"text-ada-001", "text-babbage-001", "text-curie-001", "text-davinci-002"}

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

	model := ic.getStringProperty("model", DefaultOpenAIModel)
	if model == nil || !ic.validateOpenAISetting(*model, availableOpenAIModels) {
		return errors.Errorf("wrong OpenAI model name, available model names are: %v", availableOpenAIModels)
	}

	temperature := ic.getFloatProperty("temperature", DefaultOpenAITemperature)
	if temperature == nil || (*temperature < 0 || *temperature > 1) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0")
	}

	maxTokens := ic.getIntProperty("maxTokens", DefaultOpenAIMaxTokens)
	if maxTokens == nil || (*maxTokens < 0 || *maxTokens > getMaxTokensForModel(*model)) {
		return errors.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used")
	}

	frequencyPenalty := ic.getFloatProperty("frequencyPenalty", DefaultOpenAIFrequencyPenalty)
	if frequencyPenalty == nil || (*frequencyPenalty < 0 || *frequencyPenalty > 1) {
		return errors.Errorf("Wrong frequencyPenalty configuration, values are between 0.0 and 1.0")
	}

	presencePenalty := ic.getFloatProperty("presencePenalty", DefaultOpenAIPresencePenalty)
	if presencePenalty == nil || (*presencePenalty < 0 || *presencePenalty > 1) {
		return errors.Errorf("Wrong presencePenalty configuration, values are between 0.0 and 1.0")
	}

	topP := ic.getIntProperty("topP", DefaultOpenAITopP)
	if topP == nil || (*topP < 0 || *topP > 5) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5")
	}

	err := ic.validateIndexState(class)
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("qna-openai")[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return &asString
		}
		return nil
	}
	return &defaultValue
}

func (ic *classSettings) getIntProperty(name string, defaultValue int64) *int64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("qna-openai")[name]
	if ok {
		intValue, ok := model.(int)
		if ok {
			i := int64(intValue)
			return &i
		}
		asNumber, ok := model.(json.Number)
		if ok {
			asInt, _ := asNumber.Int64()
			return &asInt
		}

		return nil
	}

	return &defaultValue
}

func (ic *classSettings) getFloatProperty(name string, defaultValue float64) *float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("qna-openai")[name]
	if ok {
		asFloat, ok := model.(float64)
		if ok {
			return &asFloat
		}
		asNumber, ok := model.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			return &asFloat
		}

		return nil
	}

	return &defaultValue
}

func getMaxTokensForModel(model string) int64 {
	return maxTokensForModel[model]
}

func (ic *classSettings) validateOpenAISetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) validateIndexState(class *models.Class) error {
	//if settings.VectorizeClassName() {
	//	// if the user chooses to vectorize the classname, vector-building will
	//	// always be possible, no need to investigate further
	//
	//	return nil
	//}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeString) &&
			prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		//if settings.PropertyIndexed(prop.Name) {
		//	// found at least one, this is a valid schema
		//	return nil
		//}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing.")

	// IndexCheck returns whether a property of a class should be indexed
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty("model", DefaultOpenAIModel)
}

func (ic *classSettings) MaxTokens() int64 {
	return *ic.getIntProperty("maxTokens", DefaultOpenAIMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty("temperature", DefaultOpenAITemperature)
}

func (ic *classSettings) FrequencyPenalty() float64 {
	return *ic.getFloatProperty("frequencyPenalty", DefaultOpenAIFrequencyPenalty)
}

func (ic *classSettings) PresencePenalty() float64 {
	return *ic.getFloatProperty("presencePenalty", DefaultOpenAIPresencePenalty)
}

func (ic *classSettings) TopP() int64 {
	return *ic.getIntProperty("topP", DefaultOpenAITopP)
}
