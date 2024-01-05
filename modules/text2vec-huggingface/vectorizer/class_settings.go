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

package vectorizer

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	DefaultHuggingFaceModel      = "sentence-transformers/msmarco-bert-base-dot-v5"
	DefaultOptionWaitForModel    = false
	DefaultOptionUseGPU          = false
	DefaultOptionUseCache        = true
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (cs *classSettings) PropertyIndexed(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPropertyIndexed
	}

	vcn, ok := cs.cfg.Property(propName)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (cs *classSettings) VectorizePropertyName(propName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizePropertyName
	}
	vcn, ok := cs.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (cs *classSettings) EndpointURL() string {
	return cs.getEndpointURL()
}

func (cs *classSettings) PassageModel() string {
	model := cs.getPassageModel()
	if model == "" {
		return DefaultHuggingFaceModel
	}
	return model
}

func (cs *classSettings) QueryModel() string {
	model := cs.getQueryModel()
	if model == "" {
		return DefaultHuggingFaceModel
	}
	return model
}

func (cs *classSettings) OptionWaitForModel() bool {
	return cs.getOptionOrDefault("waitForModel", DefaultOptionWaitForModel)
}

func (cs *classSettings) OptionUseGPU() bool {
	return cs.getOptionOrDefault("useGPU", DefaultOptionUseGPU)
}

func (cs *classSettings) OptionUseCache() bool {
	return cs.getOptionOrDefault("useCache", DefaultOptionUseCache)
}

func (cs *classSettings) VectorizeClassName() bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizeClassName
	}

	vcn, ok := cs.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	err := cs.validateClassSettings()
	if err != nil {
		return err
	}

	err = cs.validateIndexState(class, cs)
	if err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) validateClassSettings() error {
	endpointURL := cs.getEndpointURL()
	if endpointURL != "" {
		// endpoint is set, should be used for feature extraction
		// all other settings are not relevant
		return nil
	}

	model := cs.getProperty("model")
	passageModel := cs.getProperty("passageModel")
	queryModel := cs.getProperty("queryModel")

	if model != "" && (passageModel != "" || queryModel != "") {
		return errors.New("only one setting must be set either 'model' or 'passageModel' with 'queryModel'")
	}

	if model == "" {
		if passageModel != "" && queryModel == "" {
			return errors.New("'passageModel' is set, but 'queryModel' is empty")
		}
		if passageModel == "" && queryModel != "" {
			return errors.New("'queryModel' is set, but 'passageModel' is empty")
		}
	}
	return nil
}

func (cs *classSettings) getPassageModel() string {
	model := cs.getProperty("model")
	if model == "" {
		model = cs.getProperty("passageModel")
	}
	return model
}

func (cs *classSettings) getQueryModel() string {
	model := cs.getProperty("model")
	if model == "" {
		model = cs.getProperty("queryModel")
	}
	return model
}

func (cs *classSettings) getEndpointURL() string {
	endpointURL := cs.getProperty("endpointUrl")
	if endpointURL == "" {
		endpointURL = cs.getProperty("endpointURL")
	}
	return endpointURL
}

func (cs *classSettings) getOption(option string) *bool {
	if cs.cfg != nil {
		options, ok := cs.cfg.Class()["options"]
		if ok {
			asMap, ok := options.(map[string]interface{})
			if ok {
				option, ok := asMap[option]
				if ok {
					asBool, ok := option.(bool)
					if ok {
						return &asBool
					}
				}
			}
		}
	}
	return nil
}

func (cs *classSettings) getOptionOrDefault(option string, defaultValue bool) bool {
	optionValue := cs.getOption(option)
	if optionValue != nil {
		return *optionValue
	}
	return defaultValue
}

func (cs *classSettings) getProperty(name string) string {
	if cs.cfg != nil {
		model, ok := cs.cfg.Class()[name]
		if ok {
			asString, ok := model.(string)
			if ok {
				return asString
			}
		}
	}
	return ""
}

func (cs *classSettings) validateIndexState(class *models.Class, settings ClassSettings) error {
	if settings.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if settings.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing.")
}
