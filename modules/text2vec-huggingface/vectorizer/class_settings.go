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
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
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
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
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

func (cs *classSettings) Validate(class *models.Class) error {
	return cs.BaseClassSettings.Validate(class)
}

func (cs *classSettings) validateClassSettings() error {
	if err := cs.BaseClassSettings.ValidateClassSettings(); err != nil {
		return err
	}

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
	return cs.BaseClassSettings.GetPropertyAsString(name, "")
}
