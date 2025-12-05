//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ent

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	// Default values for model, WaitForModel, useGPU and useCache cannot be changed before we solve how old classes
	// that have the defaults NOT set will handle the change
	DefaultHuggingFaceModel      = "sentence-transformers/msmarco-bert-base-dot-v5"
	DefaultOptionWaitForModel    = false
	DefaultOptionUseGPU          = false
	DefaultOptionUseCache        = true
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false

	// Parameter keys for accessing the Parameters map
	ParamEndpointURL  = "EndpointURL"
	ParamModel        = "Model"
	ParamWaitForModel = "WaitForModel"
	ParamUseGPU       = "UseGPU"
	ParamUseCache     = "UseCache"
)

// Parameters defines all configuration parameters for text2vec-huggingface
var Parameters = map[string]basesettings.ParameterDef{
	ParamEndpointURL: {
		JSONKey:       "endpointUrl",
		AlternateKeys: []string{"endpointURL"}, // Backwards compatibility
		DefaultValue:  "",
		Description:   "Custom endpoint URL for HuggingFace inference",
		Required:      false,
		DataType:      "string",
	},
	ParamModel: {
		JSONKey:       "model",
		AlternateKeys: []string{"passageModel"}, // Backwards compatibility
		DefaultValue:  DefaultHuggingFaceModel,
		Description:   "HuggingFace model name; passageModel is an alternate key for backward compatibility",
		Required:      false,
		DataType:      "string",
	},
	ParamWaitForModel: {
		JSONKey:      "waitForModel",
		DefaultValue: DefaultOptionWaitForModel,
		Description:  "Wait for model to be ready (options.waitForModel)",
		Required:     false,
		DataType:     "bool",
	},
	ParamUseGPU: {
		JSONKey:      "useGPU",
		DefaultValue: DefaultOptionUseGPU,
		Description:  "Use GPU for inference (options.useGPU)",
		Required:     false,
		DataType:     "bool",
	},
	ParamUseCache: {
		JSONKey:      "useCache",
		DefaultValue: DefaultOptionUseCache,
		Description:  "Use caching for inference results (options.useCache)",
		Required:     false,
		DataType:     "bool",
	},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettingsWithCustomModel(cfg, LowerCaseInput, "passageModel")}
}

func (cs *classSettings) EndpointURL() string {
	return cs.getPropertyWithAlternates(ParamEndpointURL)
}

func (cs *classSettings) PassageModel() string {
	model := cs.getPropertyWithAlternates(ParamModel)
	if model == "" {
		return DefaultHuggingFaceModel
	}
	return model
}

func (cs *classSettings) OptionWaitForModel() bool {
	return cs.getOptionOrDefault(Parameters[ParamWaitForModel].JSONKey, DefaultOptionWaitForModel)
}

func (cs *classSettings) OptionUseGPU() bool {
	return cs.getOptionOrDefault(Parameters[ParamUseGPU].JSONKey, DefaultOptionUseGPU)
}

func (cs *classSettings) OptionUseCache() bool {
	return cs.getOptionOrDefault(Parameters[ParamUseCache].JSONKey, DefaultOptionUseCache)
}

// getPropertyWithAlternates tries primary JSONKey first, then falls back to AlternateKeys
func (cs *classSettings) getPropertyWithAlternates(paramKey string) string {
	param := Parameters[paramKey]

	// Try primary key first
	if val := cs.getProperty(param.JSONKey); val != "" {
		return val
	}

	// Fall back to alternate keys
	for _, altKey := range param.AlternateKeys {
		if val := cs.getProperty(altKey); val != "" {
			return val
		}
	}

	return ""
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	// Custom validation: endpoint takes precedence
	endpointURL := cs.EndpointURL()
	if endpointURL != "" {
		// endpoint is set, should be used for feature extraction
		// all other settings are not relevant
		return nil
	}

	// Custom validation: model and passageModel are mutually exclusive
	model := cs.getProperty(Parameters[ParamModel].JSONKey)
	passageModel := cs.getProperty(Parameters[ParamModel].AlternateKeys[0]) // "passageModel"

	if model != "" && passageModel != "" {
		return errors.New("only one setting must be set either 'model' or 'passageModel'")
	}

	return nil
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
