//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ent

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
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
)

var (
	huggingFaceAPIHosts = []string{"huggingface.co", "router.huggingface.co", "api-inference.huggingface.co"}
	// A dedicated Inference Endpoint, such as abc123.us-east-1.aws.endpoints.huggingface.cloud.
	inferenceEndpointHostPattern = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]*[a-z0-9])?\.)+endpoints\.huggingface\.cloud$`)
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettingsWithCustomModel(cfg, LowerCaseInput, "passageModel")}
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
	if err := cs.validateEndpointURL(); err != nil {
		return err
	}
	if err := cs.BaseClassSettings.Validate(class); err != nil {
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

	if model != "" && passageModel != "" {
		return errors.New("only one setting must be set either 'model' or 'passageModel'")
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

// validateEndpointURL accepts an endpointURL on a Hugging Face host over https on the default port, which needs no
// listing, and checks any other host against the operator's allow-list. The module has no default endpointURL.
func (cs *classSettings) validateEndpointURL() error {
	endpointURL := cs.EndpointURL()
	if parsed, err := url.Parse(endpointURL); err == nil && parsed.Scheme == "https" && parsed.Port() == "" {
		host := strings.ToLower(parsed.Hostname())
		if slices.Contains(huggingFaceAPIHosts, host) || inferenceEndpointHostPattern.MatchString(host) {
			return nil
		}
	}
	if err := modulecomponents.ValidateCollectionBaseURL(endpointURL); err != nil {
		return fmt.Errorf("endpointURL: %w", err)
	}
	return nil
}
