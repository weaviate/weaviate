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
	// Default values for model, baseURL and truncate cannot be changed before we solve how old classes
	// that have the defaults NOT set will handle the change
	DefaultBaseURL               = "https://api.cohere.ai"
	DefaultCohereModel           = "embed-multilingual-v3.0"
	DefaultTruncate              = "END"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false

	// Parameter keys for accessing the Parameters map
	ParamModel      = "Model"
	ParamBaseURL    = "BaseURL"
	ParamTruncate   = "Truncate"
	ParamDimensions = "Dimensions"
)

var availableTruncates = []string{"NONE", "START", "END", "LEFT", "RIGHT"}

// Parameters defines all configuration parameters for text2vec-cohere
var Parameters = map[string]basesettings.ParameterDef{
	ParamModel: {
		JSONKey:      "model",
		DefaultValue: DefaultCohereModel,
		Description:  "Cohere model name",
		Required:     false,
	},
	ParamBaseURL: {
		JSONKey:      "baseURL",
		DefaultValue: DefaultBaseURL,
		Description:  "Cohere API base URL",
		Required:     false,
	},
	ParamTruncate: {
		JSONKey:      "truncate",
		DefaultValue: DefaultTruncate,
		Description:  "Truncation strategy (NONE, START, END, LEFT, RIGHT)",
		Required:     false,
	},
	ParamDimensions: {
		JSONKey:      "dimensions",
		DefaultValue: nil,
		Description:  "Number of dimensions for the embedding",
		Required:     false,
	},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, LowerCaseInput)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString(Parameters[ParamModel].JSONKey, DefaultCohereModel)
}

func (cs *classSettings) Truncate() string {
	return cs.BaseClassSettings.GetPropertyAsString(Parameters[ParamTruncate].JSONKey, DefaultTruncate)
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString(Parameters[ParamBaseURL].JSONKey, DefaultBaseURL)
}

func (cs *classSettings) Dimensions() *int64 {
	return cs.BaseClassSettings.GetPropertyAsInt64(Parameters[ParamDimensions].JSONKey, nil)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	truncate := cs.Truncate()
	if !basesettings.ValidateSetting(truncate, availableTruncates) {
		return errors.Errorf("wrong %v parameter, available types are: %v", Parameters[ParamTruncate].JSONKey, availableTruncates)
	}

	return nil
}
