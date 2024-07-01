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

package ent

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL               = "https://api.voyageai.com/v1"
	DefaultVoyageAIModel         = "voyage-large-2"
	DefaultTruncate              = true
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var (
	availableVoyageAIModels = []string{
		"voyage-large-2",
		"voyage-code-2",
		"voyage-2",
		"voyage-law-2",
		"voyage-large-2-instruct",
		"voyage-finance-2",
		"voyage-multilingual-2",
	}
	experimetnalVoyageAIModels = []string{}
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultVoyageAIModel)
}

func (cs *classSettings) Truncate() bool {
	return cs.BaseClassSettings.GetPropertyAsBool("truncate", DefaultTruncate)
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	model := cs.Model()
	if !basesettings.ValidateSetting[string](model, append(availableVoyageAIModels, experimetnalVoyageAIModels...)) {
		return errors.Errorf("wrong VoyageAI model name, available model names are: %v", availableVoyageAIModels)
	}

	return nil
}
