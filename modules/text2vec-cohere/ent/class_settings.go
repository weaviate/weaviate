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
	DefaultBaseURL               = "https://api.cohere.ai"
	DefaultCohereModel           = "embed-multilingual-v3.0"
	DefaultTruncate              = "END"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var (
	availableCohereModels = []string{
		"medium",
		"large", "small", "multilingual-22-12",
		"embed-english-v2.0", "embed-english-light-v2.0", "embed-multilingual-v2.0",
		"embed-english-v3.0", "embed-english-light-v3.0", "embed-multilingual-v3.0", "embed-multilingual-light-v3.0",
	}
	experimetnalCohereModels = []string{"multilingual-2210-alpha"}
	availableTruncates       = []string{"NONE", "START", "END", "LEFT", "RIGHT"}
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultCohereModel)
}

func (cs *classSettings) Truncate() string {
	return cs.BaseClassSettings.GetPropertyAsString("truncate", DefaultTruncate)
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	model := cs.Model()
	if !basesettings.ValidateSetting[string](model, append(availableCohereModels, experimetnalCohereModels...)) {
		return errors.Errorf("wrong Cohere model name, available model names are: %v", availableCohereModels)
	}
	truncate := cs.Truncate()
	if !basesettings.ValidateSetting[string](truncate, availableTruncates) {
		return errors.Errorf("wrong truncate type, available types are: %v", availableTruncates)
	}

	return nil
}
