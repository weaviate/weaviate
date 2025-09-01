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
	DefaultMorphModel            = "morph-embedding-v3"
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultBaseURL               = "https://api.morphllm.com"
	LowerCaseInput               = false
)

const (
	MorphEmbeddingV2 = "morph-embedding-v2"
	MorphEmbeddingV3 = "morph-embedding-v3"
)

var availableMorphModels = []string{
	// Morph embedding models
	MorphEmbeddingV2,
	MorphEmbeddingV3,
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (cs *classSettings) BaseURL() string {
	return cs.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.GetPropertyAsString("model", DefaultMorphModel)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}
	model := cs.Model()
	if !basesettings.ValidateSetting(model, availableMorphModels) {
		return errors.Errorf("wrong Morph model name, available model names are: %v", availableMorphModels)
	}
	return nil
}
