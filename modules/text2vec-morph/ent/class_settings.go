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
	DefaultMorphDocumentType     = "text"
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

var (
	MorphEmbeddingV2DefaultDimensions int64 = 1536
	MorphEmbeddingV3DefaultDimensions int64 = 1024
)

var availableMorphTypes = []string{"text", "code"}

var availableMorphModels = []string{
	// Morph embedding models
	MorphEmbeddingV2,
	MorphEmbeddingV3,
}

var availableMorphModelsDimensions = map[string][]int64{
	MorphEmbeddingV2: {MorphEmbeddingV2DefaultDimensions},
	MorphEmbeddingV3: {MorphEmbeddingV3DefaultDimensions},
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

func (cs *classSettings) Type() string {
	return cs.GetPropertyAsString("type", DefaultMorphDocumentType)
}

func (cs *classSettings) Dimensions() *int64 {
	defaultValue := PickDefaultDimensions(cs.Model())
	return cs.GetPropertyAsInt64("dimensions", defaultValue)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	docType := cs.Type()
	if !basesettings.ValidateSetting(docType, availableMorphTypes) {
		return errors.Errorf("wrong Morph type name, available type names are: %v", availableMorphTypes)
	}

	model := cs.Model()
	if !basesettings.ValidateSetting(model, availableMorphModels) {
		return errors.Errorf("wrong Morph model name, available model names are: %v", availableMorphModels)
	}

	dimensions := cs.Dimensions()
	if dimensions != nil {
		availableDimensions := availableMorphModelsDimensions[model]
		if len(availableDimensions) > 0 && !basesettings.ValidateSetting(*dimensions, availableDimensions) {
			return errors.Errorf("wrong dimensions setting for %s model, available dimensions are: %v", model, availableDimensions)
		}
	}

	return nil
}

func PickDefaultDimensions(model string) *int64 {
	if model == MorphEmbeddingV2 {
		return &MorphEmbeddingV2DefaultDimensions
	}
	if model == MorphEmbeddingV3 {
		return &MorphEmbeddingV3DefaultDimensions
	}
	return nil
}
