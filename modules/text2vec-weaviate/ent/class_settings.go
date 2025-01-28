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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	// TODO: replace docker internal host with actual host
	DefaultBaseURL               = "https://api.embedding.weaviate.io"
	DefaultWeaviateModel         = SnowflakeArcticEmbedL
	DefaultTruncate              = "right"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
)

const (
	SnowflakeArcticEmbedL = "Snowflake/snowflake-arctic-embed-l-v2.0"
	SnowflakeArcticEmbedM = "Snowflake/snowflake-arctic-embed-m-v1.5"
)

var availableModels = []string{
	SnowflakeArcticEmbedL,
	SnowflakeArcticEmbedM,
}

var (
	SnowflakeArcticEmbedLDefaultDimensions int64 = 1024
	SnowflakeArcticEmbedMDefaultDimensions int64 = 768
)

var availableDimensions = map[string][]int64{
	SnowflakeArcticEmbedL: {256, SnowflakeArcticEmbedLDefaultDimensions},
	SnowflakeArcticEmbedM: {256, SnowflakeArcticEmbedMDefaultDimensions},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, LowerCaseInput)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultWeaviateModel)
}

func (cs *classSettings) Truncate() string {
	return cs.BaseClassSettings.GetPropertyAsString("truncate", DefaultTruncate)
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Dimensions() *int64 {
	defaultValue := PickDefaultDimensions(cs.Model())
	return cs.BaseClassSettings.GetPropertyAsInt64("dimensions", defaultValue)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	model := cs.Model()
	if !basesettings.ValidateSetting[string](model, availableModels) {
		return fmt.Errorf("wrong model name, available model names are: %v", availableModels)
	}

	dimensions := cs.Dimensions()
	if dimensions != nil {
		availableModelDimensions := availableDimensions[model]
		if !basesettings.ValidateSetting[int64](*dimensions, availableModelDimensions) {
			return fmt.Errorf("wrong dimensions setting for %s model, available dimensions are: %v", model, availableModelDimensions)
		}
	}

	return nil
}

func PickDefaultDimensions(model string) *int64 {
	if model == SnowflakeArcticEmbedL {
		return &SnowflakeArcticEmbedLDefaultDimensions
	}
	if model == SnowflakeArcticEmbedM {
		return &SnowflakeArcticEmbedMDefaultDimensions
	}
	return nil
}
