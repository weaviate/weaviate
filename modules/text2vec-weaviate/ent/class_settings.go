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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	// TODO: replace docker internal host with actual host
	DefaultBaseURL               = "https://embedding.labs.weaviate.io"
	DefaultWeaviateModel         = "Snowflake/snowflake-arctic-embed-m-v1.5"
	DefaultTruncate              = "right"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

const (
	SnowflakeArcticEmbedM = "Snowflake/snowflake-arctic-embed-m-v1.5"
)

var (
	availableWeaviateModels = []string{
		SnowflakeArcticEmbedM,
	}
	availableTruncates = []string{"left", "right"}
)

var SnowflakeArcticEmbedMDefaultDimensions int64 = 768

var availableWeaviateModelsDimensions = map[string][]int64{
	SnowflakeArcticEmbedM: {SnowflakeArcticEmbedMDefaultDimensions, 256},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
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

	return nil
}

func PickDefaultDimensions(model string) *int64 {
	if model == SnowflakeArcticEmbedM {
		return &SnowflakeArcticEmbedMDefaultDimensions
	}
	return nil
}
