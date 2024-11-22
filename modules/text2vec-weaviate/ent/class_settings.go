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
	DefaultWeaviateModel         = SnowflakeArcticEmbedM
	DefaultTruncate              = "right"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
)

const (
	SnowflakeArcticEmbedM = "Snowflake/snowflake-arctic-embed-m-v1.5"
)

var SnowflakeArcticEmbedMDefaultDimensions int64 = 768

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

	if cs.Model() == SnowflakeArcticEmbedM {
		if err := cs.ValidateSnowflakeArctic(); err != nil {
			return err
		}
	}

	return nil
}

func (cs *classSettings) ValidateSnowflakeArctic() error {
	if cs.Dimensions() != nil && *cs.Dimensions() != 256 && *cs.Dimensions() != 768 {
		return fmt.Errorf("available dimensions for model %v are: [256 768]. Got %v", cs.Model(), *cs.Dimensions())
	}

	return nil
}

func PickDefaultDimensions(model string) *int64 {
	if model == SnowflakeArcticEmbedM {
		return &SnowflakeArcticEmbedMDefaultDimensions
	}
	return nil
}
