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
	// TODO:  to be replaced with actual URL
	DefaultBaseURL               = "https://api.wcs.io"
	DefaultWCSModel              = "mixedbread-ai/mxbai-embed-large-v1"
	DefaultTruncate              = "right"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var (
	availableWCSModels = []string{
		"mixedbread-ai/mxbai-embed-large-v1",
		"intfloat/multilingual-e5-large-instruct",
		"Snowflake/snowflake-arctic-embed-s",
	}
	availableTruncates = []string{"left", "right"}
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultWCSModel)
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
	if !basesettings.ValidateSetting[string](model, availableWCSModels) {
		return errors.Errorf("wrong WCS model name, available model names are: %v", availableWCSModels)
	}
	truncate := cs.Truncate()
	if !basesettings.ValidateSetting[string](truncate, availableTruncates) {
		return errors.Errorf("wrong truncate type, available types are: %v", availableTruncates)
	}

	return nil
}
