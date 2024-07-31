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
	// TODO: Check how Databricks embeddings model tokenize the text
	DefaultOpenAIModel           = "ada"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultOpenAIModel)
}

func (cs *classSettings) ServingURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("servingUrl", "")
}

func (cs *classSettings) Instruction() string {
	return cs.BaseClassSettings.GetPropertyAsString("instruction", "")
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	servingUrl := cs.ServingURL()
	if err := cs.ValidateServingURL(servingUrl); err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) ValidateServingURL(servingUrl string) error {
	if servingUrl == "" {
		return errors.New("servingUrl cannot be empty")
	}
	return nil
}
