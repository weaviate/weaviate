//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL = "https://inference.do-ai.run"
	DefaultModel   = "bge-reranker-v2-m3"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-digitalocean")}
}

func (cs *classSettings) BaseURL() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "model", DefaultModel)
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.propertyValuesHelper.ValidateBaseURL(cs.BaseURL()); err != nil {
		return err
	}
	return nil
}
