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
	modelProperty = "model"
)

const (
	DefaultBaseURL     = "https://api.jina.ai"
	DefaultJinaaiModel = "jina-reranker-v2-base-multilingual"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-jinaai")}
}

func (ic *classSettings) BaseURL() string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, "baseURL", DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, modelProperty, "", DefaultJinaaiModel)
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.propertyValuesHelper.ValidateBaseURL(ic.BaseURL()); err != nil {
		return err
	}
	return nil
}
