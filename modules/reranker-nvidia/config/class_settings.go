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

package config

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL     = "https://ai.api.nvidia.com"
	DefaultNvidiaModel = "nvidia/rerank-qa-mistral-4b"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-nvidia")}
}

func (cs *classSettings) BaseURL() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "model", DefaultNvidiaModel)
}

func (ic *classSettings) Validate(class *models.Class) error {
	return nil
}
