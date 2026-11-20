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
	"errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	ModuleName     = "reranker-openai"
	DefaultBaseURL = "https://api.openai.com"
	DefaultModel   = "gpt-6-luna"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper(ModuleName)}
}

func (cs *classSettings) Validate(class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	return cs.propertyValuesHelper.ValidateBaseURL(cs.BaseURL())
}

func (cs *classSettings) BaseURL() string {
	return cs.getStringProperty("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.getStringProperty("model", DefaultModel)
}

func (cs *classSettings) getStringProperty(name string, defaultValue string) string {
	return cs.propertyValuesHelper.GetPropertyAsStringWithNotExists(cs.cfg, name, defaultValue, defaultValue)
}
