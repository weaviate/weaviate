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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	modelProperty = "model"
)

var availableVoyageAIModels = []string{
	"rerank-2",
	"rerank-2-lite",
	"rerank-lite-1",
	"rerank-1",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultVoyageAIModel = "rerank-lite-1"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-voyageai")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultVoyageAIModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong VoyageAI model name, available model names are: %v", availableVoyageAIModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name string, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableVoyageAIModels, model)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultVoyageAIModel)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
