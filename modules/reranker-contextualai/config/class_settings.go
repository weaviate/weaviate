//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	modelProperty       = "model"
	instructionProperty = "instruction"
	topNProperty        = "topN"
)

const (
	DefaultContextualAIModel = "ctxl-rerank-v2-instruct-multilingual"
)

var availableContextualAIModels = []string{
	"ctxl-rerank-v2-instruct-multilingual",
	"ctxl-rerank-v2-instruct-multilingual-mini",
	"ctxl-rerank-v1-instruct",
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-contextualai")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultContextualAIModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Contextual AI model name, available model names are: %v", availableContextualAIModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name string, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableContextualAIModels, model)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultContextualAIModel)
}

func (ic *classSettings) Instruction() string {
	return *ic.getStringProperty(instructionProperty, "")
}

func (ic *classSettings) TopN() int {
	defaultVal := 0
	result := ic.getIntProperty(topNProperty, &defaultVal)
	if result != nil {
		return *result
	}
	return 0
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
