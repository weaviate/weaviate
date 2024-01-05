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
)

const (
	modelProperty = "model"
)

var availableCohereModels = []string{
	"rerank-english-v2.0",
	"rerank-multilingual-v2.0",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultCohereModel = "rerank-multilingual-v2.0"
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultCohereModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Cohere model name, available model names are: %v", availableCohereModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name string, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("reranker-cohere")[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return &asString
		}
		var empty string
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableCohereModels, model)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultCohereModel)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
