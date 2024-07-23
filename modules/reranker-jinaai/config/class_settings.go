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

const (
	DefaultJinaaiModel = "jina-reranker-v2-base-multilingual"
)

var availableJinaaiModels = []string{
	"jina-reranker-v2-base-multilingual",
	"jina-reranker-v1-base-en",
	"jina-reranker-v1-turbo-en",
	"jina-reranker-v1-tiny-en",
	"jina-colbert-v1-en",
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-jinaai")}
}

func (ic *classSettings) Model() string {
	return ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, modelProperty, "", DefaultJinaaiModel)
}

func (ic *classSettings) Validate(class *models.Class) error {
	model := ic.Model()
	if !basesettings.ValidateSetting(model, availableJinaaiModels) {
		return errors.Errorf("wrong Jinaai model name, available model names are: %v", availableJinaaiModels)
	}

	return nil
}
