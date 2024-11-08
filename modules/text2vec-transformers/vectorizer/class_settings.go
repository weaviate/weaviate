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

package vectorizer

import (
	"errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
	DefaultPoolingStrategy       = "masked_mean"
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (ic *classSettings) PoolingStrategy() string {
	return ic.BaseClassSettings.GetPropertyAsString("poolingStrategy", DefaultPoolingStrategy)
}

func (ic *classSettings) InferenceURL() string {
	return ic.getSetting("inferenceUrl")
}

func (ic *classSettings) PassageInferenceURL() string {
	return ic.getSetting("passageInferenceUrl")
}

func (ic *classSettings) QueryInferenceURL() string {
	return ic.getSetting("queryInferenceUrl")
}

func (ic *classSettings) getSetting(property string) string {
	return ic.BaseClassSettings.GetPropertyAsString(property, "")
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.BaseClassSettings.ValidateClassSettings(); err != nil {
		return err
	}
	if ic.InferenceURL() != "" && (ic.PassageInferenceURL() != "" || ic.QueryInferenceURL() != "") {
		return errors.New("either inferenceUrl or passageInferenceUrl together with queryInferenceUrl needs to be set, not both")
	}
	if ic.PassageInferenceURL() == "" && ic.QueryInferenceURL() != "" {
		return errors.New("queryInferenceUrl is set but passageInferenceUrl is empty, both needs to be set")
	}
	if ic.PassageInferenceURL() != "" && ic.QueryInferenceURL() == "" {
		return errors.New("passageInferenceUrl is set but queryInferenceUrl is empty, both needs to be set")
	}
	return nil
}
