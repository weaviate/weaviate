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
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (ic *classSettings) PoolingStrategy() string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPoolingStrategy
	}

	vcn, ok := ic.cfg.Class()["poolingStrategy"]
	if !ok {
		return DefaultPoolingStrategy
	}

	asString, ok := vcn.(string)
	if !ok {
		return DefaultPoolingStrategy
	}

	return asString
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
	if ic.cfg == nil {
		return ""
	}

	url, ok := ic.cfg.Class()[property]
	if !ok {
		return ""
	}

	asString, ok := url.(string)
	if !ok {
		return ""
	}

	return asString
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.BaseClassSettings.Validate(); err != nil {
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
	return ic.BaseClassSettings.Validate()
}
