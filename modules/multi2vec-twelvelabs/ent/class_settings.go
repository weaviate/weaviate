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

package ent

import (
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	baseURLProperty = "baseURL"
	modelProperty   = "model"
)

const (
	DefaultBaseURL               = "https://api.twelvelabs.io/v1.3"
	DefaultTwelveLabsModel       = "marengo3.0"
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty}

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-twelvelabs", nil, nil),
	}
}

// TwelveLabs settings
func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(modelProperty, DefaultTwelveLabsModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.base.GetPropertyAsString(baseURLProperty, DefaultBaseURL)
}

// Multi-modal field settings
func (ic *classSettings) ImageField(property string) bool {
	return ic.base.ImageField(property)
}

func (ic *classSettings) ImageFieldsWeights() ([]float32, error) {
	return ic.base.ImageFieldsWeights()
}

func (ic *classSettings) TextField(property string) bool {
	return ic.base.TextField(property)
}

func (ic *classSettings) TextFieldsWeights() ([]float32, error) {
	return ic.base.TextFieldsWeights()
}

func (ic *classSettings) Properties() ([]string, error) {
	return ic.base.VectorizableProperties(fields)
}

func (ic *classSettings) Validate() error {
	if err := ic.base.ValidateMultiModal(fields); err != nil {
		return err
	}
	if err := ic.base.ValidateBaseURL(ic.BaseURL()); err != nil {
		return err
	}
	return nil
}
