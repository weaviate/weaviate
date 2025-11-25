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

package ent

import (
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	modelProperty      = "model"
	regionProperty     = "region"
	dimensionsProperty = "dimensions"
)

const (
	DefaultBedrockModel          = "amazon.titan-embed-image-v1"
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
)

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty}

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, LowerCaseInput, "multi2vec-aws", nil, nil),
	}
}

// AWS settings
func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(modelProperty, DefaultBedrockModel)
}

func (cs *classSettings) Region() string {
	return cs.base.GetPropertyAsString(regionProperty, "")
}

func (cs *classSettings) Dimensions() *int64 {
	return cs.base.GetPropertyAsInt64(dimensionsProperty, nil)
}

// CLIP module specific settings
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
	return ic.base.ValidateMultiModal(fields)
}
