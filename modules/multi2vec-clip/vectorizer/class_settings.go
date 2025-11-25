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

package vectorizer

import (
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

type classSettings struct {
	cfg  moduletools.ClassConfig
	base *basesettings.BaseClassMultiModalSettings
}

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-clip", nil, nil),
	}
}

func (ic *classSettings) InferenceURL() string {
	return ic.base.GetPropertyAsString("inferenceUrl", "")
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
