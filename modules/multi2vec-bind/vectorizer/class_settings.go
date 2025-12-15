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

var fields = []string{
	basesettings.ImageFieldsProperty,
	basesettings.TextFieldsProperty,
	basesettings.AudioFieldsProperty,
	basesettings.VideoFieldsProperty,
	basesettings.ImuFieldsProperty,
	basesettings.ThermalFieldsProperty,
	basesettings.DepthFieldsProperty,
}

type classSettings struct {
	cfg  moduletools.ClassConfig
	base *basesettings.BaseClassMultiModalSettings
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-bind", nil, nil),
	}
}

// BIND module specific settings
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

func (ic *classSettings) AudioField(property string) bool {
	return ic.base.AudioField(property)
}

func (ic *classSettings) AudioFieldsWeights() ([]float32, error) {
	return ic.base.AudioFieldsWeights()
}

func (ic *classSettings) VideoField(property string) bool {
	return ic.base.VideoField(property)
}

func (ic *classSettings) VideoFieldsWeights() ([]float32, error) {
	return ic.base.VideoFieldsWeights()
}

func (ic *classSettings) IMUField(property string) bool {
	return ic.base.IMUField(property)
}

func (ic *classSettings) IMUFieldsWeights() ([]float32, error) {
	return ic.base.IMUFieldsWeights()
}

func (ic *classSettings) ThermalField(property string) bool {
	return ic.base.ThermalField(property)
}

func (ic *classSettings) ThermalFieldsWeights() ([]float32, error) {
	return ic.base.ThermalFieldsWeights()
}

func (ic *classSettings) DepthField(property string) bool {
	return ic.base.DepthField(property)
}

func (ic *classSettings) DepthFieldsWeights() ([]float32, error) {
	return ic.base.DepthFieldsWeights()
}

func (ic *classSettings) Properties() ([]string, error) {
	return ic.base.VectorizableProperties(fields)
}

func (ic *classSettings) Validate() error {
	return ic.base.ValidateMultiModal(fields)
}
