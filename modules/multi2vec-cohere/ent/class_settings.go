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
	baseURLProperty  = "baseURL"
	modelProperty    = "model"
	truncateProperty = "truncate"
)

const (
	DefaultBaseURL               = "https://api.cohere.com"
	DefaultCohereModel           = "embed-multilingual-v3.0"
	DefaultTruncate              = "END"
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
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-cohere", nil, nil),
	}
}

// Cohere settings
func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(modelProperty, DefaultCohereModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.base.GetPropertyAsString(baseURLProperty, DefaultBaseURL)
}

func (cs *classSettings) Truncate() string {
	return cs.base.GetPropertyAsString(truncateProperty, DefaultTruncate)
}

func (cs *classSettings) Dimensions() *int64 {
	return cs.base.GetPropertyAsInt64("dimensions", nil)
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
