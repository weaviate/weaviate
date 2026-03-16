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
	BaseURLProperty = "baseURL"
	ModelProperty   = "model"
)

const (
	DefaultBaseURL            = ""
	DefaultWeaviateModel      = "ModernVBERT/colmodernvbert"
	DefaultVectorizeClassName = false
)

var fields = []string{basesettings.ImageFieldsProperty}

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2multivec-weaviate", nil, nil),
	}
}

func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(ModelProperty, DefaultWeaviateModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.base.GetPropertyAsString(BaseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) ImageField(property string) bool {
	return ic.base.ImageField(property)
}

func (ic *classSettings) Properties() ([]string, error) {
	return ic.base.VectorizableProperties(fields)
}

func (ic *classSettings) Validate() error {
	return ic.base.ValidateMultiModal(fields)
}
