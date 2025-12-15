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
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	BaseURLProperty = "baseURL"
	ModelProperty   = "model"
)

const (
	DefaultBaseURL            = "https://api.jina.ai"
	DefaultJinaAIModel        = "jina-clip-v2"
	DefaultVectorizeClassName = false
)

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty}

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2vec-jinaai", nil, nil),
	}
}

// JinaAI settings
func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(ModelProperty, DefaultJinaAIModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.base.GetPropertyAsString(BaseURLProperty, DefaultBaseURL)
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
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	var errorMessages []string

	if err := ic.base.ValidateMultiModal(fields); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	if ic.Dimensions() != nil {
		if ic.Model() == DefaultJinaAIModel && (*ic.Dimensions() < 64 || *ic.Dimensions() > 1024) {
			errorMessages = append(errorMessages, "dimensions needs to within [64, 1024] range")
		}
		if ic.Model() == "jina-clip-v1" && *ic.Dimensions() != 768 {
			errorMessages = append(errorMessages, "dimensions needs to equal 768")
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}
