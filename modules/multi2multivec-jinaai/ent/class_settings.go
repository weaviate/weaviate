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
	DefaultJinaAIModel        = "jina-embeddings-v4"
	DefaultVectorizeClassName = false
)

type classSettings struct {
	base *basesettings.BaseClassMultiModalSettings
	cfg  moduletools.ClassConfig
}

var fields = []string{basesettings.TextFieldsProperty, basesettings.ImageFieldsProperty}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:  cfg,
		base: basesettings.NewBaseClassMultiModalSettingsWithAltNames(cfg, false, "multi2multivec-jinaai", nil, nil),
	}
}

// JinaAI settings
func (cs *classSettings) Model() string {
	return cs.base.GetPropertyAsString(ModelProperty, DefaultJinaAIModel)
}

func (cs *classSettings) BaseURL() string {
	return cs.base.GetPropertyAsString(BaseURLProperty, DefaultBaseURL)
}

// CLIP module specific settings
func (ic *classSettings) ImageField(property string) bool {
	return ic.base.ImageField(property)
}

func (ic *classSettings) TextField(property string) bool {
	return ic.base.TextField(property)
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

	imageFields, imageFieldsOk := ic.base.GetSettings()[basesettings.ImageFieldsProperty]
	textFields, textFieldsOk := ic.base.GetSettings()[basesettings.TextFieldsProperty]
	if !imageFieldsOk && !textFieldsOk {
		errorMessages = append(errorMessages, "textFields or imageFields setting needs to be present")
	}

	if imageFieldsOk && textFieldsOk {
		errorMessages = append(errorMessages, "only one textFields or imageFields setting needs to be present, not both")
	}

	if imageFieldsOk {
		if errorMsgs := ic.validateField("image", imageFields); len(errorMsgs) > 0 {
			errorMessages = append(errorMessages, errorMsgs...)
		}
	}

	if textFieldsOk {
		if errorMsgs := ic.validateField("text", textFields); len(errorMsgs) > 0 {
			errorMessages = append(errorMessages, errorMsgs...)
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validateField(name string, fields interface{}) []string {
	var errorMessages []string
	fieldsCount, err := ic.validateFields(name, fields)
	if err != nil {
		errorMessages = append(errorMessages, err.Error())
	}
	if fieldsCount > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("only one %s property is allowed to define", name))
	}
	_, ok := ic.getWeights(name)
	if ok {
		errorMessages = append(errorMessages, "%s weights settigs are not allowed to define", name)
	}
	return errorMessages
}

func (ic *classSettings) validateFields(name string, fields interface{}) (int, error) {
	fieldsArray, ok := fields.([]interface{})
	if !ok {
		return 0, errors.Errorf("%sFields must be an array", name)
	}

	if len(fieldsArray) == 0 {
		return 0, errors.Errorf("must contain at least one %s field name in %sFields", name, name)
	}

	for _, value := range fieldsArray {
		v, ok := value.(string)
		if !ok {
			return 0, errors.Errorf("%sField must be a string", name)
		}
		if len(v) == 0 {
			return 0, errors.Errorf("%sField values cannot be empty", name)
		}
	}

	return len(fieldsArray), nil
}

func (ic *classSettings) getWeights(name string) ([]interface{}, bool) {
	weights, ok := ic.base.GetSettings()["weights"]
	if ok {
		weightsObject, ok := weights.(map[string]interface{})
		if ok {
			fieldWeights, ok := weightsObject[fmt.Sprintf("%sFields", name)]
			if ok {
				fieldWeightsArray, ok := fieldWeights.([]interface{})
				if ok {
					return fieldWeightsArray, ok
				}
			}
		}
	}

	return nil, false
}
