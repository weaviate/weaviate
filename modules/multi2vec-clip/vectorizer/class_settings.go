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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/moduletools"
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) ImageField(property string) bool {
	return ic.field("imageFields", property)
}

func (ic *classSettings) ImageFieldsWeights() ([]float32, error) {
	return ic.getFieldsWeights("image")
}

func (ic *classSettings) TextField(property string) bool {
	return ic.field("textFields", property)
}

func (ic *classSettings) TextFieldsWeights() ([]float32, error) {
	return ic.getFieldsWeights("text")
}

func (ic *classSettings) field(name, property string) bool {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return false
	}

	fields, ok := ic.cfg.Class()[name]
	if !ok {
		return false
	}

	fieldsArray, ok := fields.([]interface{})
	if !ok {
		return false
	}

	fieldNames := make([]string, len(fieldsArray))
	for i, value := range fieldsArray {
		fieldNames[i] = value.(string)
	}

	for i := range fieldNames {
		if fieldNames[i] == property {
			return true
		}
	}

	return false
}

func (ic *classSettings) Validate() error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	imageFields, imageFieldsOk := ic.cfg.Class()["imageFields"]
	textFields, textFieldsOk := ic.cfg.Class()["textFields"]
	if !imageFieldsOk && !textFieldsOk {
		return errors.New("textFields or imageFields setting needs to be present")
	}

	if imageFieldsOk {
		imageFieldsCount, err := ic.validateFields("image", imageFields)
		if err != nil {
			return err
		}
		err = ic.validateWeights("image", imageFieldsCount)
		if err != nil {
			return err
		}
	}

	if textFieldsOk {
		textFieldsCount, err := ic.validateFields("text", textFields)
		if err != nil {
			return err
		}
		err = ic.validateWeights("text", textFieldsCount)
		if err != nil {
			return err
		}
	}

	return nil
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

func (ic *classSettings) validateWeights(name string, count int) error {
	weights, ok := ic.getWeights(name)
	if ok {
		if len(weights) != count {
			return errors.Errorf("weights.%sFields does not equal number of %sFields", name, name)
		}
		_, err := ic.getWeightsArray(weights)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ic *classSettings) getWeights(name string) ([]interface{}, bool) {
	weights, ok := ic.cfg.Class()["weights"]
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

func (ic *classSettings) getWeightsArray(weights []interface{}) ([]float32, error) {
	weightsArray := make([]float32, len(weights))
	for i := range weights {
		weight, err := ic.getNumber(weights[i])
		if err != nil {
			return nil, err
		}
		weightsArray[i] = weight
	}
	return weightsArray, nil
}

func (ic *classSettings) getFieldsWeights(name string) ([]float32, error) {
	weights, ok := ic.getWeights(name)
	if ok {
		return ic.getWeightsArray(weights)
	}
	return nil, nil
}

func (ic *classSettings) getNumber(in interface{}) (float32, error) {
	switch i := in.(type) {
	case float64:
		return float32(i), nil
	case float32:
		return i, nil
	case int:
		return float32(i), nil
	case string:
		num, err := strconv.ParseFloat(i, 64)
		if err != nil {
			return 0, err
		}
		return float32(num), err
	case json.Number:
		num, err := i.Float64()
		if err != nil {
			return 0, err
		}
		return float32(num), err
	default:
		return 0.0, errors.Errorf("Unrecognized weight entry type: %T", i)
	}
}
