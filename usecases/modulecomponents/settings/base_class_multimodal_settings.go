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

package settings

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	ImageFieldsProperty   = "imageFields"
	TextFieldsProperty    = "textFields"
	AudioFieldsProperty   = "audioFields"
	VideoFieldsProperty   = "videoFields"
	ImuFieldsProperty     = "imuFields"
	ThermalFieldsProperty = "thermalFields"
	DepthFieldsProperty   = "depthFields"
)

type BaseClassMultiModalSettings struct {
	*BaseClassSettings
}

func NewBaseClassMultiModalSettingsWithAltNames(cfg moduletools.ClassConfig,
	lowerCaseInput bool, moduleName string, altNames []string, customModelParameterName []string,
) *BaseClassMultiModalSettings {
	return &BaseClassMultiModalSettings{
		NewBaseClassSettingsWithAltNames(cfg, lowerCaseInput, moduleName, altNames, customModelParameterName),
	}
}

func (s *BaseClassMultiModalSettings) ImageField(property string) bool {
	return s.field(ImageFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) ImageFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("image")
}

func (s *BaseClassMultiModalSettings) TextField(property string) bool {
	return s.field(TextFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) TextFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("text")
}

func (s *BaseClassMultiModalSettings) AudioField(property string) bool {
	return s.field(AudioFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) AudioFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("audio")
}

func (s *BaseClassMultiModalSettings) VideoField(property string) bool {
	return s.field(VideoFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) VideoFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("video")
}

func (s *BaseClassMultiModalSettings) IMUField(property string) bool {
	return s.field(ImuFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) IMUFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("imu")
}

func (s *BaseClassMultiModalSettings) ThermalField(property string) bool {
	return s.field(ThermalFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) ThermalFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("thermal")
}

func (s *BaseClassMultiModalSettings) DepthField(property string) bool {
	return s.field(DepthFieldsProperty, property)
}

func (s *BaseClassMultiModalSettings) DepthFieldsWeights() ([]float32, error) {
	return s.getFieldsWeights("depth")
}

func (s *BaseClassMultiModalSettings) VectorizableProperties(fields []string) ([]string, error) {
	if s.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return nil, errors.New("empty config")
	}
	props := make([]string, 0)

	for _, field := range fields {
		fields, ok := s.GetSettings()[field]
		if !ok {
			continue
		}

		fieldsArray, ok := fields.([]any)
		if !ok {
			return nil, fmt.Errorf("%s must be an array", field)
		}

		for _, value := range fieldsArray {
			v, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string", field)
			}
			props = append(props, v)
		}
	}
	return props, nil
}

func (s *BaseClassMultiModalSettings) ValidateMultiModal(fields []string) error {
	if s.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	fieldsMap := map[string]struct {
		field any
		ok    bool
	}{}
	for _, fieldName := range fields {
		field, ok := s.GetSettings()[fieldName]
		fieldsMap[fieldName] = struct {
			field any
			ok    bool
		}{field: field, ok: ok}
	}

	allOk := true
	for _, field := range fieldsMap {
		if field.ok {
			allOk = true
			break
		}
	}

	var errorMessages []string

	if !allOk {
		errorMessages = append(errorMessages, fmt.Sprintf("%s setting needs to be present", strings.Join(fields, " or ")))
	}

	for name, f := range fieldsMap {
		if f.ok {
			fieldName, ok := strings.CutSuffix(name, "Fields")
			if !ok {
				return fmt.Errorf("field name has no Fields suffix: %s", name)
			}
			fieldsCount, err := s.validateFields(fieldName, f.field)
			if err != nil {
				errorMessages = append(errorMessages, err.Error())
			}
			err = s.validateWeights(fieldName, fieldsCount)
			if err != nil {
				errorMessages = append(errorMessages, err.Error())
			}
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (s *BaseClassMultiModalSettings) validateFields(name string, fields any) (int, error) {
	fieldsArray, ok := fields.([]any)
	if !ok {
		return 0, fmt.Errorf("%sFields must be an array", name)
	}

	if len(fieldsArray) == 0 {
		return 0, fmt.Errorf("must contain at least one %s field name in %sFields", name, name)
	}

	for _, value := range fieldsArray {
		v, ok := value.(string)
		if !ok {
			return 0, fmt.Errorf("%sField must be a string", name)
		}
		if len(v) == 0 {
			return 0, fmt.Errorf("%sField values cannot be empty", name)
		}
	}

	return len(fieldsArray), nil
}

func (s *BaseClassMultiModalSettings) validateWeights(name string, count int) error {
	weights, ok := s.getWeights(name)
	if ok {
		if len(weights) != count {
			return fmt.Errorf("weights.%sFields does not equal number of %sFields", name, name)
		}
		_, err := s.getWeightsArray(weights)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BaseClassMultiModalSettings) field(name, property string) bool {
	fields, ok := s.GetSettings()[name]
	if !ok {
		return false
	}

	fieldsArray, ok := fields.([]any)
	if !ok {
		return false
	}

	fieldNames := make([]string, len(fieldsArray))
	for i, value := range fieldsArray {
		fieldNames[i] = value.(string)
	}

	return slices.Contains(fieldNames, property)
}

func (s *BaseClassMultiModalSettings) getFieldsWeights(name string) ([]float32, error) {
	weights, ok := s.getWeights(name)
	if ok {
		return s.getWeightsArray(weights)
	}
	return nil, nil
}

func (s *BaseClassMultiModalSettings) getWeights(name string) ([]any, bool) {
	weights, ok := s.GetSettings()["weights"]
	if ok {
		weightsObject, ok := weights.(map[string]any)
		if ok {
			fieldWeights, ok := weightsObject[fmt.Sprintf("%sFields", name)]
			if ok {
				fieldWeightsArray, ok := fieldWeights.([]any)
				if ok {
					return fieldWeightsArray, ok
				}
			}
		}
	}

	return nil, false
}

func (s *BaseClassMultiModalSettings) getWeightsArray(weights []any) ([]float32, error) {
	weightsArray := make([]float32, len(weights))
	for i := range weights {
		weight, err := s.GetNumber(weights[i])
		if err != nil {
			return nil, err
		}
		weightsArray[i] = weight
	}
	return weightsArray, nil
}
