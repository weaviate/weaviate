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

package settings

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

type BaseClassSettings struct {
	cfg                 moduletools.ClassConfig
	propertyHelper      *classPropertyValuesHelper
	lowerCaseInput      bool
	modelParameterNames []string
}

func NewBaseClassSettings(cfg moduletools.ClassConfig, lowerCaseInput bool) *BaseClassSettings {
	return &BaseClassSettings{cfg: cfg, propertyHelper: &classPropertyValuesHelper{}, lowerCaseInput: lowerCaseInput}
}

func NewBaseClassSettingsWithAltNames(cfg moduletools.ClassConfig,
	lowerCaseInput bool, moduleName string, altNames []string, customModelParameterName []string,
) *BaseClassSettings {
	modelParameters := append(customModelParameterName, "model")

	return &BaseClassSettings{
		cfg:                 cfg,
		propertyHelper:      &classPropertyValuesHelper{moduleName: moduleName, altNames: altNames},
		lowerCaseInput:      lowerCaseInput,
		modelParameterNames: modelParameters,
	}
}

func NewBaseClassSettingsWithCustomModel(cfg moduletools.ClassConfig, lowerCaseInput bool, customModelParameterName string) *BaseClassSettings {
	return &BaseClassSettings{
		cfg:                 cfg,
		propertyHelper:      &classPropertyValuesHelper{},
		lowerCaseInput:      lowerCaseInput,
		modelParameterNames: []string{"model", customModelParameterName},
	}
}

func (s BaseClassSettings) LowerCaseInput() bool {
	return s.lowerCaseInput
}

func (s BaseClassSettings) PropertyIndexed(propName string) bool {
	if s.cfg == nil {
		return DefaultPropertyIndexed
	}

	if len(s.Properties()) > 0 {
		return s.isPropertyIndexed(propName)
	}

	vcn, ok := s.cfg.Property(propName)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (s BaseClassSettings) VectorizePropertyName(propName string) bool {
	if s.cfg == nil {
		return DefaultVectorizePropertyName
	}

	vcn, ok := s.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (s BaseClassSettings) VectorizeClassName() bool {
	if s.cfg == nil {
		return DefaultVectorizeClassName
	}

	vcn, ok := s.GetSettings()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (s BaseClassSettings) Properties() []string {
	if s.cfg == nil || len(s.cfg.Class()) == 0 {
		return nil
	}

	field, ok := s.GetSettings()["properties"]
	if !ok {
		return nil
	}

	asArray, ok := field.([]interface{})
	if ok {
		asStringArray := make([]string, len(asArray))
		for i := range asArray {
			asStringArray[i] = asArray[i].(string)
		}
		return asStringArray
	}

	asStringArray, ok := field.([]string)
	if ok {
		return asStringArray
	}

	return nil
}

func (s BaseClassSettings) Model() string {
	if s.cfg == nil || len(s.cfg.Class()) == 0 {
		return ""
	}

	for _, parameterName := range s.modelParameterNames {
		if model, ok := s.GetSettings()[parameterName]; ok {
			return model.(string)
		}
	}

	return ""
}

func (s BaseClassSettings) ValidateClassSettings() error {
	if s.cfg != nil && len(s.cfg.Class()) > 0 {
		if field, ok := s.GetSettings()["properties"]; ok {
			fieldsArray, fieldsArrayOk := field.([]interface{})
			if fieldsArrayOk {
				if len(fieldsArray) == 0 {
					return errors.New("properties field needs to have at least 1 property defined")
				}
				for _, value := range fieldsArray {
					_, ok := value.(string)
					if !ok {
						return fmt.Errorf("properties field value: %v must be a string", value)
					}
				}
			}
			stringArray, stringArrayOk := field.([]string)
			if stringArrayOk && len(stringArray) == 0 {
				return errors.New("properties field needs to have at least 1 property defined")
			}
			if !fieldsArrayOk && !stringArrayOk {
				return fmt.Errorf("properties field needs to be of array type, got: %T", field)
			}
		}
	}
	return nil
}

func (s BaseClassSettings) isPropertyIndexed(propName string) bool {
	for _, name := range s.Properties() {
		if propName == name {
			return true
		}
	}
	return false
}

func (s BaseClassSettings) GetPropertyAsInt64(name string, defaultValue *int64) *int64 {
	return s.propertyHelper.GetPropertyAsInt64(s.cfg, name, defaultValue)
}

func (s BaseClassSettings) GetPropertyAsString(name, defaultValue string) string {
	return s.propertyHelper.GetPropertyAsString(s.cfg, name, defaultValue)
}

func (s BaseClassSettings) GetPropertyAsBool(name string, defaultValue bool) bool {
	return s.propertyHelper.GetPropertyAsBool(s.cfg, name, defaultValue)
}

func (s BaseClassSettings) GetNumber(in interface{}) (float32, error) {
	return s.propertyHelper.GetNumber(in)
}

func (s BaseClassSettings) ValidateIndexState(class *models.Class) error {
	if s.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return fmt.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if s.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing")
}

func (s BaseClassSettings) GetSettings() map[string]interface{} {
	return s.propertyHelper.GetSettings(s.cfg)
}

func (s BaseClassSettings) Validate(class *models.Class) error {
	if s.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	if err := s.ValidateClassSettings(); err != nil {
		return err
	}

	err := s.ValidateIndexState(class)
	if err != nil {
		return err
	}

	return nil
}

func ValidateSetting[T string | int64](value T, availableValues []T) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}
