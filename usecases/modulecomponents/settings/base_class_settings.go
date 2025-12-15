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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

var errInvalidProperties = fmt.Errorf("invalid properties: didn't find a single property which is " +
	"vectorizable and is not excluded from indexing. " +
	"To fix this add a vectorizable property which is not excluded from indexing")

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

func (s BaseClassSettings) hasSourceProperties() bool {
	return s.cfg != nil && len(s.Properties()) > 0
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
	if s.cfg == nil || len(s.GetSettings()) == 0 {
		return nil
	}

	field, ok := s.GetSettings()["properties"]
	if !ok {
		return nil
	}

	asArray, ok := field.([]any)
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
	if s.cfg == nil || len(s.GetSettings()) == 0 {
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
	if s.cfg != nil && len(s.GetSettings()) > 0 {
		if field, ok := s.GetSettings()["properties"]; ok {
			fieldsArray, fieldsArrayOk := field.([]any)
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

func (s BaseClassSettings) GetNumber(in any) (float32, error) {
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

		if !s.isPropertyDataTypeSupported(prop.DataType[0]) {
			// we can only vectorize text-like props
			continue
		}

		if s.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return errInvalidProperties
}

func (s BaseClassSettings) GetSettings() map[string]any {
	if s.cfg == nil || s.propertyHelper == nil {
		return nil
	}
	return s.propertyHelper.GetSettings(s.cfg)
}

func (s BaseClassSettings) Validate(class *models.Class) error {
	if s.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	// validate properties setting if it's a right format only if it's defined
	if err := s.ValidateClassSettings(); err != nil {
		return err
	}

	if !s.isAutoSchemaEnabled() {
		// validate class's properties against properties
		err := s.ValidateIndexState(class)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s BaseClassSettings) isAutoSchemaEnabled() bool {
	if s.cfg != nil && s.cfg.Config() != nil && s.cfg.Config().AutoSchema.Enabled != nil {
		return s.cfg.Config().AutoSchema.Enabled.Get()
	}
	return false
}

func (s BaseClassSettings) isPropertyDataTypeSupported(dt string) bool {
	switch schema.DataType(dt) {
	case schema.DataTypeText, schema.DataTypeString, schema.DataTypeTextArray, schema.DataTypeStringArray:
		return true
	default:
		// do nothing
	}
	if s.hasSourceProperties() {
		// include additional property types
		switch schema.DataType(dt) {
		case schema.DataTypeObject, schema.DataTypeObjectArray:
			return true
		case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeIntArray, schema.DataTypeNumberArray:
			return true
		case schema.DataTypeDate, schema.DataTypeDateArray:
			return true
		case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
			return true
		default:
			// do nothing
		}
	}
	return false
}

func ValidateSetting[T string | int64](value T, availableValues []T) bool {
	return slices.Contains(availableValues, value)
}
