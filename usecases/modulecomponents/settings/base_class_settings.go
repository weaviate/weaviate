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

	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

type BaseClassSettings struct {
	cfg            moduletools.ClassConfig
	propertyHelper *classPropertyValuesHelper
}

func NewBaseClassSettings(cfg moduletools.ClassConfig) *BaseClassSettings {
	return &BaseClassSettings{cfg: cfg, propertyHelper: &classPropertyValuesHelper{}}
}

func (s *BaseClassSettings) PropertyIndexed(propName string) bool {
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

func (s *BaseClassSettings) VectorizePropertyName(propName string) bool {
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

func (s *BaseClassSettings) VectorizeClassName() bool {
	if s.cfg == nil {
		return DefaultVectorizeClassName
	}

	vcn, ok := s.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (s *BaseClassSettings) Properties() []string {
	if s.cfg == nil || len(s.cfg.Class()) == 0 {
		return nil
	}

	field, ok := s.cfg.Class()["properties"]
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

func (s *BaseClassSettings) Validate() error {
	if s.cfg != nil && len(s.cfg.Class()) > 0 {
		if field, ok := s.cfg.Class()["properties"]; ok {
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

func (s *BaseClassSettings) isPropertyIndexed(propName string) bool {
	for _, name := range s.Properties() {
		if propName == name {
			return true
		}
	}
	return false
}

func (s *BaseClassSettings) GetPropertyAsInt64(name string, defaultValue *int64) *int64 {
	return s.propertyHelper.GetPropertyAsInt64(s.cfg, name, defaultValue)
}

func (s *BaseClassSettings) GetPropertyAsString(name, defaultValue string) string {
	return s.propertyHelper.GetPropertyAsString(s.cfg, name, defaultValue)
}

func (s *BaseClassSettings) GetPropertyAsBool(name string, defaultValue bool) bool {
	return s.propertyHelper.GetPropertyAsBool(s.cfg, name, defaultValue)
}

func (s *BaseClassSettings) GetNumber(in interface{}) (float32, error) {
	return s.propertyHelper.GetNumber(in)
}
