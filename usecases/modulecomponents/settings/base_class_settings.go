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

import "github.com/weaviate/weaviate/entities/moduletools"

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

type BaseClassSettings struct {
	cfg moduletools.ClassConfig
}

func NewBaseClassSettings(cfg moduletools.ClassConfig) *BaseClassSettings {
	return &BaseClassSettings{cfg: cfg}
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

func (s *BaseClassSettings) isPropertyIndexed(propName string) bool {
	for _, name := range s.Properties() {
		if propName == name {
			return true
		}
	}
	return false
}
