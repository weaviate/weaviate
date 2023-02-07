//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return &asString
		}
		var empty string
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
	if ok {
		asFloat, ok := val.(float64)
		if ok {
			return &asFloat
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			return &asFloat
		}
		asInt, ok := val.(int)
		if ok {
			asFloat := float64(asInt)
			return &asFloat
		}
		var wrongVal float64 = -1.0
		return &wrongVal
	}

	if defaultValue != nil {
		return defaultValue
	}
	return nil
}
