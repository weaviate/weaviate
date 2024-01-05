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

package config

import "github.com/weaviate/weaviate/entities/moduletools"

const (
	MethodMean    = "mean"
	MethodDefault = MethodMean
)

const (
	calculationMethodField   = "method"
	referencePropertiesField = "referenceProperties"
)

func Default() map[string]interface{} {
	return map[string]interface{}{
		calculationMethodField: MethodDefault,
	}
}

type Config struct {
	class moduletools.ClassConfig
}

func New(cfg moduletools.ClassConfig) *Config {
	return &Config{class: cfg}
}

func (c *Config) ReferenceProperties() map[string]struct{} {
	refProps := map[string]struct{}{}
	props := c.class.Class()

	iRefProps := props[referencePropertiesField].([]interface{})
	for _, iProp := range iRefProps {
		refProps[iProp.(string)] = struct{}{}
	}

	return refProps
}

func (c *Config) CalculationMethod() string {
	props := c.class.Class()
	calcMethod := props[calculationMethodField].(string)
	return calcMethod
}
