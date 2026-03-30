//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package parser

import (
	"fmt"
	"os"
	"strconv"
	"time"

	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func parseDynamicValue[T configRuntime.DynamicType](envName string, defaultValue T,
	parse func(string) (T, error), onSuccess func(*configRuntime.DynamicValue[T]),
) error {
	val := defaultValue
	if env := os.Getenv(envName); env != "" {
		var err error
		val, err = parse(env)
		if err != nil {
			return fmt.Errorf("%s: %w", envName, err)
		}
	}

	onSuccess(configRuntime.NewDynamicValue(val))
	return nil
}

func parseDynamicValueWithValidation[T configRuntime.DynamicType](envName string, defaultValue T,
	parse func(string) (T, error), validate func(T) error, onSuccess func(*configRuntime.DynamicValue[T]),
) error {
	val := defaultValue
	if v := os.Getenv(envName); v != "" {
		var err error
		val, err = parse(v)
		if err != nil {
			return fmt.Errorf("%s: %w", envName, err)
		}
	}

	dynVal, err := configRuntime.NewDynamicValueWithValidation(val, func(val T) error {
		if err := validate(val); err != nil {
			return fmt.Errorf("%s: %w", envName, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	onSuccess(dynVal)
	return nil
}

// ----------------------------------------------------------------------------

func parseFloat(v string) (float64, error) {
	return strconv.ParseFloat(v, 64)
}

func ParseDynamicFloat(envName string, defaultValue float64,
	onSuccess func(val *configRuntime.DynamicValue[float64]),
) error {
	return parseDynamicValue(envName, defaultValue, parseFloat, onSuccess)
}

func ParseDynamicFloatWithValidation(envName string, defaultValue float64,
	validate func(val float64) error,
	onSuccess func(val *configRuntime.DynamicValue[float64]),
) error {
	return parseDynamicValueWithValidation(envName, defaultValue, parseFloat, validate, onSuccess)
}

// ----------------------------------------------------------------------------

func ParseDynamicInt(envName string, defaultValue int,
	onSuccess func(val *configRuntime.DynamicValue[int]),
) error {
	return parseDynamicValue(envName, defaultValue, strconv.Atoi, onSuccess)
}

func ParseDynamicIntWithValidation(envName string, defaultValue int,
	validate func(val int) error,
	onSuccess func(val *configRuntime.DynamicValue[int]),
) error {
	return parseDynamicValueWithValidation(envName, defaultValue, strconv.Atoi, validate, onSuccess)
}

// ----------------------------------------------------------------------------

func parseString(v string) (string, error) {
	return v, nil
}

func ParseDynamicString(envName string, defaultValue string,
	onSuccess func(val *configRuntime.DynamicValue[string]),
) error {
	return parseDynamicValue(envName, defaultValue, parseString, onSuccess)
}

func ParseDynamicStringWithValidation(envName string, defaultValue string,
	validate func(val string) error,
	onSuccess func(val *configRuntime.DynamicValue[string]),
) error {
	return parseDynamicValueWithValidation(envName, defaultValue, parseString, validate, onSuccess)
}

// ----------------------------------------------------------------------------

func ParseDynamicDuration(envName string, defaultValue time.Duration,
	onSuccess func(val *configRuntime.DynamicValue[time.Duration]),
) error {
	return parseDynamicValue(envName, defaultValue, time.ParseDuration, onSuccess)
}

func ParseDynamicDurationWithValidation(envName string, defaultValue time.Duration,
	validate func(val time.Duration) error,
	onSuccess func(val *configRuntime.DynamicValue[time.Duration]),
) error {
	return parseDynamicValueWithValidation(envName, defaultValue, time.ParseDuration, validate, onSuccess)
}
