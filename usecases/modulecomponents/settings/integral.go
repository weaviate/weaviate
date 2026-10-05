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

package settings

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/weaviate/weaviate/entities/moduletools"
)

// integerSettingError returns an error when an int setting of a new class
// was given a number with a fraction.
//
// Only a json.Number is checked: that is how the REST API decodes the
// numbers of a new class. A class read back from the schema or from a backup
// holds float64, so an existing class with a fraction keeps working.
func integerSettingError(settings map[string]any, name string) error {
	number, ok := settings[name].(json.Number)
	if !ok {
		return nil
	}
	if _, err := number.Float64(); err != nil {
		// Not a number: the getter returns the wrong value and the module
		// decides, as before.
		return nil
	}
	if _, integral := integralValue(number); !integral {
		return fmt.Errorf("%s must be an integer, got %s", name, number)
	}
	return nil
}

// reportNonIntegral tells a class config under validation that the int
// setting a getter was asked for has a fraction. This covers every int
// setting a module reads while it validates a class.
func reportNonIntegral(cfg moduletools.ClassConfig, settings map[string]any, name string) {
	validation, ok := cfg.(*moduletools.ValidationClassConfig)
	if !ok {
		return
	}
	if err := integerSettingError(settings, name); err != nil {
		validation.ReportInvalidSetting(err)
	}
}

// ValidateIntegers returns an error when one of the named int settings of a
// new class has a fraction. A module calls it from its class validation for
// the int settings that validation does not read otherwise.
func (h *classPropertyValuesHelper) ValidateIntegers(cfg moduletools.ClassConfig, names ...string) error {
	if cfg == nil {
		return nil
	}
	settings := h.GetSettings(cfg)
	for _, name := range names {
		if err := integerSettingError(settings, name); err != nil {
			return err
		}
	}
	return nil
}

// maxExactInt is the float64 below which every integer is exact.
const maxExactInt = 1 << 53

// integralValue returns the integer a number stands for, written as an
// integer ("1024") or as a float without a fraction ("1024.0", "1e3").
func integralValue(number json.Number) (int64, bool) {
	if asInt, err := number.Int64(); err == nil {
		return asInt, true
	}
	asFloat, err := number.Float64()
	if err != nil || math.IsInf(asFloat, 0) || math.IsNaN(asFloat) {
		return 0, false
	}
	if asFloat != math.Trunc(asFloat) || math.Abs(asFloat) >= maxExactInt {
		return 0, false
	}
	return int64(asFloat), true
}
