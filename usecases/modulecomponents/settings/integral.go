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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/moduletools"
)

// integerSettingError returns an error when an int setting of a new class
// was given a number with a fraction, or one the stored float64 cannot hold
// exactly.
//
// Only a json.Number is checked: that is how the REST API decodes the
// numbers of a new class. A class read back from the schema or from a backup
// holds float64, so an existing class with a fraction keeps working.
func integerSettingError(settings map[string]any, name string) error {
	number, ok := settings[name].(json.Number)
	if !ok {
		return nil
	}
	_, err := number.Float64()
	if errors.Is(err, strconv.ErrRange) {
		return fmt.Errorf("%s is out of range, got %s", name, number)
	}
	if err != nil {
		// Invalid syntax, which the REST decoder never produces: the getter
		// returns the wrong value and the module decides.
		return nil
	}
	if hasFraction(number) {
		return fmt.Errorf("%s must be an integer, got %s", name, number)
	}
	if _, integral := integralValue(number); !integral {
		return fmt.Errorf("%s is out of range, got %s", name, number)
	}
	return nil
}

// reportNonIntegral tells a class config under validation that the int
// setting a getter was asked for has a fraction or is out of range. This
// covers every int setting a module reads while it validates a class.
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
// new class has a fraction or is out of range. A module calls it from its
// class validation for the int settings that validation does not read.
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

// maxExactInt is the float64 below which every integer is exact. A class
// stores its settings as float64, so an int at or beyond it is out of range
// however it is written.
const maxExactInt = 1 << 53

// integralValue returns the integer a number stands for, written as an
// integer ("1024") or as a float without a fraction ("1024.0", "1e3"), when
// a float64 holds it exactly.
func integralValue(number json.Number) (int64, bool) {
	if asInt, err := number.Int64(); err == nil {
		if asInt >= maxExactInt || asInt <= -maxExactInt {
			return 0, false
		}
		return asInt, true
	}
	asFloat, err := number.Float64()
	if err != nil || hasFraction(number) || math.Abs(asFloat) >= maxExactInt {
		return 0, false
	}
	return int64(asFloat), true
}

// hasFraction reports whether a number that parses as a float64 has a
// nonzero fraction. It reads the digits, because the float64 rounds a
// fraction below its precision away: 1024.0000000000000000001 to 1024, 1e-400
// to 0. NaN, Inf and hex count as a fraction.
func hasFraction(number json.Number) bool {
	mantissa, exponent, _ := strings.Cut(strings.ToLower(string(number)), "e")
	whole, fraction, _ := strings.Cut(strings.TrimLeft(mantissa, "+-"), ".")
	if strings.Trim(whole, "0123456789") != "" || strings.Trim(fraction, "0123456789") != "" {
		return true
	}
	exp := 0
	if exponent != "" {
		var err error
		// Atoi clamps an exponent beyond int to its bound, which keeps the sign.
		if exp, err = strconv.Atoi(exponent); err != nil && !errors.Is(err, strconv.ErrRange) {
			return true
		}
	}
	// The last nonzero digit decides.
	if digits := strings.TrimRight(fraction, "0"); digits != "" {
		return exp < len(digits)
	}
	if digits := strings.TrimRight(whole, "0"); digits != "" {
		return exp < -(len(whole) - len(digits))
	}
	return false
}
