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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
)

func TestIntegralValue(t *testing.T) {
	tests := []struct {
		number string
		want   int64
		ok     bool
	}{
		{number: "1024", want: 1024, ok: true},
		{number: "-3", want: -3, ok: true},
		{number: "0", want: 0, ok: true},
		{number: "1024.0", want: 1024, ok: true},
		{number: "1e3", want: 1000, ok: true},
		{number: "-2.0", want: -2, ok: true},
		{number: "9223372036854775807", want: 9223372036854775807, ok: true},
		{number: "100.5"},
		{number: "0.1"},
		{number: "1e-3"},
		// Too large for a float64 to hold the integer exactly.
		{number: "9007199254740993.0"},
		{number: "1e400"},
		{number: "abc"},
		{number: ""},
	}
	for _, tt := range tests {
		t.Run(tt.number, func(t *testing.T) {
			got, ok := integralValue(json.Number(tt.number))
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// An int setting written as a float without a fraction reads as that int,
// which is what the stored float64 gives at request time.
func TestGetNumberValueReadsAnIntWrittenAsAFloat(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int
	}{
		{name: "integer", value: json.Number("1024"), want: 1024},
		{name: "float without a fraction", value: json.Number("1024.0"), want: 1024},
		{name: "exponent", value: json.Number("1e3"), want: 1000},
		{name: "fraction is the wrong value", value: json.Number("100.5"), want: -1},
		{name: "stored fraction is truncated", value: 100.5, want: 100},
		{name: "stored integer", value: float64(1024), want: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrong, missing := -1, 99
			got := getNumberValue(map[string]any{"dimensions": tt.value}, "dimensions", &wrong, &missing)
			require.NotNil(t, got)
			assert.Equal(t, tt.want, *got)
		})
	}
}

func TestIntGettersReportAFractionToAClassUnderValidation(t *testing.T) {
	helper := NewPropertyValuesHelper("")
	// The getter is asked the way many modules ask: the default is also the
	// wrong value, so the fraction would not be seen otherwise.
	getters := map[string]func(cfg moduletools.ClassConfig, name string) any{
		"GetPropertyAsInt": func(cfg moduletools.ClassConfig, name string) any {
			fallback := 3072
			return *helper.GetPropertyAsInt(cfg, name, &fallback)
		},
		"GetPropertyAsInt64": func(cfg moduletools.ClassConfig, name string) any {
			fallback := int64(3072)
			return *helper.GetPropertyAsInt64(cfg, name, &fallback)
		},
	}
	tests := []struct {
		name    string
		value   any
		wantErr string
	}{
		{name: "fraction", value: json.Number("100.5"), wantErr: "dimensions must be an integer, got 100.5"},
		{name: "small fraction", value: json.Number("1e-3"), wantErr: "dimensions must be an integer, got 1e-3"},
		{name: "integer", value: json.Number("1024")},
		{name: "float without a fraction", value: json.Number("1024.0")},
		{name: "missing", value: nil},
		// Values of a class that was stored before: not a json.Number.
		{name: "stored fraction", value: 100.5},
		{name: "stored integer", value: float64(1024)},
		// The module's own validation decides about these.
		{name: "string", value: "many"},
		{name: "bool", value: true},
	}
	for getterName, get := range getters {
		for _, tt := range tests {
			t.Run(getterName+"/"+tt.name, func(t *testing.T) {
				settings := map[string]any{}
				if tt.value != nil {
					settings["dimensions"] = tt.value
				}
				validation := moduletools.NewValidationClassConfig(fakeClassConfig{config: settings})

				get(validation, "dimensions")

				if tt.wantErr == "" {
					assert.NoError(t, validation.Err())
				} else {
					assert.EqualError(t, validation.Err(), tt.wantErr)
				}
				// Outside of validation the getter reports nothing and does not fail.
				get(fakeClassConfig{config: settings}, "dimensions")
			})
		}
	}

	t.Run("a float setting with a fraction is not reported", func(t *testing.T) {
		validation := moduletools.NewValidationClassConfig(
			fakeClassConfig{config: map[string]any{"temperature": json.Number("0.5")}})
		fallback := 1.0
		got := helper.GetPropertyAsFloat64(validation, "temperature", &fallback)
		assert.Equal(t, 0.5, *got)
		assert.NoError(t, validation.Err())
	})

	t.Run("the first report is kept", func(t *testing.T) {
		validation := moduletools.NewValidationClassConfig(fakeClassConfig{config: map[string]any{
			"dimensions": json.Number("100.5"), "maxTokens": json.Number("7.5"),
		}})
		fallback := 1
		helper.GetPropertyAsInt(validation, "maxTokens", &fallback)
		helper.GetPropertyAsInt(validation, "dimensions", &fallback)
		assert.EqualError(t, validation.Err(), "maxTokens must be an integer, got 7.5")
		validation.ReportInvalidSetting(errors.New("later"))
		assert.EqualError(t, validation.Err(), "maxTokens must be an integer, got 7.5")
	})
}

func TestValidateIntegers(t *testing.T) {
	helper := NewPropertyValuesHelper("")
	settings := map[string]any{
		"maxTokens":  json.Number("100"),
		"topK":       json.Number("2.5"),
		"dimensions": 100.5,
		"model":      "m",
	}
	cfg := fakeClassConfig{config: settings}

	assert.NoError(t, helper.ValidateIntegers(cfg))
	assert.NoError(t, helper.ValidateIntegers(cfg, "maxTokens", "missing", "model", "dimensions"))
	assert.EqualError(t, helper.ValidateIntegers(cfg, "maxTokens", "topK"), "topK must be an integer, got 2.5")
	assert.NoError(t, helper.ValidateIntegers(nil, "topK"))
	assert.EqualError(t, NewBaseClassSettings(cfg, false).ValidateIntegers("topK"), "topK must be an integer, got 2.5")
}

// An integer that cannot be read as an int is out of range, not a fraction.
func TestIntegerSettingErrorOutOfRange(t *testing.T) {
	tests := []struct {
		number  string
		wantErr string
	}{
		// Written as an integer, it is read exactly beyond 2^53.
		{number: "9223372036854775807"},
		{number: "1e16", wantErr: "dimensions is out of range, got 1e16"},
		{number: "99999999999999999999", wantErr: "dimensions is out of range, got 99999999999999999999"},
		{number: "1e400", wantErr: "dimensions is out of range, got 1e400"},
	}
	for _, tt := range tests {
		t.Run(tt.number, func(t *testing.T) {
			err := integerSettingError(map[string]any{"dimensions": json.Number(tt.number)}, "dimensions")
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
