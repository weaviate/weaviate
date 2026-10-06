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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The REST API decodes the numbers of a module config as json.Number. A
// fraction such as a temperature of 0.5 must not be read as a missing or
// invalid value.
func TestGetNumberValueFromJSONNumber(t *testing.T) {
	const wrong, missing = -1.0, 99.0

	t.Run("float setting", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
			want  float64
		}{
			{name: "fraction", value: json.Number("0.5"), want: 0.5},
			{name: "fraction written with a zero", value: json.Number("0.0"), want: 0},
			{name: "whole number", value: json.Number("1"), want: 1},
			{name: "whole number written as a fraction", value: json.Number("2.0"), want: 2},
			{name: "negative fraction", value: json.Number("-0.25"), want: -0.25},
			{name: "exponent", value: json.Number("1e-2"), want: 0.01},
			{name: "not a number", value: json.Number("abc"), want: wrong},
			{name: "float64", value: 0.5, want: 0.5},
			{name: "int", value: 3, want: 3},
			{name: "string that is not a number", value: "high", want: wrong},
			{name: "missing", value: nil, want: missing},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				wrongValue, missingValue := wrong, missing
				settings := map[string]any{}
				if tt.value != nil {
					settings["temperature"] = tt.value
				}

				got := getNumberValue(settings, "temperature", &wrongValue, &missingValue)

				require.NotNil(t, got)
				assert.Equal(t, tt.want, *got)
			})
		}
	})

	t.Run("int setting", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
			want  int
		}{
			{name: "whole number", value: json.Number("25"), want: 25},
			// An int setting cannot hold a fraction, so it is invalid.
			{name: "fraction", value: json.Number("2.5"), want: -1},
			{name: "not a number", value: json.Number("abc"), want: -1},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				wrongValue, missingValue := -1, 99

				got := getNumberValue(map[string]any{"maxTokens": tt.value}, "maxTokens", &wrongValue, &missingValue)

				require.NotNil(t, got)
				assert.Equal(t, tt.want, *got)
			})
		}
	})
}
