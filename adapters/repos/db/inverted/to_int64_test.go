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

package inverted

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToInt64(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		exp    int64
		expErr bool
	}{
		{name: "int64", in: int64(42), exp: 42},
		{name: "int", in: 42, exp: 42},
		{name: "float64", in: float64(42), exp: 42},
		{name: "int64 max as int64", in: int64(math.MaxInt64), exp: math.MaxInt64},

		// A json.Number carries the digits the client sent, so nothing is lost
		// before this point. Reading it through Float64 was what lost them.
		{name: "json.Number small", in: json.Number("42"), exp: 42},
		{name: "json.Number negative", in: json.Number("-42"), exp: -42},
		{name: "json.Number int64 max", in: json.Number("9223372036854775807"), exp: math.MaxInt64},
		{name: "json.Number int64 min", in: json.Number("-9223372036854775808"), exp: math.MinInt64},
		{name: "json.Number just past 2^53", in: json.Number("9007199254740993"), exp: 9007199254740993},

		// Not integral: no Int64 reading, so the Float64 path still applies.
		{name: "json.Number with a fraction", in: json.Number("42.7"), exp: 42},

		{name: "json.Number that is not a number", in: json.Number("nope"), expErr: true},
		{name: "unsupported type", in: "42", expErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := toInt64(test.in)
			if test.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.exp, got)
		})
	}
}
