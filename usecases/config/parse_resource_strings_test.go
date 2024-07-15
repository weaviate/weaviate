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

import (
	"math"
	"testing"
)

func TestParseResourceString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
		err      bool
	}{
		{"ValidBytes", "1024", 1024, false},
		{"ValidKiB", "1KiB", 1024, false},
		{"ValidMiB", "500MiB", 500 * 1024 * 1024, false},
		{"ValidTiB", "43TiB", 43 * 1024 * 1024 * 1024 * 1024, false},
		{"ValidKB", "1KB", 1000, false},
		{"ValidMB", "500MB", 500 * 1e6, false},
		{"ValidTB", "43TB", 43 * 1e12, false},
		{"InvalidUnit", "100GiL", 0, true},
		{"InvalidNumber", "tenKiB", 0, true},
		{"InvalidFormat", "1024 KiB", 1024 * 1024, false},
		{"EmptyString", "", 0, true},
		{"NoUnit", "12345", 12345, false},
		{"Unlimited lower case", "unlimited", math.MaxInt64, false},
		{"Unlimited unlimited upper case", "UNLIMITED", math.MaxInt64, false},
		{"Nolimit lower case", "nolimit", math.MaxInt64, false},
		{"Nolimit upper case", "NOLIMIT", math.MaxInt64, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseResourceString(tc.input)
			if (err != nil) != tc.err {
				t.Errorf("parseResourceString(%s) expected error: %v, got: %v", tc.input, tc.err, err != nil)
			}
			if result != tc.expected {
				t.Errorf("parseResourceString(%s) expected %d, got %d", tc.input, tc.expected, result)
			}
		})
	}
}
