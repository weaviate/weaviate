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

package config

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// wellFormedLicenseKey returns a key with the correct form for tests. It is
// not a real license.
func wellFormedLicenseKey() string {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	return "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV." + base64.RawURLEncoding.EncodeToString(seed)
}

func TestLicenseKeyWellFormed(t *testing.T) {
	valid := wellFormedLicenseKey()

	factors := []struct {
		name     string
		key      string
		expected bool
	}{
		{"well-formed key", valid, true},
		{"empty", "", false},
		{"no separators", "wv8lic_01ARZ3NDEKTSV4RRFFQ69G5FAVAAAA", false},
		{"too many segments", valid + ".extra", false},
		{"missing seed segment", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV", false},
		{"wrong format prefix", "wv7.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"uppercase format prefix", "WV8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"wrong id prefix", "wv8.foo_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id too short", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FA.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id too long", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAVV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id with excluded char I", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAI.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id with excluded char L", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAL.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id with excluded char O", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAO.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id with excluded char U", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAU.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"id with lowercase char", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAa.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"seed with padding", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", false},
		{"seed too short", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"seed too long", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false},
		{"seed with standard base64 char", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+A", false},
		{"seed with non-zero trailing bits", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAV.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB", false},
		{"seed with embedded newline", strings.Replace(valid, "AA", "A\nA", 1), false},
		{"seed with embedded carriage return", strings.Replace(valid, "AA", "A\rA", 1), false},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, licenseKeyWellFormed(tt.key))
		})
	}
}
