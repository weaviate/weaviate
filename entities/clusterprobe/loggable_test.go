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

package clusterprobe

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Loggable() must quote (log injection) and not cut a multi-byte rune in half;
// the ascii prefixes cover every byte offset the cap can land on.
func TestLoggableIsQuotedAndCutsOnRuneBoundaries(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "short value is kept whole", in: "not found"},
		{name: "newline forging a second log line", in: "not found\nlevel=error msg=forged"},
		{name: "carriage return", in: "not found\rmsg=forged"},
		{name: "ascii is cut at the cap", in: strings.Repeat("a", 500)},
		{name: "two-byte runes, cap on a boundary", in: strings.Repeat("é", 200)},
		{name: "two-byte runes, cap one byte in", in: "a" + strings.Repeat("é", 200)},
		{name: "three-byte runes, cap on a boundary", in: strings.Repeat("日", 200)},
		{name: "three-byte runes, cap one byte in", in: "a" + strings.Repeat("日", 200)},
		{name: "three-byte runes, cap two bytes in", in: "aa" + strings.Repeat("日", 200)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Loggable(tt.in)

			assert.NotContains(t, got, "\n", "a raw newline ends the line and forges the next one")
			assert.NotContains(t, got, "\r")

			unquoted, err := strconv.Unquote(got)
			require.NoError(t, err, "the result has to be a quoted Go string")
			kept := strings.TrimSuffix(unquoted, loggableTruncationMarker)
			assert.True(t, strings.HasPrefix(tt.in, kept), "what is kept has to be a prefix of the input")
			assert.True(t, utf8.ValidString(kept), "a cut must not split a rune")
			assert.LessOrEqual(t, len(kept), loggableLimit)
		})
	}
}
