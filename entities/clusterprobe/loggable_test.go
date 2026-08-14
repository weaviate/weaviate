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

package clusterprobe_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

func TestLoggable(t *testing.T) {
	const euro = "€" // three bytes, so a cut inside it splits a rune

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: `""`},
		{name: "plain", in: "node-1", want: `"node-1"`},
		{name: "newline is escaped", in: "busy\nfake log line", want: `"busy\nfake log line"`},
		{name: "carriage return is escaped", in: "a\rb", want: `"a\rb"`},
		{name: "quote and backslash are escaped", in: `a"b\c`, want: `"a\"b\\c"`},
		{
			name: "exactly at the cap is kept whole",
			in:   strings.Repeat("a", 128),
			want: `"` + strings.Repeat("a", 128) + `"`,
		},
		{
			name: "one byte over the cap is truncated",
			in:   strings.Repeat("a", 129),
			want: `"` + strings.Repeat("a", 128) + `…(truncated)"`,
		},
		{
			name: "cut inside a rune backs up past it",
			in:   strings.Repeat("a", 127) + euro,
			want: `"` + strings.Repeat("a", 127) + `…(truncated)"`,
		},
		{
			name: "cut on a rune's last byte backs up past the whole rune",
			in:   strings.Repeat("a", 126) + euro + "z",
			want: `"` + strings.Repeat("a", 126) + `…(truncated)"`,
		},
		{
			name: "a rune ending exactly at the cap survives",
			in:   strings.Repeat("a", 125) + euro + strings.Repeat("z", 20),
			want: `"` + strings.Repeat("a", 125) + euro + `…(truncated)"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clusterprobe.Loggable(tt.in)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "\n", "a raw newline would forge a log line")
			assert.True(t, utf8.ValidString(got), "truncation must not leave half a rune")
			assert.NotContains(t, got, `\x`, "a half rune would show up as a byte escape")
		})
	}
}
