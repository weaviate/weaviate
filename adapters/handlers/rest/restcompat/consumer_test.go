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

package restcompat

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRejectLoneSurrogates(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "normal text",
			input:   `{"text":"hello world"}`,
			wantErr: false,
		},
		{
			name:    "no unicode escapes at all (fast path)",
			input:   `{"text":"plain text without backslash-u"}`,
			wantErr: false,
		},
		{
			name:    "valid non-surrogate unicode escape",
			input:   `{"text":"\u0041"}`,
			wantErr: false,
		},
		{
			name:    "valid surrogate pair (emoji U+1F600)",
			input:   `{"text":"\uD83D\uDE00"}`,
			wantErr: false,
		},
		{
			name:    "lone high surrogate",
			input:   `{"text":"\uD800"}`,
			wantErr: true,
		},
		{
			name:    "lone low surrogate",
			input:   `{"text":"\uDC00"}`,
			wantErr: true,
		},
		{
			name:    "high surrogate at end of string",
			input:   `{"text":"abc\uD800"}`,
			wantErr: true,
		},
		{
			name:    "high surrogate followed by non-surrogate escape",
			input:   `{"text":"\uD800\u0041"}`,
			wantErr: true,
		},
		{
			name:    "lone high surrogate max value",
			input:   `{"text":"\uDBFF"}`,
			wantErr: true,
		},
		{
			name:    "lone low surrogate max value",
			input:   `{"text":"\uDFFF"}`,
			wantErr: true,
		},
		{
			name:    "escaped backslash before u - not a unicode escape",
			input:   `{"text":"\\uD800"}`,
			wantErr: false,
		},
		{
			name:    "double escaped backslash then real unicode escape",
			input:   `{"text":"\\\\uD800"}`,
			wantErr: false,
		},
		{
			name:    "odd backslashes then unicode escape (real escape)",
			input:   `{"text":"\\\uD800"}`,
			wantErr: true,
		},
		{
			name:    "surrogate in property key",
			input:   `{"\uD800":"value"}`,
			wantErr: true,
		},
		{
			name:    "mixed valid and invalid surrogates",
			input:   `{"a":"\uD83D\uDE00","b":"\uD800"}`,
			wantErr: true,
		},
		{
			name:    "lowercase hex surrogate",
			input:   `{"text":"\ud800"}`,
			wantErr: true,
		},
		{
			name:    "mixed case hex surrogate",
			input:   `{"text":"\uD8aB"}`,
			wantErr: true,
		},
		{
			name:    "empty object",
			input:   `{}`,
			wantErr: false,
		},
		{
			name:    "empty string value",
			input:   `{"text":""}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectLoneSurrogates([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "lone")
				assert.Contains(t, err.Error(), "surrogate")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewStrictJSONConsumer(t *testing.T) {
	consumer := NewStrictJSONConsumer()

	t.Run("valid JSON unmarshals correctly", func(t *testing.T) {
		input := bytes.NewReader([]byte(`{"name":"hello","value":42}`))
		var target map[string]interface{}
		err := consumer.Consume(input, &target)
		require.NoError(t, err)
		assert.Equal(t, "hello", target["name"])
		assert.Equal(t, float64(42), target["value"])
	})

	t.Run("lone surrogate is rejected", func(t *testing.T) {
		input := bytes.NewReader([]byte(`{"text":"\uD800"}`))
		var target map[string]interface{}
		err := consumer.Consume(input, &target)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "surrogate")
	})

	t.Run("valid surrogate pair is accepted", func(t *testing.T) {
		input := bytes.NewReader([]byte(`{"text":"\uD83D\uDE00"}`))
		var target map[string]interface{}
		err := consumer.Consume(input, &target)
		require.NoError(t, err)
	})
}
