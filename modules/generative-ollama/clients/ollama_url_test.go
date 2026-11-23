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

package ollama

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOllamaURLHeader(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		validate bool
		wantURL  string
		wantErr  string
	}{
		{name: "no header uses the class baseURL", wantURL: "http://localhost:11434/api/generate"},
		{name: "header replaces the class baseURL", header: "http://ollama.example.com", wantURL: "http://ollama.example.com/api/generate"},
		{
			name:     "header is checked when validation is on",
			header:   "https://169.254.169.254",
			validate: true,
			wantErr:  "baseURL cannot target internal addresses",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validate {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Ollama-BaseURL", []string{tt.header})
			}

			got, err := New(0, nullLogger()).getOllamaUrl(ctx, "http://localhost:11434")

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}
