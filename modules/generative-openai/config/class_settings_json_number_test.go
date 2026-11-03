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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The REST API decodes a new class's moduleConfig numbers as json.Number.
func TestValidateJSONNumberSettings(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]any
		wantErr string
	}{
		{name: "temperature 0.5", cfg: map[string]any{"temperature": json.Number("0.5")}},
		{name: "temperature 1.0", cfg: map[string]any{"temperature": json.Number("1.0")}},
		{name: "topP 0.9", cfg: map[string]any{"topP": json.Number("0.9")}},
		{name: "frequencyPenalty 0.2", cfg: map[string]any{"frequencyPenalty": json.Number("0.2")}},
		{name: "presencePenalty 0.2", cfg: map[string]any{"presencePenalty": json.Number("0.2")}},
		{
			name:    "temperature 1.5 out of range",
			cfg:     map[string]any{"temperature": json.Number("1.5")},
			wantErr: "Wrong temperature configuration, values are between 0.0 and 1.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewClassSettings(fakeClassConfig{classConfig: tt.cfg}).Validate(nil)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}
