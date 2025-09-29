//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestClassSettingsValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         map[string]interface{}
		expectError bool
	}{
		{
			name: "valid default model",
			cfg:  map[string]interface{}{},
		},
		{
			name: "valid specific model",
			cfg: map[string]interface{}{
				"model": "ctxl-rerank-v2-instruct-multilingual-mini",
			},
		},
		{
			name: "invalid model",
			cfg: map[string]interface{}{
				"model": "invalid-model",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			err := settings.Validate(&models.Class{})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClassSettingsModel(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]interface{}
		expected string
	}{
		{
			name:     "default model",
			cfg:      map[string]interface{}{},
			expected: DefaultContextualAIModel,
		},
		{
			name: "specific model",
			cfg: map[string]interface{}{
				"model": "ctxl-rerank-v1-instruct",
			},
			expected: "ctxl-rerank-v1-instruct",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			model := settings.Model()
			assert.Equal(t, tt.expected, model)
		})
	}
}

func TestClassSettingsInstruction(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]interface{}
		expected string
	}{
		{
			name:     "no instruction",
			cfg:      map[string]interface{}{},
			expected: "",
		},
		{
			name: "with instruction",
			cfg: map[string]interface{}{
				"instruction": "Prioritize recent documents",
			},
			expected: "Prioritize recent documents",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			instruction := settings.Instruction()
			assert.Equal(t, tt.expected, instruction)
		})
	}
}

func TestClassSettingsTopN(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]interface{}
		expected int
	}{
		{
			name:     "no topN",
			cfg:      map[string]interface{}{},
			expected: 0,
		},
		{
			name: "with topN",
			cfg: map[string]interface{}{
				"topN": 10,
			},
			expected: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			topN := settings.TopN()
			assert.Equal(t, tt.expected, topN)
		})
	}
}
