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
		cfg         map[string]any
		expectError bool
	}{
		{
			name: "valid default model",
			cfg:  map[string]any{},
		},
		{
			name: "valid specific model v1",
			cfg: map[string]any{
				"model": "v1",
			},
		},
		{
			name: "valid specific model v2",
			cfg: map[string]any{
				"model": "v2",
			},
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
		cfg      map[string]any
		expected string
	}{
		{
			name:     "default model",
			cfg:      map[string]any{},
			expected: DefaultContextualAIModel,
		},
		{
			name: "specific model v1",
			cfg: map[string]any{
				"model": "v1",
			},
			expected: "v1",
		},
		{
			name: "specific model v2",
			cfg: map[string]any{
				"model": "v2",
			},
			expected: "v2",
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

func TestClassSettingsTemperature(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]any
		expected float64
	}{
		{
			name:     "default temperature",
			cfg:      map[string]any{},
			expected: DefaultContextualAITemperature,
		},
		{
			name: "custom temperature",
			cfg: map[string]any{
				"temperature": 0.5,
			},
			expected: 0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			temperature := settings.Temperature()
			assert.Equal(t, tt.expected, *temperature)
		})
	}
}

func TestClassSettingsAvoidCommentary(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]any
		expected bool
	}{
		{
			name:     "default avoid commentary",
			cfg:      map[string]any{},
			expected: DefaultContextualAIAvoidCommentary,
		},
		{
			name: "custom avoid commentary true",
			cfg: map[string]any{
				"avoidCommentary": true,
			},
			expected: true,
		},
		{
			name: "custom avoid commentary false",
			cfg: map[string]any{
				"avoidCommentary": false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeConfig := fakeClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(fakeConfig)

			avoidCommentary := settings.AvoidCommentary()
			assert.Equal(t, tt.expected, *avoidCommentary)
		})
	}
}
