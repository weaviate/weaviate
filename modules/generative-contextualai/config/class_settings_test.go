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
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClassSettings_Validate(t *testing.T) {
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
			name: "valid specific model v1",
			cfg: map[string]interface{}{
				"model": "v1",
			},
		},
		{
			name: "valid specific model v2",
			cfg: map[string]interface{}{
				"model": "v2",
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
			mockConfig := &mockClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(mockConfig)

			err := settings.Validate(&models.Class{})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClassSettings_Model(t *testing.T) {
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
			name: "specific model v1",
			cfg: map[string]interface{}{
				"model": "v1",
			},
			expected: "v1",
		},
		{
			name: "specific model v2",
			cfg: map[string]interface{}{
				"model": "v2",
			},
			expected: "v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConfig := &mockClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(mockConfig)

			model := settings.Model()
			assert.Equal(t, tt.expected, model)
		})
	}
}

func TestClassSettings_Temperature(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]interface{}
		expected float64
	}{
		{
			name:     "default temperature",
			cfg:      map[string]interface{}{},
			expected: DefaultContextualAITemperature,
		},
		{
			name: "custom temperature",
			cfg: map[string]interface{}{
				"temperature": 0.5,
			},
			expected: 0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConfig := &mockClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(mockConfig)

			temperature := settings.Temperature()
			assert.Equal(t, tt.expected, *temperature)
		})
	}
}

func TestClassSettings_AvoidCommentary(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]interface{}
		expected bool
	}{
		{
			name:     "default avoid commentary",
			cfg:      map[string]interface{}{},
			expected: DefaultContextualAIAvoidCommentary,
		},
		{
			name: "custom avoid commentary true",
			cfg: map[string]interface{}{
				"avoidCommentary": true,
			},
			expected: true,
		},
		{
			name: "custom avoid commentary false",
			cfg: map[string]interface{}{
				"avoidCommentary": false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConfig := &mockClassConfig{classConfig: tt.cfg}
			settings := NewClassSettings(mockConfig)

			avoidCommentary := settings.AvoidCommentary()
			assert.Equal(t, tt.expected, *avoidCommentary)
		})
	}
}

// mockClassConfig implements moduletools.ClassConfig for testing
type mockClassConfig struct {
	classConfig map[string]interface{}
}

func (m *mockClassConfig) Class() map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) Property(propName string) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *mockClassConfig) Tenant() string {
	return ""
}

func (m *mockClassConfig) TargetVector() string {
	return ""
}

func (m *mockClassConfig) Config() *config.Config {
	return nil
}

func (m *mockClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
