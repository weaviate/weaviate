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

package vectorizer

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantApiEndpoint string
		wantModelID     string
		wantErr         error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantApiEndpoint: "http://localhost:11434",
			wantModelID:     "nomic-embed-text",
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "https://localhost:11434",
					"modelId":     "future-text-embed",
				},
			},
			wantApiEndpoint: "https://localhost:11434",
			wantModelID:     "future-text-embed",
			wantErr:         nil,
		},
		{
			name: "empty endpoint",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "",
					"modelId":     "test",
				},
			},
			wantErr: errors.Errorf("apiEndpoint cannot be empty"),
		},
		{
			name: "empty model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "http://localhost:8080",
					"modelId":     "",
				},
			},
			wantErr: errors.Errorf("modelId cannot be empty"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(&models.Class{Class: "Test", Properties: []*models.Property{
					{
						Name:     "test",
						DataType: []string{schema.DataTypeText.String()},
					},
				}}), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantApiEndpoint, ic.ApiEndpoint())
				assert.Equal(t, tt.wantModelID, ic.ModelID())
			}
		})
	}
}
