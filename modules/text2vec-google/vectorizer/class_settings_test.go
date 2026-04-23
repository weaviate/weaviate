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
		wantProjectID   string
		wantModelID     string
		wantTitle       string
		wantTaskType    string
		wantDimensions  *int64
		wantErr         error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
				},
			},
			wantApiEndpoint: "us-central1-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "gemini-embedding-001",
			wantTaskType:    DefaultTaskType,
			wantDimensions:  &DefaultDimensions,
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint":   "google.com",
					"projectId":     "projectId",
					"titleProperty": "title",
					"taskType":      "CODE_RETRIEVAL_QUERY",
				},
			},
			wantApiEndpoint: "google.com",
			wantProjectID:   "projectId",
			wantModelID:     "gemini-embedding-001",
			wantTitle:       "title",
			wantTaskType:    "CODE_RETRIEVAL_QUERY",
			wantDimensions:  &DefaultDimensions,
			wantErr:         nil,
		},
		{
			name: "empty projectId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "",
				},
			},
			wantErr: errors.Errorf("projectId cannot be empty"),
		},
		{
			name: "empty projectId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "",
					"modelId":   "wrong-model",
				},
			},
			wantErr: errors.Errorf("projectId cannot be empty"),
		},
		{
			name: "Generative AI",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "gemini-embedding-001",
			wantTaskType:    DefaultTaskType,
			wantDimensions:  &DefaultDimensions,
			wantErr:         nil,
		},
		{
			name: "Generative AI with model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "embedding-gecko-001",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "embedding-gecko-001",
			wantTaskType:    DefaultTaskType,
			wantDimensions:  nil,
			wantErr:         nil,
		},
		{
			name: "wrong properties",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
				},
				properties: "wrong-properties",
			},
			wantApiEndpoint: "us-central1-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "textembedding-gecko@001",
			wantTaskType:    DefaultTaskType,
			wantDimensions:  nil,
			wantErr:         errors.New("properties field needs to be of array type, got: string"),
		},
		{
			name: "wrong taskType",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
					"taskType":  "wrong-task-type",
				},
			},
			wantErr: errors.Errorf("wrong taskType supported task types are: " +
				"[RETRIEVAL_QUERY QUESTION_ANSWERING FACT_VERIFICATION CODE_RETRIEVAL_QUERY CLASSIFICATION CLUSTERING SEMANTIC_SIMILARITY]"),
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
				assert.Equal(t, tt.wantProjectID, ic.ProjectID())
				assert.Equal(t, tt.wantModelID, ic.Model())
				assert.Equal(t, tt.wantTitle, ic.TitleProperty())
				assert.Equal(t, tt.wantTaskType, ic.TaskType())
				assert.Equal(t, tt.wantDimensions, ic.Dimensions())
			}
		})
	}
}
