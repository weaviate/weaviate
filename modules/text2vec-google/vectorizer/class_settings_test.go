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
		wantLocation    string
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
			wantDimensions:  &DefaultDimensions,
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint":   "europe-west4-aiplatform.googleapis.com",
					"projectId":     "projectId",
					"titleProperty": "title",
					"taskType":      "CODE_RETRIEVAL_QUERY",
				},
			},
			wantApiEndpoint: "europe-west4-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "gemini-embedding-001",
			wantTitle:       "title",
			wantTaskType:    "CODE_RETRIEVAL_QUERY",
			wantDimensions:  &DefaultDimensions,
			wantErr:         nil,
		},
		{
			name: "custom location",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
					"location":  "europe-west1",
				},
			},
			wantApiEndpoint: "us-central1-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "gemini-embedding-001",
			wantLocation:    "europe-west1",
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
			name: "apiEndpoint outside the Google API domain",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "attacker.example.com",
					"projectId":   "projectId",
				},
			},
			wantErr: errors.Errorf("apiEndpoint must be a Google API host ending in .googleapis.com, got \"attacker.example.com\""),
		},
		{
			name: "location carrying a host",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
					"location":  "attacker.example.com/",
				},
			},
			wantErr: errors.Errorf("location must be a Google region name, got \"attacker.example.com/\""),
		},
		{
			name: "Vertex-only model on a Vertex endpoint",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "europe-west4-aiplatform.googleapis.com",
					"projectId":   "projectId",
					"location":    "europe-west4",
					"model":       "text-embedding-005",
				},
			},
			wantApiEndpoint: "europe-west4-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "text-embedding-005",
			wantLocation:    "europe-west4",
			wantDimensions:  nil,
			wantErr:         nil,
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
				assert.EqualError(t, ic.Validate(classForSettingsValidation()), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantApiEndpoint, ic.ApiEndpoint())
				assert.Equal(t, tt.wantProjectID, ic.ProjectID())
				assert.Equal(t, tt.wantModelID, ic.Model())
				assert.Equal(t, tt.wantTitle, ic.TitleProperty())
				assert.Equal(t, wantOrDefault(tt.wantLocation, DefaultLocation), ic.Location())
				assert.Equal(t, wantOrDefault(tt.wantTaskType, DefaultTaskType), ic.TaskType())
				assert.Equal(t, tt.wantDimensions, ic.Dimensions())
			}
		})
	}
}

func classForSettingsValidation() *models.Class {
	return &models.Class{Class: "Test", Properties: []*models.Property{
		{
			Name:     "test",
			DataType: []string{schema.DataTypeText.String()},
		},
	}}
}

func wantOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func TestMutableSettings(t *testing.T) {
	aiStudio := map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint}
	vertex := func(setting ...interface{}) map[string]interface{} {
		settings := map[string]interface{}{"apiEndpoint": DefaultApiEndpoint, "projectId": "project"}
		if len(setting) == 2 {
			settings[setting[0].(string)] = setting[1]
		}
		return settings
	}
	vertexModel := func(model string, setting ...interface{}) map[string]interface{} {
		settings := vertex(setting...)
		settings["model"] = model
		return settings
	}

	tests := []struct {
		name    string
		current map[string]interface{}
		updated map[string]interface{}
		want    bool
	}{
		{
			name:    "gemini-embedding-001 on both sides",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "model": "gemini-embedding-001"},
			updated: vertex("model", "gemini-embedding-001"),
			want:    true,
		},
		{name: "default model on both sides", current: aiStudio, updated: vertex(), want: true},
		{name: "model setting added with the same effective model", current: aiStudio, updated: vertex("model", "gemini-embedding-001")},
		{
			name:    "Vertex model other than gemini-embedding-001, endpoint and project change",
			current: vertexModel("text-embedding-005"),
			updated: map[string]interface{}{"apiEndpoint": "europe-west4-aiplatform.googleapis.com", "projectId": "other", "model": "text-embedding-005"},
			want:    true,
		},
		{
			name:    "Vertex model other than gemini-embedding-001, location change",
			current: vertexModel("text-embedding-005", "location", "us-central1"),
			updated: vertexModel("text-embedding-005", "location", "europe-west4"),
			want:    true,
		},
		{
			name:    "Vertex model other than gemini-embedding-001, project change with explicit dimensions",
			current: vertexModel("text-embedding-004", "dimensions", 768),
			updated: map[string]interface{}{"apiEndpoint": DefaultApiEndpoint, "projectId": "other", "model": "text-embedding-004", "dimensions": 768},
			want:    true,
		},
		{
			name:    "same model set through modelId on both sides",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "modelId": "text-embedding-004"},
			updated: vertex("modelId", "text-embedding-004"),
			want:    true,
		},
		{
			name:    "gemini-embedding-001 set through modelId on both sides",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "modelId": "gemini-embedding-001", "dimensions": 1536},
			updated: map[string]interface{}{"apiEndpoint": DefaultApiEndpoint, "projectId": "project", "location": "us-central1", "modelId": "gemini-embedding-001", "dimensions": 1536},
			want:    true,
		},
		{
			name:    "same explicit dimensions on both sides",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "dimensions": 1536},
			updated: vertex("dimensions", 1536),
			want:    true,
		},
		{
			name:    "dimensions setting removed with the same effective dimensions",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "dimensions": 768},
			updated: vertex(),
		},
		{name: "model changes from the default", current: aiStudio, updated: vertex("model", "text-embedding-005")},
		{name: "model set through modelId changes from the default", current: aiStudio, updated: vertex("modelId", "text-embedding-005")},
		{name: "non-gemini model changes", current: vertexModel("text-embedding-004"), updated: vertexModel("text-embedding-005")},
		{
			name:    "non-gemini model changes together with the endpoint",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "model": "text-embedding-004"},
			updated: vertexModel("text-embedding-005"),
		},
		{
			name:    "explicit dimensions on one side, default on the other",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "dimensions": 1536},
			updated: vertex(),
		},
		{
			name:    "different explicit dimensions",
			current: map[string]interface{}{"apiEndpoint": DefaultAIStudioEndpoint, "dimensions": 1536},
			updated: vertex("dimensions", 3072),
		},
		{
			name:    "dimensions change for a non-gemini model",
			current: vertexModel("text-embedding-005", "dimensions", 256),
			updated: vertexModel("text-embedding-005", "dimensions", 768),
		},
		{name: "taskType changes with the endpoint", current: aiStudio, updated: vertex("taskType", "CLUSTERING")},
		{name: "titleProperty changes", current: vertex(), updated: vertex("titleProperty", "title")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MutableSettings(fakeClassConfig{classConfig: tt.current}, fakeClassConfig{classConfig: tt.updated})
			assert.Equal(t, tt.want, got)
		})
	}
}
