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

package ent

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
)

func Test_classSettings_Validate(t *testing.T) {
	class := &models.Class{
		Class: "test",
		Properties: []*models.Property{
			{
				DataType: []string{schema.DataTypeText.String()},
				Name:     "test",
			},
		},
	}
	tests := []struct {
		name    string
		cfg     moduletools.ClassConfig
		wantErr error
	}{
		{
			name: "text-embedding-3-small",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "text-embedding-3-small",
				},
			},
		},
		{
			name: "text-embedding-3-small, 512 dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-small",
					"dimensions": 512,
				},
			},
		},
		{
			name: "text-embedding-3-small, wrong dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-small",
					"dimensions": 1,
				},
			},
			wantErr: errors.New("wrong dimensions setting for text-embedding-3-small model, available dimensions are: [512 1536]"),
		},
		{
			name: "text-embedding-3-large",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "text-embedding-3-large",
				},
			},
		},
		{
			name: "text-embedding-3-large, 512 dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"dimensions": 1024,
				},
			},
		},
		{
			name: "text-embedding-3-large, wrong dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"dimensions": 512,
				},
			},
			wantErr: errors.New("wrong dimensions setting for text-embedding-3-large model, available dimensions are: [256 1024 3072]"),
		},
		{
			name: "text-embedding-ada-002",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":        "ada",
					"modelVersion": "002",
				},
			},
		},
		{
			name: "text-embedding-ada-002 - dimensions error",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "ada",
					"dimensions": 512,
				},
			},
			wantErr: errors.New("dimensions setting can only be used with V3 embedding models: [text-embedding-3-small text-embedding-3-large]"),
		},
		{
			name: "custom endpoint - no dimension validation",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "model-that-openai-does-not-have",
					"baseURL":    "https://something-else.com",
					"dimensions": 512,
				},
			},
		},
		{
			name: "text-embedding-ada-002 - wrong model version",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":        "ada",
					"modelVersion": "003",
				},
			},
			wantErr: errors.New("unsupported version 003"),
		},
		{
			name: "wrong model name",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "unknown-model",
				},
			},
			wantErr: errors.New("wrong OpenAI model name, available model names are: [ada babbage curie davinci text-embedding-3-small text-embedding-3-large]"),
		},
		{
			name: "third party provider",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":   "model-that-openai-does-not-have",
					"baseURL": "https://something-else.com",
				},
			},
		},
		{
			name: "wrong properties",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"properties": "wrong-properties",
				},
			},
			wantErr: errors.New("properties field needs to be of array type, got: string"),
		},
		{
			name: "wrong apiVersion",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"resourceName": "resource",
					"deploymentId": "deploymentId",
					"apiVersion":   "wrong-api-version",
				},
			},
			wantErr: errors.New("wrong Azure OpenAI apiVersion setting, available api versions are: " +
				"[2022-12-01 2023-03-15-preview 2023-05-15 2023-06-01-preview 2023-07-01-preview 2023-08-01-preview " +
				"2023-09-01-preview 2023-12-01-preview 2024-02-15-preview 2024-03-01-preview 2024-02-01]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(tt.cfg)
			err := cs.Validate(class)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_classSettings(t *testing.T) {
	t.Run("with target vector and properties", func(t *testing.T) {
		targetVector := "targetVector"
		propertyToIndex := "someProp"
		class := &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				targetVector: {
					Vectorizer: map[string]interface{}{
						"my-module": map[string]interface{}{
							"vectorizeClassName": false,
							"properties":         []interface{}{propertyToIndex},
						},
					},
					VectorIndexType: "hnsw",
				},
			},
			Properties: []*models.Property{
				{
					Name: propertyToIndex,
					ModuleConfig: map[string]interface{}{
						"my-module": map[string]interface{}{
							"skip":                  true,
							"vectorizePropertyName": true,
						},
					},
				},
				{
					Name: "otherProp",
				},
			},
		}

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector)
		ic := NewClassSettings(cfg)

		assert.True(t, ic.PropertyIndexed(propertyToIndex))
		assert.True(t, ic.VectorizePropertyName(propertyToIndex))
		assert.False(t, ic.PropertyIndexed("otherProp"))
		assert.False(t, ic.VectorizePropertyName("otherProp"))
		assert.False(t, ic.VectorizeClassName())
	})
}

func TestValidateModelVersion(t *testing.T) {
	type test struct {
		model    string
		docType  string
		version  string
		possible bool
	}

	tests := []test{
		// 001 models
		{"ada", "text", "001", true},
		{"ada", "code", "001", true},
		{"babbage", "text", "001", true},
		{"babbage", "code", "001", true},
		{"curie", "text", "001", true},
		{"curie", "code", "001", true},
		{"davinci", "text", "001", true},
		{"davinci", "code", "001", true},

		// 002 models
		{"ada", "text", "002", true},
		{"davinci", "text", "002", true},
		{"ada", "code", "002", false},
		{"babbage", "text", "002", false},
		{"babbage", "code", "002", false},
		{"curie", "text", "002", false},
		{"curie", "code", "002", false},
		{"davinci", "code", "002", false},

		// 003
		{"davinci", "text", "003", true},
		{"ada", "text", "003", false},
		{"babbage", "text", "003", false},

		// 004
		{"davinci", "text", "004", false},
		{"ada", "text", "004", false},
		{"babbage", "text", "004", false},
	}

	for _, test := range tests {
		name := fmt.Sprintf("model=%s docType=%s version=%s", test.model, test.docType, test.version)
		t.Run(name, func(t *testing.T) {
			err := (&classSettings{}).validateModelVersion(test.version, test.model, test.docType)
			if test.possible {
				assert.Nil(t, err, "this combination should be possible")
			} else {
				assert.NotNil(t, err, "this combination should not be possible")
			}
		})
	}
}

func Test_getModelString(t *testing.T) {
	t.Run("getModelStringDocument", func(t *testing.T) {
		type args struct {
			docType string
			model   string
			version string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-doc-001",
			},
			{
				name: "Document type: text model: ada-002 vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
					version: "002",
				},
				want: "text-embedding-ada-002",
			},
			{
				name: "Document type: text model: babbage vectorizationType: document",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-doc-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: document",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-doc-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: document",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-doc-001",
			},
			{
				name: "Document type: code model: ada vectorizationType: code",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-code-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: code",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-code-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cs := NewClassSettings(&fakeClassConfig{
					classConfig: map[string]interface{}{
						"type":         tt.args.docType,
						"model":        tt.args.model,
						"modelVersion": tt.args.version,
					},
				})
				if got := cs.ModelStringForAction("document"); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			docType string
			model   string
			version string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: query",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-query-001",
			},
			{
				name: "Document type: text model: babbage vectorizationType: query",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-query-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: query",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-query-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: query",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-query-001",
			},
			{
				name: "Document type: code model: ada vectorizationType: text",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-text-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: text",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-text-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cs := NewClassSettings(&fakeClassConfig{
					classConfig: map[string]interface{}{
						"type":         tt.args.docType,
						"model":        tt.args.model,
						"modelVersion": tt.args.version,
					},
				})
				if got := cs.ModelStringForAction("query"); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}

func TestPickDefaultModelVersion(t *testing.T) {
	t.Run("ada with text", func(t *testing.T) {
		version := PickDefaultModelVersion("ada", "text")
		assert.Equal(t, "002", version)
	})

	t.Run("ada with code", func(t *testing.T) {
		version := PickDefaultModelVersion("ada", "code")
		assert.Equal(t, "001", version)
	})

	t.Run("with curie", func(t *testing.T) {
		version := PickDefaultModelVersion("curie", "text")
		assert.Equal(t, "001", version)
	})
}

func TestClassSettings(t *testing.T) {
	type testCase struct {
		expectedBaseURL       string
		expectedDimensions    int64
		expectedNilDimensions bool
		cfg                   moduletools.ClassConfig
	}
	tests := []testCase{
		{
			cfg: fakeClassConfig{
				classConfig: make(map[string]interface{}),
			},
			expectedBaseURL:    DefaultBaseURL,
			expectedDimensions: 1536,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL": "https://proxy.weaviate.dev",
				},
			},
			expectedBaseURL:    "https://proxy.weaviate.dev",
			expectedDimensions: 1536,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL":    "https://proxy.weaviate.dev",
					"dimensions": 768,
				},
			},
			expectedBaseURL:    "https://proxy.weaviate.dev",
			expectedDimensions: 768,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL":      "https://proxy.weaviate.dev",
					"resourceName": "resourceName",
					"deploymentId": "deploymentId",
				},
			},
			expectedBaseURL:       "https://proxy.weaviate.dev",
			expectedNilDimensions: true,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL":      "https://proxy.weaviate.dev",
					"resourceName": "resourceName",
					"deploymentId": "deploymentId",
					"dimensions":   1024,
				},
			},
			expectedBaseURL:       "https://proxy.weaviate.dev",
			expectedNilDimensions: false,
			expectedDimensions:    1024,
		},
	}

	for _, tt := range tests {
		ic := NewClassSettings(tt.cfg)
		assert.Equal(t, tt.expectedBaseURL, ic.BaseURL())
		if tt.expectedNilDimensions {
			assert.Nil(t, ic.Dimensions())
		} else {
			assert.Equal(t, tt.expectedDimensions, *ic.Dimensions())
		}
	}
}
