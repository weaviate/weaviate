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

package text2vecbase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestBatchTokenizerWithEmptyProperties(t *testing.T) {
	className := "TestClass"
	tests := []struct {
		name                   string
		vectorizableProperties []string
		objects                []*models.Object
		wantTexts              []string
		wantSkipObjects        []bool
	}{
		{
			name:                   "all objects vectorizable",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": "a"}},
				{Class: className, Properties: map[string]any{"text_prop": "b"}},
				{Class: className, Properties: map[string]any{"text_prop": "c"}},
			},
			wantTexts:       []string{"a", "b", "c"},
			wantSkipObjects: []bool{false, false, false},
		},
		{
			name:                   "one empty",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": "a"}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": "c"}},
			},
			wantTexts:       []string{"a", "", "c"},
			wantSkipObjects: []bool{false, true, false},
		},
		{
			name:                   "all empty",
			vectorizableProperties: []string{"text_prop"},
			objects: []*models.Object{
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
				{Class: className, Properties: map[string]any{"text_prop": ""}},
			},
			wantTexts:       []string{"", "", ""},
			wantSkipObjects: []bool{true, true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetVector := "targetVector"
			class := &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					targetVector: {
						Vectorizer: map[string]any{
							"my-module": map[string]any{
								"vectorizeClassName": false,
								"properties":         tt.vectorizableProperties,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector, nil)
			tokenizer := batch.ReturnBatchTokenizer(1, "", false)
			vectorizer := objectsvectorizer.New()
			encoderCache := batch.NewEncoderCache()
			skipObjects := make([]bool, len(tt.objects))
			texts, tokenCounts, skippedObjects, skipAll, err := tokenizer(context.TODO(), tt.objects, skipObjects, cfg, vectorizer, encoderCache)
			require.NoError(t, err)
			require.False(t, skipAll)
			assert.Equal(t, len(texts), len(tokenCounts))
			assert.Equal(t, tt.wantTexts, texts)
			assert.Equal(t, tt.wantSkipObjects, skippedObjects)
		})
	}
}

type fakeClassConfig struct {
	classConfig           map[string]any
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	properties            []string
	// module specific settings
	cohereModel  string
	truncateType string
	baseURL      string
}

func (f fakeClassConfig) Class() map[string]any {
	classSettings := map[string]any{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.cohereModel,
		"truncate":           f.truncateType,
		"baseURL":            f.baseURL,
	}
	return classSettings
}

func (f fakeClassConfig) PropertyIndexed(property string) bool {
	return !((property == f.skippedProperty) || (property == f.excludedProperty))
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	if propName == f.skippedProperty {
		return map[string]any{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]any{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]any{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) VectorizeClassName() bool {
	return f.classConfig["vectorizeClassName"].(bool)
}

func (f fakeClassConfig) VectorizePropertyName(propertyName string) bool {
	return f.vectorizePropertyName
}

func (f fakeClassConfig) Properties() []string {
	return f.properties
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func BenchmarkBatchVectorizer(b *testing.B) {
	tokenizer := batch.ReturnBatchTokenizer(1, "", false)
	ctx := context.Background()
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]any{"vectorizeClassName": false}}

	vectorizer := objectsvectorizer.New()
	encoderCache := batch.NewEncoderCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, err := tokenizer(ctx, []*models.Object{}, []bool{false}, cfg, vectorizer, encoderCache)
		require.NoError(b, err)
		require.Len(b, encoderCache, 1)
	}
}
