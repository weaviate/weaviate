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

package batchclip

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const image = "iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFts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"

func Test_BatchCLIPVectorizer(t *testing.T) {
	t.Run("should vectorize image", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := New("moduleName", client)
		config := newConfigBuilder().addSetting("imageFields", []any{"image"}).build()

		props := map[string]any{
			"image": image,
		}
		objects := []*models.Object{
			{ID: "some-uuid", Properties: props},
		}

		// when
		vector, _, err := vectorizer.Objects(context.Background(), objects, config)

		// then
		require.NoError(t, err)
		assert.NotEmpty(t, vector)
		assert.NotNil(t, vector[0])
	})

	t.Run("should vectorize 2 image fields", func(t *testing.T) {
		// given
		client := &fakeClient{
			result: &modulecomponents.VectorizationCLIPResult[[]float32]{
				ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}, {10.1, 20.1, 30.1, 40.1, 50.1}},
			},
		}
		vectorizer := New("moduleName", client)
		config := newConfigBuilder().addSetting("imageFields", []any{"image1", "image2"}).build()

		props := map[string]any{
			"image1": image,
			"image2": image,
		}
		objects := []*models.Object{
			{ID: "some-uuid", Properties: props},
		}

		// when
		vector, _, err := vectorizer.Objects(context.Background(), objects, config)

		// then
		require.NoError(t, err)
		assert.NotEmpty(t, vector)
		assert.NotNil(t, vector[0])
	})

	t.Run("should vectorize 4 objects with 2 image fields and 1 text field", func(t *testing.T) {
		// given
		client := &fakeClient{
			result: &modulecomponents.VectorizationCLIPResult[[]float32]{
				TextVectors: [][]float32{
					{1.0, 2.0, 3.0, 4.0, 5.0},
					{1.0, 2.0, 3.0, 4.0, 5.0},
					{1.0, 2.0, 3.0, 4.0, 5.0},
					{1.0, 2.0, 3.0, 4.0, 5.0},
				},
				ImageVectors: [][]float32{
					{10.0, 20.0, 30.0, 40.0, 50.0},
					{10.1, 20.1, 30.1, 40.1, 50.1},
					{10.0, 20.0, 30.0, 40.0, 50.0},
					{10.1, 20.1, 30.1, 40.1, 50.1},
					{10.0, 20.0, 30.0, 40.0, 50.0},
					{10.1, 20.1, 30.1, 40.1, 50.1},
					{10.0, 20.0, 30.0, 40.0, 50.0},
					{10.1, 20.1, 30.1, 40.1, 50.1},
				},
			},
		}
		vectorizer := New("moduleName", client)
		config := newConfigBuilder().
			addSetting("imageFields", []any{"image1", "image2"}).
			addSetting("textFields", []any{"text1"}).
			build()

		props := map[string]any{
			"image1": image,
			"image2": image,
			"text1":  "text",
		}
		objects := []*models.Object{
			{ID: "some-uuid1", Properties: props},
			{ID: "some-uuid2", Properties: props},
			{ID: "some-uuid3", Properties: props},
			{ID: "some-uuid4", Properties: props},
		}

		// when
		vectors, _, err := vectorizer.Objects(context.Background(), objects, config)

		// then
		require.NoError(t, err)
		assert.NotEmpty(t, vectors)
		assert.Len(t, vectors, 4)
		for _, vector := range vectors {
			assert.NotEmpty(t, vector)
		}
	})

	t.Run("should return error if for one of the vectorizable fields vector was not found", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := New("moduleName", client)
		config := newConfigBuilder().addSetting("imageFields", []any{"image1", "image2"}).build()

		props := map[string]any{
			"image1": image,
			"image2": image,
		}
		objects := []*models.Object{
			{ID: "some-uuid", Properties: props},
		}

		// when
		vector, _, err := vectorizer.Objects(context.Background(), objects, config)

		// then
		require.Error(t, err)
		assert.Empty(t, vector)
	})
}

func Test_BatchCLIPVectorizer_WithDiff(t *testing.T) {
	type testCase struct {
		name    string
		objects []*models.Object
	}

	props := map[string]any{
		"image":       image,
		"text":        "text",
		"description": "non-vectorizable",
	}

	tests := []testCase{
		{
			name: "noop comp",
			objects: []*models.Object{
				{ID: "some-uuid", Properties: props},
			},
		},
		{
			name: "one vectorizable prop changed (1)",
			objects: []*models.Object{
				{ID: "some-uuid", Properties: props},
			},
		},
		{
			name: "one vectorizable prop changed (2)",
			objects: []*models.Object{
				{ID: "some-uuid", Properties: props},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			vectorizer := New("moduleName", client)
			config := newConfigBuilder().
				addSetting("imageFields", []any{"image"}).
				addSetting("textFields", []any{"text"}).
				build()

			vectors, _, err := vectorizer.Objects(context.Background(), test.objects, config)

			require.NoError(t, err)
			require.NotEmpty(t, vectors)
			assert.Equal(t, []float32{5.5, 11, 16.5, 22, 27.5}, vectors[0])
		})
	}
}

func Test_BatchCLIPVectorizer_WithWeights(t *testing.T) {
	client := &fakeClient{}
	vectorizer := New("moduleName", client)
	config := newConfigBuilder().
		addSetting("imageFields", []any{"image"}).
		addSetting("textFields", []any{"text"}).
		addWeights([]any{0.4}, []any{0.6}).
		build()

	props := map[string]any{
		"image":       image,
		"text":        "text",
		"description": "non-vectorizable",
	}
	objects := []*models.Object{
		{ID: "some-uuid", Properties: props},
	}

	vectors, _, err := vectorizer.Objects(context.Background(), objects, config)

	require.NoError(t, err)
	require.NotEmpty(t, vectors)
	assert.Equal(t, []float32{3.2, 6.4, 9.6, 12.8, 16}, vectors[0])
	// vectors are defined in Vectorize within fakes_for_test.go
	// result calculated with above weights as (textVectors[0][i]*0.4+imageVectors[0][i]*0.6) / 2
}

func Test_BatchCLIPVectorizer_normalizeWeights(t *testing.T) {
	tests := []struct {
		name    string
		weights []float32
	}{
		{
			name:    "normalize example 1",
			weights: []float32{200, 100, 0.1},
		},
		{
			name:    "normalize example 2",
			weights: []float32{300.22, 0.7, 17, 54},
		},
		{
			name:    "normalize example 3",
			weights: []float32{300, 0.02, 17},
		},
		{
			name:    "normalize example 4",
			weights: []float32{500, 0.02, 17.4, 180},
		},
		{
			name:    "normalize example 5",
			weights: []float32{500, 0.02, 17.4, 2, 4, 5, .88},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &BatchCLIPVectorizer[[]float32]{}
			if got := v.normalizeWeights(tt.weights); !checkNormalization(got) {
				t.Errorf("Vectorizer.normalizeWeights() = %v, want %v", got, 1.0)
			}
		})
	}
}

func checkNormalization(weights []float32) bool {
	var result float32
	for i := range weights {
		result += weights[i]
	}
	return result == 1.0
}

type builder struct {
	fakeClassConfig *fakeClassConfig
}

func newConfigBuilder() *builder {
	return &builder{
		fakeClassConfig: &fakeClassConfig{config: map[string]any{}},
	}
}

func (b *builder) addSetting(name string, value any) *builder {
	b.fakeClassConfig.config[name] = value
	return b
}

func (b *builder) addWeights(textWeights, imageWeights []any) *builder {
	if textWeights != nil || imageWeights != nil {
		weightSettings := map[string]any{}
		if textWeights != nil {
			weightSettings["textFields"] = textWeights
		}
		if imageWeights != nil {
			weightSettings["imageFields"] = imageWeights
		}
		b.fakeClassConfig.config["weights"] = weightSettings
	}
	return b
}

func (b *builder) build() *fakeClassConfig {
	return b.fakeClassConfig
}

type fakeClassConfig struct {
	config map[string]any
}

func (c fakeClassConfig) Class() map[string]any {
	return c.config
}

func (c fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return c.config
}

func (c fakeClassConfig) Property(propName string) map[string]any {
	return c.config
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

type fakeClient struct {
	result *modulecomponents.VectorizationCLIPResult[[]float32]
}

func (c *fakeClient) VectorizeImages(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return c.getEmbeddings()
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	texts []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return c.getEmbeddings()
}

func (c *fakeClient) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return c.getEmbeddings()
}

func (c *fakeClient) getEmbeddings() (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	if c.result != nil {
		return c.result, nil
	}
	result := &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors:  [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}},
		ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}},
	}
	return result, nil
}
