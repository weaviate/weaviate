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

package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type builder struct {
	fakeClassConfig *fakeClassConfig
}

func newConfigBuilder() *builder {
	return &builder{
		fakeClassConfig: &fakeClassConfig{config: map[string]interface{}{}},
	}
}

func (b *builder) addSetting(name string, value interface{}) *builder {
	b.fakeClassConfig.config[name] = value
	return b
}

func (b *builder) addWeights(textWeights, imageWeights, videoWeights []interface{}) *builder {
	if textWeights != nil || imageWeights != nil || videoWeights != nil {
		weightSettings := map[string]interface{}{}
		if textWeights != nil {
			weightSettings["textFields"] = textWeights
		}
		if imageWeights != nil {
			weightSettings["imageFields"] = imageWeights
		}
		if videoWeights != nil {
			weightSettings["videoFields"] = videoWeights
		}
		b.fakeClassConfig.config["weights"] = weightSettings
	}
	return b
}

func (b *builder) build() *fakeClassConfig {
	return b.fakeClassConfig
}

type fakeClassConfig struct {
	config map[string]interface{}
}

func (c fakeClassConfig) Class() map[string]interface{} {
	return c.config
}

func (c fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return c.config
}

func (c fakeClassConfig) Property(propName string) map[string]interface{} {
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

type fakeClient struct{}

func (c *fakeClient) Vectorize(ctx context.Context,
	texts, images, videos []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	result := &modulecomponents.VectorizationCLIPResult[[]float32]{}
	if len(texts) > 0 {
		result.TextVectors = [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}}
	}
	if len(images) > 0 {
		result.ImageVectors = [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}}
	}
	if len(videos) > 0 {
		result.VideoVectors = [][]float32{{100.0, 200.0, 300.0, 400.0, 500.0}}
	}
	return result, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	result := &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors: [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}},
	}
	return result, nil
}

func (c *fakeClient) VectorizeImageQuery(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	result := &modulecomponents.VectorizationCLIPResult[[]float32]{
		ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}},
	}
	return result, nil
}

func (c *fakeClient) VectorizeVideoQuery(ctx context.Context,
	videos []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	result := &modulecomponents.VectorizationCLIPResult[[]float32]{
		VideoVectors: [][]float32{{100.0, 200.0, 300.0, 400.0, 500.0}},
	}
	return result, nil
}
