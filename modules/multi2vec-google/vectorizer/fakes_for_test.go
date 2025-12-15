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

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/multi2vec-google/ent"
	"github.com/weaviate/weaviate/usecases/config"
)

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

type fakeClient struct{}

func (c *fakeClient) Vectorize(ctx context.Context,
	texts, images, videos []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	result := &ent.VectorizationResult{
		TextVectors:  [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}},
		ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}},
	}
	return result, nil
}
