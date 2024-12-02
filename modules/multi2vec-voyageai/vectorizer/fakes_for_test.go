package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/ent"
)

type fakeClient struct {
	lastInput  []string
	lastImages []string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	texts []string, images []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = texts
	c.lastImages = images
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		TextVectors:  [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}},
		ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}},
	}, nil
}

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

func (b *builder) addWeights(textWeights, imageWeights []interface{}) *builder {
	if textWeights != nil || imageWeights != nil {
		weightSettings := map[string]interface{}{}
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
