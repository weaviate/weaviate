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
	"context"

	"github.com/weaviate/weaviate/modules/multi2vec-bind/ent"
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

func (b *builder) addWeights(textWeights, imageWeights, audioWeights,
	videoWeights, imuWeights, thermalWeights, depthWeights []interface{},
) *builder {
	weightSettings := map[string]interface{}{}
	if textWeights != nil {
		weightSettings["textFields"] = textWeights
	}
	if imageWeights != nil {
		weightSettings["imageFields"] = imageWeights
	}
	if audioWeights != nil {
		weightSettings["audioFields"] = audioWeights
	}
	if videoWeights != nil {
		weightSettings["videoFields"] = videoWeights
	}
	if imuWeights != nil {
		weightSettings["imuFields"] = imuWeights
	}
	if thermalWeights != nil {
		weightSettings["thermalFields"] = thermalWeights
	}
	if depthWeights != nil {
		weightSettings["depthFields"] = depthWeights
	}
	if len(weightSettings) > 0 {
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

func (c fakeClassConfig) Tenant() string {
	return ""
}

type fakeClient struct{}

func (c *fakeClient) Vectorize(ctx context.Context,
	texts, images, audio, video, imu, thermal, depth []string,
) (*ent.VectorizationResult, error) {
	result := &ent.VectorizationResult{
		TextVectors:  [][]float32{{1.0, 2.0, 3.0, 4.0, 5.0}},
		ImageVectors: [][]float32{{10.0, 20.0, 30.0, 40.0, 50.0}},
	}
	return result, nil
}
