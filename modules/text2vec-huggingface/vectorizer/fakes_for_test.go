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

	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
)

type fakeClient struct {
	lastInput  string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     []float32{0, 1, 2, 3},
		Dimensions: 4,
		Text:       text,
	}, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     []float32{0.1, 1.1, 2.1, 3.1},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	// module specific settings
	model, passageModel, queryModel string
	waitForModel, useGPU, useCache  bool
	endpointURL                     string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	if len(f.classConfig) > 0 {
		return f.classConfig
	}
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.model,
		"passageModel":       f.passageModel,
		"queryModel":         f.queryModel,
		"waitForModel":       f.waitForModel,
		"useGPU":             f.useGPU,
		"useCache":           f.useCache,
		"endpointURL":        f.endpointURL,
	}
	return classSettings
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}
