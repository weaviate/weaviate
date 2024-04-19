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

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type fakeClient struct {
	lastInput  []string
	lastConfig moduletools.ClassConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       text,
	}, nil, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

func (c *fakeClient) GetVectorizerRateLimit(ctx context.Context) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{}
}

func (c *fakeClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	jinaAIModel           string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	if len(f.classConfig) > 0 {
		return f.classConfig
	}
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.jinaAIModel,
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

func (f fakeClassConfig) TargetVector() string {
	return ""
}
