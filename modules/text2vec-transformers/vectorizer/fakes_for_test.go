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
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"
)

type fakeClient struct {
	lastInput  string
	lastConfig transformers.VectorizationConfig
}

func (c *fakeClient) VectorizeObject(ctx context.Context,
	text string, cfg transformers.VectorizationConfig,
) (*transformers.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &transformers.VectorizationResult{
		Vector:     []float32{0, 1, 2, 3},
		Dimensions: 4,
		Text:       text,
	}, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text string, cfg transformers.VectorizationConfig,
) (*transformers.VectorizationResult, error) {
	return c.VectorizeObject(ctx, text, cfg)
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	poolingStrategy       string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
		"poolingStrategy":    f.poolingStrategy,
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

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
