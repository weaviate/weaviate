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
	lastInput string
}

func (c *fakeClient) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	c.lastInput = input[0]
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       input,
	}, nil, 0, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       input,
	}, nil
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	apiEndpoint           string
	modelID               string
	properties            interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
	}
	if f.apiEndpoint != "" {
		classSettings["apiEndpoint"] = f.apiEndpoint
	}
	if f.modelID != "" {
		classSettings["modelID"] = f.modelID
	}
	if f.properties != nil {
		classSettings["properties"] = f.properties
	}
	for k, v := range f.classConfig {
		classSettings[k] = v
	}
	return classSettings
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.Class()
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
