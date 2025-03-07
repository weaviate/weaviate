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

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-google/ent"
)

type fakeClient struct {
	lastInput  []string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg ent.VectorizationConfig, titlePropertyValue string,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vectors:    [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Texts:      text,
	}, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vectors:    [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Texts:      text,
	}, nil
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	apiEndpoint           string
	projectID             string
	endpointID            string
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
	if f.projectID != "" {
		classSettings["projectID"] = f.projectID
	}
	if f.endpointID != "" {
		classSettings["endpointID"] = f.endpointID
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

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
