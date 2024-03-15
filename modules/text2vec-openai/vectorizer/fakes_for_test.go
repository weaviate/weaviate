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
	"fmt"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

type fakeBatchClient struct {
	lastInput        []string
	lastConfig       ent.VectorizationConfig
	defaultResetRate int
}

func (c *fakeBatchClient) Vectorize(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, *ent.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg

	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	vectors := make([][]float32, len(text))
	errors := make([]error, len(text))
	rateLimit := &ent.RateLimits{RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: c.defaultResetRate}
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf(text[i][6:])
			continue
		}
		if len(text[i]) >= len("rate ") && text[i][:5] == "rate " {
			rate, _ := strconv.Atoi(text[i][5:])
			rateLimit = &ent.RateLimits{RemainingTokens: rate, RemainingRequests: rate, LimitTokens: 2 * rate, ResetTokens: c.defaultResetRate}
		}
		if len(text[i]) >= len("wait ") && text[i][:5] == "wait " {
			wait, _ := strconv.Atoi(text[i][5:])
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
		vectors[i] = []float32{0, 1, 2, 3}
	}

	return &ent.VectorizationResult{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, rateLimit, nil
}

func (c *fakeBatchClient) VectorizeQuery(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type fakeClient struct {
	lastInput  []string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, *ent.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       text,
	}, nil, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
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
