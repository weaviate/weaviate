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
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/weaviate/weaviate/entities/moduletools"
)

type fakeBatchClient struct {
	defaultResetRate int
}

func (c *fakeBatchClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	vectors := make([][]float32, len(text))
	errors := make([]error, len(text))
	rateLimit := &modulecomponents.RateLimits{RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf(text[i][6:])
			continue
		}

		req := len("requests ")
		if len(text[i]) >= req && text[i][:req] == "requests " {
			reqs, _ := strconv.Atoi(strings.Split(text[i][req:], " ")[0])
			rateLimit.RemainingRequests = reqs
			rateLimit.LimitRequests = 2 * reqs
		}

		if len(text[i]) >= len("wait ") && text[i][:5] == "wait " {
			wait, _ := strconv.Atoi(text[i][5:])
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
		vectors[i] = []float32{0, 1, 2, 3}
	}

	return &modulecomponents.VectorizationResult{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, rateLimit, nil
}

func (c *fakeBatchClient) VectorizeQuery(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil, nil
}

func (c *fakeBatchClient) GetVectorizerRateLimit(ctx context.Context) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
}

func (c *fakeBatchClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

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
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil, nil
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
	// module specific settings
	cohereModel  string
	truncateType string
	baseURL      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.cohereModel,
		"truncate":           f.truncateType,
		"baseURL":            f.baseURL,
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
