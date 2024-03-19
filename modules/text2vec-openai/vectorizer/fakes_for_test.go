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

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type fakeBatchClient struct {
	lastInput        []string
	lastConfig       moduletools.ClassConfig
	defaultResetRate int
}

func (c *fakeBatchClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecapabilities.VectorizationResult, *modulecapabilities.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg

	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	vectors := make([][]float32, len(text))
	errors := make([]error, len(text))
	rateLimit := &modulecapabilities.RateLimits{RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: c.defaultResetRate, ResetRequests: 1}
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf(text[i][6:])
			continue
		}

		tok := len("tokens ")
		if len(text[i]) >= tok && text[i][:tok] == "tokens " {
			rate, _ := strconv.Atoi(text[i][tok:])
			rateLimit.RemainingTokens = rate
			rateLimit.LimitTokens = 2 * rate
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

	return &modulecapabilities.VectorizationResult{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, rateLimit, nil
}

func (c *fakeBatchClient) VectorizeQuery(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecapabilities.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecapabilities.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type fakeClient struct {
	lastInput  []string
	lastConfig moduletools.ClassConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecapabilities.VectorizationResult, *modulecapabilities.RateLimits, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecapabilities.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       text,
	}, nil, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecapabilities.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecapabilities.VectorizationResult{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type FakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f FakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f FakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f FakeClassConfig) Property(propName string) map[string]interface{} {
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

func (f FakeClassConfig) Tenant() string {
	return ""
}

func (f FakeClassConfig) TargetVector() string {
	return ""
}
