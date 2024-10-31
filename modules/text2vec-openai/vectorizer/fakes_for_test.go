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
	defaultRPM       int
	defaultTPM       int
	rateLimit        *modulecomponents.RateLimits
}

func (c *fakeBatchClient) VectorizeQuery(ctx context.Context, input []string, cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult, error) {
	panic("implement me")
}

func (c *fakeBatchClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	var reqError error

	vectors := make([][]float32, len(text))
	errors := make([]error, len(text))
	if c.rateLimit == nil {
		c.rateLimit = &modulecomponents.RateLimits{LastOverwrite: time.Now(), RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
	} else {
		c.rateLimit.ResetTokens = time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)
	}
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf(text[i][6:])
			continue
		}

		tok := len("tokens ")
		if len(text[i]) >= tok && text[i][:tok] == "tokens " {
			rate, _ := strconv.Atoi(text[i][tok:])
			c.rateLimit.RemainingTokens = rate
			c.rateLimit.LimitTokens = 2 * rate
		} else if req := len("requests "); len(text[i]) >= req && text[i][:req] == "requests " {
			reqs, _ := strconv.Atoi(strings.Split(text[i][req:], " ")[0])
			c.rateLimit.RemainingRequests = reqs
			c.rateLimit.LimitRequests = 2 * reqs
		} else if reqErr := len("ReqError "); len(text[i]) >= reqErr && text[i][:reqErr] == "ReqError " {
			reqError = fmt.Errorf("%v", strings.Split(text[i][reqErr:], " ")[0])
		} else if len(text[i]) >= len("wait ") && text[i][:5] == "wait " {
			wait, _ := strconv.Atoi(text[i][5:])
			time.Sleep(time.Duration(wait) * time.Millisecond)
		} else {
			// refresh the remaining token
			secondsSinceLastRefresh := time.Since(c.rateLimit.LastOverwrite)
			fraction := secondsSinceLastRefresh.Seconds() / time.Until(c.rateLimit.ResetTokens).Seconds()
			if fraction > 1 {
				c.rateLimit.RemainingTokens = c.rateLimit.LimitTokens
			} else {
				c.rateLimit.RemainingTokens += int(float64(c.rateLimit.LimitTokens) * fraction / float64(c.defaultResetRate))
			}
			if len(text[i]) > c.rateLimit.LimitTokens || len(text[i]) > c.rateLimit.RemainingTokens {
				errors[i] = fmt.Errorf("text too long for vectorization from provider: got %v, total limit: %v, remaining: %v", len(text[i]), c.rateLimit.LimitTokens, c.rateLimit.RemainingTokens)
			}

		}
		vectors[i] = []float32{0, 1, 2, 3}
	}
	c.rateLimit.LastOverwrite = time.Now()
	return &modulecomponents.VectorizationResult{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, c.rateLimit, 0, reqError
}

func (c *fakeBatchClient) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{RemainingTokens: c.defaultTPM, RemainingRequests: c.defaultRPM, LimitTokens: c.defaultTPM, LimitRequests: c.defaultRPM, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
}

func (c *fakeBatchClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

func (v *fakeBatchClient) HasTokenLimit() bool { return true }

func (v *fakeBatchClient) ReturnsRateLimit() bool { return true }

type fakeClient struct {
	lastInput  []string
	lastConfig moduletools.ClassConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &modulecomponents.VectorizationResult{
		Vector:     [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Text:       text,
	}, nil, 0, nil
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

func (c *fakeClient) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{}
}

func (c *fakeClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

func (v *fakeClient) HasTokenLimit() bool { return false }

func (v *fakeClient) ReturnsRateLimit() bool { return false }

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
