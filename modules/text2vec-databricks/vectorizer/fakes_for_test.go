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
	"github.com/weaviate/weaviate/entities/schema"
)

type fakeBatchClient struct {
	defaultResetRate int
}

func (c *fakeBatchClient) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	vectors := make([][]float32, len(text))
	errors := make([]error, len(text))
	rateLimit := &modulecomponents.RateLimits{RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf("%v", text[i][6:])
			continue
		}

		tok := len("tokens ")
		if len(text[i]) >= tok && text[i][:tok] == "tokens " {
			rate, _ := strconv.Atoi(text[i][tok:])
			rateLimit.RemainingTokens = rate
			rateLimit.LimitTokens = 2 * rate
		}

		azureTok := len("azureTokens ")
		if len(text[i]) >= azureTok && text[i][:azureTok] == "azureTokens " {
			rate, _ := strconv.Atoi(text[i][tok:])
			rateLimit.RemainingTokens = rate
			rateLimit.LimitTokens = 0
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

	return &modulecomponents.VectorizationResult[[]float32]{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, rateLimit, 0, nil
}

func (c *fakeBatchClient) VectorizeQuery(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return &modulecomponents.VectorizationResult[[]float32]{
		Vector:     [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Text:       text,
	}, nil
}

func (c *fakeBatchClient) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{RemainingTokens: 0, RemainingRequests: 0, LimitTokens: 0, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
}

func (c *fakeBatchClient) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
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

func (f FakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
