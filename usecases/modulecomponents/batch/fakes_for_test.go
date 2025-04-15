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

package batch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type fakeBatchClientWithRL[T []float32] struct {
	defaultResetRate int
	defaultRPM       int
	defaultTPM       int
	rateLimit        *modulecomponents.RateLimits
	sync.Mutex
}

func (c *fakeBatchClientWithRL[T]) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[T], *modulecomponents.RateLimits, int, error) {
	c.Lock()
	defer c.Unlock()

	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	var reqError error

	vectors := make([]T, len(text))
	errors := make([]error, len(text))
	if c.rateLimit == nil {
		c.rateLimit = &modulecomponents.RateLimits{LastOverwrite: time.Now(), RemainingTokens: 100, RemainingRequests: 100, LimitTokens: 200, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
	} else if c.rateLimit.UpdateWithMissingValues {
		// return original values
		c.rateLimit.UpdateWithMissingValues = false
		c.rateLimit.RemainingTokens = 100
		c.rateLimit.RemainingRequests = 100
	} else {
		c.rateLimit.ResetTokens = time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)
	}

	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf("%s", text[i][6:])
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
		} else if len(text[i]) >= len("missingValues ") && text[i][:14] == "missingValues " {
			c.rateLimit.UpdateWithMissingValues = true
			c.rateLimit.RemainingTokens = -1
			c.rateLimit.RemainingRequests = -1
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
	return &modulecomponents.VectorizationResult[T]{
			Vector:     vectors,
			Dimensions: 4,
			Text:       text,
			Errors:     errors,
		}, &modulecomponents.RateLimits{
			LastOverwrite:           c.rateLimit.LastOverwrite,
			RemainingTokens:         c.rateLimit.RemainingTokens,
			RemainingRequests:       c.rateLimit.RemainingRequests,
			LimitTokens:             c.rateLimit.LimitTokens,
			ResetTokens:             c.rateLimit.ResetTokens,
			ResetRequests:           c.rateLimit.ResetRequests,
			LimitRequests:           c.rateLimit.LimitRequests,
			UpdateWithMissingValues: c.rateLimit.UpdateWithMissingValues,
		}, 0, reqError
}

func (c *fakeBatchClientWithRL[T]) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{RemainingTokens: c.defaultTPM, RemainingRequests: c.defaultRPM, LimitTokens: c.defaultTPM, LimitRequests: c.defaultRPM, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
}

func (c *fakeBatchClientWithRL[T]) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
}

type fakeBatchClientWithoutRL[T []float32] struct {
	defaultResetRate int
	defaultRPM       int
	defaultTPM       int
}

func (c *fakeBatchClientWithoutRL[T]) Vectorize(ctx context.Context,
	text []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[T], *modulecomponents.RateLimits, int, error) {
	if c.defaultResetRate == 0 {
		c.defaultResetRate = 60
	}

	var reqError error

	vectors := make([]T, len(text))
	errors := make([]error, len(text))
	for i := range text {
		if len(text[i]) >= len("error ") && text[i][:6] == "error " {
			errors[i] = fmt.Errorf("%s", text[i][6:])
			continue
		}

		if reqErr := len("ReqError "); len(text[i]) >= reqErr && text[i][:reqErr] == "ReqError " {
			reqError = fmt.Errorf("%v", strings.Split(text[i][reqErr:], " ")[0])
		} else if len(text[i]) >= len("wait ") && text[i][:5] == "wait " {
			wait, _ := strconv.Atoi(text[i][5:])
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
		vectors[i] = []float32{0, 1, 2, 3}
	}
	return &modulecomponents.VectorizationResult[T]{
		Vector:     vectors,
		Dimensions: 4,
		Text:       text,
		Errors:     errors,
	}, nil, 0, reqError
}

func (c *fakeBatchClientWithoutRL[T]) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{RemainingTokens: c.defaultTPM, RemainingRequests: c.defaultRPM, LimitTokens: c.defaultTPM, LimitRequests: c.defaultRPM, ResetTokens: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second), ResetRequests: time.Now().Add(time.Duration(c.defaultResetRate) * time.Second)}
}

func (c *fakeBatchClientWithoutRL[T]) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return [32]byte{}
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

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
