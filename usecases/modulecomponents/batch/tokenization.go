//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"sync"

	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/tiktoken-go"
	"github.com/weaviate/weaviate/entities/models"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type EncoderCache struct {
	lock   sync.RWMutex
	cache  map[string]*tiktoken.Tiktoken
	logger logrus.FieldLogger
}

func NewEncoderCache(logger logrus.FieldLogger) *EncoderCache {
	return &EncoderCache{cache: make(map[string]*tiktoken.Tiktoken), lock: sync.RWMutex{}, logger: logger}
}

func (e *EncoderCache) Get(model string) (*tiktoken.Tiktoken, bool) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	tke, ok := e.cache[model]
	return tke, ok
}

func (e *EncoderCache) Set(model string, tk *tiktoken.Tiktoken) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.cache[model] = tk
}

type TokenizerFuncType func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer, encoderCache *EncoderCache) ([]string, []int, []bool, bool, error)

func ReturnBatchTokenizer(multiplier float32, moduleName string, lowerCaseInput bool) TokenizerFuncType {
	return ReturnBatchTokenizerWithAltNames(multiplier, moduleName, nil, lowerCaseInput)
}

func ReturnBatchTokenizerWithAltNames(multiplier float32, moduleName string, altNames []string, lowerCaseInput bool) TokenizerFuncType {
	return func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer, encoderCache *EncoderCache) ([]string, []int, []bool, bool, error) {
		texts := make([]string, len(objects))
		tokenCounts := make([]int, len(objects))
		var tke *tiktoken.Tiktoken
		icheck := settings.NewBaseClassSettingsWithAltNames(cfg, lowerCaseInput, moduleName, altNames, nil)
		modelString := modelToModelString(icheck.Model(), moduleName)
		if multiplier > 0 {
			// creating the tokenizer is quite expensive => cache for each module
			if tke2, ok := encoderCache.Get(modelString); ok {
				tke = tke2
			} else {
				tke = loadEncoder(modelString, tiktoken.EncodingForModel, encoderCache.logger)
				encoderCache.Set(modelString, tke)
			}
		}

		// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
		skipAll := true
		for i := range texts {
			if skipObject[i] {
				continue
			}
			skipAll = false
			text, isEmpty := objectVectorizer.Texts(ctx, objects[i], icheck)
			if isEmpty {
				skipObject[i] = true
				continue
			}
			texts[i] = text
			if multiplier > 0 {
				tokenCounts[i] = int(float32(GetTokensCount(modelString, text, tke)) * multiplier)
			}
		}
		return texts, tokenCounts, skipObject, skipAll, nil
	}
}

// loadEncoder returns the tokenizer for modelString, falling back to ada-002 when
// the model has no encoding of its own. Returns nil and warns when neither loads,
// so callers estimate the token count instead.
func loadEncoder(modelString string, load func(string) (*tiktoken.Tiktoken, error), logger logrus.FieldLogger) *tiktoken.Tiktoken {
	tke, err := load(modelString)
	if err == nil {
		return tke
	}
	tke, fallbackErr := load("text-embedding-ada-002")
	if fallbackErr != nil {
		logger.Warnf("could not load tokenizer vocabulary for model %q; batch token counts will be estimated. Provide the vocabulary offline via TOKENIZER_BPE_DIR to avoid this: %v", modelString, fallbackErr)
		return nil
	}
	return tke
}

func modelToModelString(model, moduleName string) string {
	if moduleName == "text2vec-openai" {
		if model == "ada" {
			return "text-embedding-ada-002"
		}
	}
	return model
}
