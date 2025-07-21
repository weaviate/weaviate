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

package batch

import (
	"context"
	"sync"

	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/tiktoken-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type EncoderCache struct {
	lock  sync.RWMutex
	cache map[string]*tiktoken.Tiktoken
}

func NewEncoderCache() *EncoderCache {
	return &EncoderCache{cache: make(map[string]*tiktoken.Tiktoken), lock: sync.RWMutex{}}
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

type TokenizerFuncType func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer, encoderCache *EncoderCache) ([]string, []int, bool, error)

func ReturnBatchTokenizer(multiplier float32, moduleName string, lowerCaseInput bool) TokenizerFuncType {
	return func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer, encoderCache *EncoderCache) ([]string, []int, bool, error) {
		texts := make([]string, len(objects))
		tokenCounts := make([]int, len(objects))
		var tke *tiktoken.Tiktoken
		icheck := settings.NewBaseClassSettings(cfg, lowerCaseInput)
		modelString := modelToModelString(icheck.Model(), moduleName)
		if multiplier > 0 {
			var err error
			// creating the tokenizer is quite expensive => cache for each module
			if tke2, ok := encoderCache.Get(modelString); ok {
				tke = tke2
			} else {
				tke, err = tiktoken.EncodingForModel(modelString)
				if err != nil {
					tke, _ = tiktoken.EncodingForModel("text-embedding-ada-002")
				}
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
			text := objectVectorizer.Texts(ctx, objects[i], icheck)
			texts[i] = text
			if multiplier > 0 {
				tokenCounts[i] = int(float32(clients.GetTokensCount(modelString, text, tke)) * multiplier)
			}
		}
		return texts, tokenCounts, skipAll, nil
	}
}

func modelToModelString(model, moduleName string) string {
	if moduleName == "text2vec-openai" {
		if model == "ada" {
			return "text-embedding-ada-002"
		}
	}
	return model
}
