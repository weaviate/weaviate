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

	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/tiktoken-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type TokenizerFuncType func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer) ([]string, []int, bool, error)

func ReturnBatchTokenizer(multiplier float32, moduleName string) TokenizerFuncType {
	return func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer) ([]string, []int, bool, error) {
		texts := make([]string, len(objects))
		tokenCounts := make([]int, len(objects))
		var tke *tiktoken.Tiktoken
		icheck := settings.NewBaseClassSettings(cfg)
		modelString := modelToModelString(icheck.Model(), moduleName)
		if multiplier > 0 {
			var err error
			tke, err = tiktoken.EncodingForModel(modelString)
			if err != nil {
				tke, _ = tiktoken.EncodingForModel("text-embedding-ada-002")
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
