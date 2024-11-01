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
	"time"

	"github.com/weaviate/tiktoken-go"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-jinaai/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

const (
	MaxObjectsPerBatch = 512 // Info from jina
	MaxTimePerBatch    = float64(10)
	MaxTokensPerBatch  = 2500 // real limit is 8192, but the vectorization times go up by A LOT if the batches are larger
)

func New(client text2vecbase.BatchClient, logger logrus.FieldLogger) *text2vecbase.BatchVectorizer {
	batchTokenizer := func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer) ([]string, []int, bool, error) {
		texts := make([]string, len(objects))
		tokenCounts := make([]int, len(objects))
		icheck := ent.NewClassSettings(cfg)

		// the encoding is different than OpenAI, but the code is not available in Go and too complicated to port.
		// using 30% more than the OpenAI model is a rough estimate but seems to work
		tke, err := tiktoken.EncodingForModel("text-embedding-ada-002")
		if err != nil { // fail all objects as they all have the same model
			return nil, nil, false, err
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
			tokenCounts[i] = int(float32(clients.GetTokensCount(icheck.Model(), text, tke)) * 1.3)
		}
		return texts, tokenCounts, skipAll, nil
	}
	// There is a limit of 8192 tokens per batch, however we cannot easily precompute how many tokens an object will.
	// Therefore, we use a dummy value and let the jina API fail in case of a too large input object.
	maxTokensPerBatch := func(cfg moduletools.ClassConfig) int { return MaxTokensPerBatch }
	return text2vecbase.New(client, batch.NewBatchVectorizer(client, 50*time.Second, MaxObjectsPerBatch, maxTokensPerBatch, MaxTimePerBatch, logger, "jinaai"), batchTokenizer)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	BaseURL() string
	Dimensions() *int64
}
