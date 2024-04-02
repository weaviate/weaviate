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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"

	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"

	"github.com/sirupsen/logrus"
)

const (
	MaxObjectsPerBatch = 2000 // https://platform.openai.com/docs/api-reference/embeddings/create
	// time per token goes down up to a certain batch size and then flattens - however the times vary a lot so we
	// don't want to get too close to the maximum of 50s
	OpenAIMaxTimePerBatch = float64(10)
)

func New(client text2vecbase.BatchClient, logger logrus.FieldLogger) *text2vecbase.BatchVectorizer {
	batchTokenizer := func(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, objectVectorizer *objectsvectorizer.ObjectVectorizer) ([]string, []int, bool, error) {
		texts := make([]string, len(objects))
		tokenCounts := make([]int, len(objects))
		icheck := ent.NewClassSettings(cfg)

		tke, err := tiktoken.EncodingForModel(icheck.Model())
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
			tokenCounts[i] = clients.GetTokensCount(icheck.Model(), text, tke)
		}
		return texts, tokenCounts, skipAll, nil
	}

	// there does not seem to be a limit
	maxTokensPerBatch := func(cfg moduletools.ClassConfig) int { return 500000 }

	return text2vecbase.New(client, batch.NewBatchVectorizer(client, 50*time.Second, MaxObjectsPerBatch, maxTokensPerBatch, OpenAIMaxTimePerBatch, logger), batchTokenizer)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	Type() string
	ModelVersion() string
	ResourceName() string
	DeploymentID() string
	BaseURL() string
	IsAzure() bool
}
