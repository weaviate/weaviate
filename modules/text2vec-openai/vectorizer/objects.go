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

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/tiktoken-go"

	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

const (
	MaxObjectsPerBatch = 2000 // https://platform.openai.com/docs/api-reference/embeddings/create
	// time per token goes down up to a certain batch size and then flattens - however the times vary a lot so we
	// don't want to get too close to the maximum of 50s
	OpenAIMaxTimePerBatch = float64(10)
)

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
	batchVectorizer  *batch.Batch
}

func New(client Client, maxBatchTime time.Duration, logger logrus.FieldLogger) *Vectorizer {
	vec := &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
		batchVectorizer:  batch.NewBatchVectorizer(client, maxBatchTime, MaxObjectsPerBatch, OpenAIMaxTimePerBatch, logger),
	}

	return vec
}

type Client interface {
	batch.BatchClient
	VectorizeQuery(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult, error)
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

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	text := v.objectVectorizer.Texts(ctx, object, ent.NewClassSettings(cfg))
	res, _, err := v.client.Vectorize(ctx, []string{text}, cfg)
	if err != nil {
		return nil, err
	}

	if len(res.Vector) > 1 {
		return libvectorizer.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}

func (v *Vectorizer) ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig,
) ([][]float32, map[int]error) {
	texts := make([]string, len(objects))
	tokenCounts := make([]int, len(objects))
	icheck := ent.NewClassSettings(cfg)

	tke, err := tiktoken.EncodingForModel(icheck.Model())
	if err != nil { // fail all objects as they all have the same model
		errs := make(map[int]error)
		for j := range objects {
			errs[j] = err
		}
		return nil, errs
	}

	// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
	skipAll := true
	for i := range objects {
		if skipObject[i] {
			continue
		}
		skipAll = false
		text := v.objectVectorizer.Texts(ctx, objects[i], icheck)
		texts[i] = text
		tokenCounts[i] = clients.GetTokensCount(icheck.Model(), text, tke)
	}

	if skipAll {
		return make([][]float32, len(objects)), make(map[int]error)
	}

	return v.batchVectorizer.SubmitBatchAndWait(ctx, cfg, skipObject, tokenCounts, texts)
}
