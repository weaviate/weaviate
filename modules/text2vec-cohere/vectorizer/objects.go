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
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-cohere/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

const (
	MaxObjectsPerBatch = 96 // https://docs.cohere.com/reference/embed
	MaxTimePerBatch    = float64(10)
)

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
	batchVectorizer  *batch.Batch
}

func New(client Client, logger logrus.FieldLogger) *Vectorizer {
	return &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
		batchVectorizer:  batch.NewBatchVectorizer(client, 50*time.Second, MaxObjectsPerBatch, MaxTimePerBatch, logger),
	}
}

type Client interface {
	batch.BatchClient
	VectorizeQuery(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	Truncate() string
	BaseURL() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := ent.NewClassSettings(cfg)
	text := v.objectVectorizer.Texts(ctx, object, icheck)

	res, _, err := v.client.Vectorize(ctx, []string{text}, cfg)
	if err != nil {
		return nil, err
	}
	if len(res.Vector) == 0 {
		return nil, fmt.Errorf("no vectors generated")
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

	// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
	skipAll := true
	for i := range objects {
		if skipObject[i] {
			continue
		}
		skipAll = false
		texts[i] = v.objectVectorizer.Texts(ctx, objects[i], icheck)
		tokenCounts[i] = 0 // no token limit
	}

	if skipAll {
		return make([][]float32, len(objects)), make(map[int]error)
	}

	return v.batchVectorizer.SubmitBatchAndWait(ctx, cfg, skipObject, tokenCounts, texts)
}
