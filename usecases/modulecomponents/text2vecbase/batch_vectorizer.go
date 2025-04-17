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

package text2vecbase

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	"github.com/weaviate/weaviate/usecases/monitoring"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

func New(client BatchClient, batchVectorizer *batch.Batch, tokenizerFunc batch.TokenizerFuncType) *BatchVectorizer {
	vec := &BatchVectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
		batchVectorizer:  batchVectorizer,
		tokenizerFunc:    tokenizerFunc,
		encoderCache:     batch.NewEncoderCache(),
	}

	return vec
}

func (v *BatchVectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig, cs objectsvectorizer.ClassSettings,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg, cs)
	return vec, nil, err
}

func (v *BatchVectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig, cs objectsvectorizer.ClassSettings,
) ([]float32, error) {
	text := v.objectVectorizer.Texts(ctx, object, cs)
	res, _, _, err := v.client.Vectorize(ctx, []string{text}, cfg)
	if err != nil {
		return nil, err
	}

	if len(res.Vector) > 1 {
		return libvectorizer.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}

func (v *BatchVectorizer) ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig,
) ([][]float32, map[int]error) {
	beforeTokenization := time.Now()
	texts, tokenCounts, skipAll, err := v.tokenizerFunc(ctx, objects, skipObject, cfg, v.objectVectorizer, v.encoderCache)
	if err != nil {
		errs := make(map[int]error)
		for j := range texts {
			errs[j] = err
		}
		return nil, errs
	}

	monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(v.batchVectorizer.Label, "tokenization").
		Observe(time.Since(beforeTokenization).Seconds())

	if skipAll {
		return make([][]float32, len(objects)), make(map[int]error)
	}

	monitoring.GetMetrics().ModuleExternalBatchLength.WithLabelValues("vectorizeBatch", objects[0].Class).Observe(float64(len(objects)))

	return v.batchVectorizer.SubmitBatchAndWait(ctx, cfg, skipObject, tokenCounts, texts)
}

func (v *BatchVectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	res, err := v.client.VectorizeQuery(ctx, inputs, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "remote client vectorize")
	}

	if len(res.Vector) > 1 {
		return libvectorizer.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}
