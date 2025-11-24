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

package clients

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2multivec-weaviate/ent"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/weaviateembed"
)

const (
	defaultRPM = 500
	defaultTPM = 1_000_000
)

type vectorizer struct {
	client *weaviateembed.Client[[][]float32]
}

func New(timeout time.Duration) *vectorizer {
	return &vectorizer{
		weaviateembed.New[[][]float32](timeout, defaultRPM, defaultTPM),
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, images []string, cfg moduletools.ClassConfig) ([][][]float32, error) {
	vectors, err := v.vectorize(ctx, images, false, cfg)
	return vectors, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[][]float32], error) {
	vectors, err := v.vectorize(ctx, input, true, cfg)
	return &modulecomponents.VectorizationResult[[][]float32]{
		Text:   input,
		Vector: vectors,
	}, err
}

func (v *vectorizer) vectorize(ctx context.Context, input []string, query bool, cfg moduletools.ClassConfig) ([][][]float32, error) {
	settings := ent.NewClassSettings(cfg)
	embeddingsResponse, err := v.client.Vectorize(ctx, input, query, nil, settings.Model(), settings.BaseURL())
	if err != nil {
		return nil, errors.Wrap(err, "multi2multivec-weaviate module")
	}

	return embeddingsResponse.Embeddings, nil
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return v.client.GetVectorizerRateLimit(ctx, cfg)
}
