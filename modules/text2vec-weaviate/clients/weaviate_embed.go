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

package clients

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/weaviateembed"

	"github.com/weaviate/weaviate/modules/text2vec-weaviate/ent"
)

const (
	DefaultRPM = 10000
	DefaultTPM = 10_000_000
)

type vectorizer struct {
	client *weaviateembed.Client[[]float32]
}

func New(timeout time.Duration) *vectorizer {
	return &vectorizer{
		weaviateembed.New[[]float32](timeout, DefaultRPM, DefaultTPM),
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	return v.vectorize(ctx, input, false, cfg)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	res, _, _, err := v.vectorize(ctx, input, true, cfg)
	return res, err
}

func (v *vectorizer) vectorize(ctx context.Context, input []string, isSearchQuery bool,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	settings := ent.NewClassSettings(cfg)
	embeddingsResponse, err := v.client.Vectorize(ctx, input, isSearchQuery, settings.Dimensions(), settings.Model(), settings.BaseURL())
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "text2vec-weaviate module")
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(embeddingsResponse.Embeddings[0]),
		Vector:     embeddingsResponse.Embeddings,
	}, nil, modulecomponents.GetTotalTokens(embeddingsResponse.Metadata.Usage), nil
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return v.client.GetVectorizerRateLimit(ctx, cfg)
}
