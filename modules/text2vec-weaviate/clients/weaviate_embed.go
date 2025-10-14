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

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/weaviateembed"

	"github.com/weaviate/weaviate/modules/text2vec-weaviate/ent"
)

const (
	DefaultRPM = 10000
	DefaultTPM = 10_000_000
)

type embeddingsRequest struct {
	Texts         []string `json:"texts"`
	IsSearchQuery bool     `json:"is_search_query,omitempty"`
	Dimensions    *int64   `json:"dimensions,omitempty"`
}

type vectorizer struct {
	client *weaviateembed.Client[[]float32]
}

func New(timeout time.Duration) *vectorizer {
	return &vectorizer{
		weaviateembed.New[[]float32](timeout, DefaultRPM, DefaultTPM),
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	return v.vectorize(ctx, input, config.Model, config.BaseURL, false, config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg)
	res, _, _, err := v.vectorize(ctx, input, config.Model, config.BaseURL, true, config)
	return res, err
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	icheck := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:      icheck.Model(),
		BaseURL:    icheck.BaseURL(),
		Dimensions: icheck.Dimensions(),
	}
}

func (v *vectorizer) vectorize(ctx context.Context, input []string,
	model, baseURL string, isSearchQuery bool, config ent.VectorizationConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	embeddingRequest := v.getEmbeddingsRequest(input, isSearchQuery, config.Dimensions)

	embeddingsResponse, err := v.client.Vectorize(ctx, embeddingRequest, model, baseURL)
	if err != nil {
		return nil, nil, 0, err
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(embeddingsResponse.Embeddings[0]),
		Vector:     embeddingsResponse.Embeddings,
	}, nil, modulecomponents.GetTotalTokens(embeddingsResponse.Metadata.Usage), nil
}

func (v *vectorizer) getEmbeddingsRequest(texts []string, isSearchQuery bool, dimensions *int64) embeddingsRequest {
	return embeddingsRequest{Texts: texts, IsSearchQuery: isSearchQuery, Dimensions: dimensions}
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return v.client.GetVectorizerRateLimit(ctx, cfg)
}
