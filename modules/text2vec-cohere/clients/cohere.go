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

package clients

import (
	"context"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/cohere"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-cohere/ent"
)

type vectorizer struct {
	client *cohere.Client
	logger logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: cohere.New(apiKey, timeout, logger),
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	icheck := ent.NewClassSettings(cfg)
	res, err := v.client.Vectorize(ctx, input, cohere.Settings{
		Model:     icheck.Model(),
		BaseURL:   icheck.BaseURL(),
		Truncate:  icheck.Truncate(),
		InputType: cohere.SearchDocument,
	})
	return res, nil, 0, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	icheck := ent.NewClassSettings(cfg)
	return v.client.Vectorize(ctx, input, cohere.Settings{
		Model:     icheck.Model(),
		BaseURL:   icheck.BaseURL(),
		Truncate:  icheck.Truncate(),
		InputType: cohere.SearchQuery,
	})
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return v.client.GetVectorizerRateLimit(ctx, cfg)
}
