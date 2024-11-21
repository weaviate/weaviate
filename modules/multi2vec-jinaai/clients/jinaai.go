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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/multi2vec-jinaai/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/jinaai"
)

const (
	defaultRPM = 500 // from https://jina.ai/embeddings/
	defaultTPM = 1_000_000
)

type vectorizer struct {
	client *jinaai.Client
	logger logrus.FieldLogger
}

func New(jinaAIApiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: jinaai.New(jinaAIApiKey, timeout, defaultRPM, defaultTPM, logger),
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult, error) {
	settings := ent.NewClassSettings(cfg)
	res, err := v.client.VectorizeMultiModal(ctx, texts, images, jinaai.Settings{
		BaseURL:    settings.BaseURL(),
		Model:      settings.Model(),
		Dimensions: settings.Dimensions(),
		Normalized: true,
	})
	return res, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, texts []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	settings := ent.NewClassSettings(cfg)
	res, _, _, err := v.client.Vectorize(ctx, texts, jinaai.Settings{
		BaseURL:    settings.BaseURL(),
		Model:      settings.Model(),
		Dimensions: settings.Dimensions(),
		Task:       jinaai.RetrievalQuery,
		Normalized: true,
	})
	return res, err
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return v.client.GetVectorizerRateLimit(ctx, cfg)
}
