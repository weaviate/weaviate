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
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/voyageai"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-voyageai/ent"
)

var rateLimitPerModel = map[string]voyageai.VoyageRLModel{
	"voyage-3.5":      {TokenLimit: 8_000_000, RequestLimit: 2000},
	"voyage-3.5-lite": {TokenLimit: 16_000_000, RequestLimit: 2000},
	"voyage-3":        {TokenLimit: 2_000_000, RequestLimit: 1000},
	"voyage-3-lite":   {TokenLimit: 4_000_000, RequestLimit: 1000},
	"default":         {TokenLimit: 1_000_000, RequestLimit: 1000},
}

func getLimitForModel(model string) voyageai.VoyageRLModel {
	if rl, ok := rateLimitPerModel[model]; ok {
		return rl
	}
	return rateLimitPerModel["default"]
}

type voyageaiUrlBuilder struct {
	origin   string
	pathMask string
}

func newVoyageAIUrlBuilder() *voyageaiUrlBuilder {
	return &voyageaiUrlBuilder{
		origin:   ent.DefaultBaseURL,
		pathMask: "/embeddings",
	}
}

func (c *voyageaiUrlBuilder) URL(baseURL string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.pathMask)
	}
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}

type vectorizer struct {
	client *voyageai.Client
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: voyageai.New(apiKey, timeout, newVoyageAIUrlBuilder(), logger),
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	settings := ent.NewClassSettings(cfg)
	return v.client.Vectorize(ctx, input, voyageai.Settings{
		BaseURL:  settings.BaseURL(),
		Model:    settings.Model(),
		Truncate: settings.Truncate(),
	})
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	settings := ent.NewClassSettings(cfg)
	return v.client.VectorizeQuery(ctx, input, voyageai.Settings{
		BaseURL:  settings.BaseURL(),
		Model:    settings.Model(),
		Truncate: settings.Truncate(),
	})
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	return v.client.GetApiKeyHash(ctx, cfg)
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	settings := ent.NewClassSettings(cfg)
	modelRL := getLimitForModel(settings.Model())
	return v.client.GetVectorizerRateLimit(ctx, modelRL)
}
